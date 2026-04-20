"""
Gold layer: Date dimension with NYSE trading calendar.

Creates gold.dim_date as a static SCD0 dimension:
  - Calendar attributes: day_of_week, month, quarter, year, is_weekend, is_month_end, is_quarter_end
  - Trading calendar: is_trading_day, is_early_close, trading_day_of_month, trading_day_of_year
  - Navigation: prior_trading_day, next_trading_day

Uses exchange_calendars package for NYSE holiday logic.
Built once, refreshed yearly. CREATE OR REPLACE TABLE.
"""
import sys
import os
try:
    _src_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
except NameError:
    _src_dir = os.path.join(os.getcwd(), "..")
sys.path.insert(0, os.path.normpath(_src_dir))

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, BooleanType
)
from common.config import TABLE_GOLD_DIM_DATE


def build_dim_date(start_year: int = 2006, end_year: int = 2030) -> pd.DataFrame:
    """Build the date dimension using exchange_calendars for NYSE schedule."""
    import exchange_calendars as xc

    cal = xc.get_calendar("XNYS")  # NYSE

    # Determine valid date range (XNYS bounds: first session 2006-04-19, last session ~2027)
    first_session = cal.first_session
    last_session = cal.last_session
    start_date = max(pd.Timestamp(f"{start_year}-01-01"), first_session)
    end_date = min(pd.Timestamp(f"{end_year}-12-31"), last_session)

    # Generate all calendar days
    dates = pd.date_range(start_date, end_date, freq="D")

    # Get NYSE trading days
    trading_sessions = cal.sessions_in_range(start_date, end_date)

    # Early close dates (e.g. day before July 4, Black Friday, Christmas Eve)
    # exchange_calendars API: use .early_closes property (not early_closes_schedule_in_range)
    early_closes_raw = cal.early_closes
    # Filter to our date range and convert to date strings
    early_closes_set = set(
        early_closes_raw[
            (early_closes_raw >= start_date) & (early_closes_raw <= end_date)
        ].strftime("%Y-%m-%d")
    )

    trading_dates = set(trading_sessions.strftime("%Y-%m-%d"))

    rows = []
    trading_day_of_year = 0
    current_year = None

    for d in dates:
        date_str = d.strftime("%Y-%m-%d")
        is_trading = date_str in trading_dates
        is_early = date_str in early_closes_set

        # Reset trading day of year counter on new year
        if d.year != current_year:
            current_year = d.year
            trading_day_of_year = 0

        if is_trading:
            trading_day_of_year += 1

        rows.append({
            "date_key": int(d.strftime("%Y%m%d")),
            "date": date_str,
            "day_of_week": d.dayofweek,       # 0=Mon, 6=Sun
            "day_name": d.day_name(),
            "month": d.month,
            "month_name": d.month_name(),
            "quarter": d.quarter,
            "year": d.year,
            "is_weekend": d.dayofweek >= 5,
            "is_month_end": d.is_month_end,
            "is_quarter_end": d.is_quarter_end,
            "is_year_end": d.is_year_end,
            "is_trading_day": is_trading,
            "is_early_close": is_early,
            "trading_day_of_year": trading_day_of_year if is_trading else None,
        })

    df = pd.DataFrame(rows)

    # Add prior/next trading day navigation
    trading_date_list = sorted(
        [d.strftime("%Y-%m-%d") for d in trading_sessions]
    )
    trading_date_set = set(trading_date_list)

    prior_map = {}
    next_map = {}
    for i, td in enumerate(trading_date_list):
        prior_map[td] = trading_date_list[i - 1] if i > 0 else None
        next_map[td] = trading_date_list[i + 1] if i < len(trading_date_list) - 1 else None

    df["prior_trading_day"] = df["date"].map(prior_map)
    df["next_trading_day"] = df["date"].map(next_map)

    # Trading day of month
    df["trading_day_of_month"] = None
    for sym, group in df[df["is_trading_day"]].groupby(
        [df["date"].str[:7]]  # YYYY-MM group
    ):
        for idx, (_, row) in enumerate(group.iterrows(), 1):
            df.loc[row.name, "trading_day_of_month"] = idx

    return df


def main():
    spark = SparkSession.builder.getOrCreate()

    print("[build_dim_date] Building date dimension...")
    dim_date = build_dim_date()

    print(f"[build_dim_date] {len(dim_date)} dates, "
          f"{dim_date['is_trading_day'].sum()} trading days")

    # Convert DataFrame rows to Spark Row objects with explicit types
    # (avoid pd.isna/where issues that can corrupt dtypes)
    from pyspark.sql import Row

    spark_rows = []
    for _, row in dim_date.iterrows():
        spark_rows.append(Row(
            date_key=int(row["date_key"]),
            date=str(row["date"]),
            day_of_week=int(row["day_of_week"]),
            day_name=str(row["day_name"]),
            month=int(row["month"]),
            month_name=str(row["month_name"]),
            quarter=int(row["quarter"]),
            year=int(row["year"]),
            is_weekend=bool(row["is_weekend"]),
            is_month_end=bool(row["is_month_end"]),
            is_quarter_end=bool(row["is_quarter_end"]),
            is_year_end=bool(row["is_year_end"]),
            is_trading_day=bool(row["is_trading_day"]),
            is_early_close=bool(row["is_early_close"]),
            trading_day_of_year=int(row["trading_day_of_year"]) if pd.notna(row.get("trading_day_of_year")) else None,
            trading_day_of_month=int(row["trading_day_of_month"]) if pd.notna(row.get("trading_day_of_month")) else None,
            prior_trading_day=str(row["prior_trading_day"]) if pd.notna(row.get("prior_trading_day")) else None,
            next_trading_day=str(row["next_trading_day"]) if pd.notna(row.get("next_trading_day")) else None,
        ))

    # Explicit schema to avoid CANNOT_DETERMINE_TYPE on all-None columns
    dim_date_schema = StructType([
        StructField("date_key", IntegerType(), False),
        StructField("date", StringType(), False),
        StructField("day_of_week", IntegerType(), False),
        StructField("day_name", StringType(), False),
        StructField("month", IntegerType(), False),
        StructField("month_name", StringType(), False),
        StructField("quarter", IntegerType(), False),
        StructField("year", IntegerType(), False),
        StructField("is_weekend", BooleanType(), False),
        StructField("is_month_end", BooleanType(), False),
        StructField("is_quarter_end", BooleanType(), False),
        StructField("is_year_end", BooleanType(), False),
        StructField("is_trading_day", BooleanType(), False),
        StructField("is_early_close", BooleanType(), False),
        StructField("trading_day_of_year", IntegerType(), True),
        StructField("trading_day_of_month", IntegerType(), True),
        StructField("prior_trading_day", StringType(), True),
        StructField("next_trading_day", StringType(), True),
    ])

    spark_df = spark.createDataFrame(spark_rows, schema=dim_date_schema)

    # Cast date column and write
    spark_df = spark_df.selectExpr(
        "date_key",
        "CAST(date AS DATE) as date",
        "day_of_week", "day_name", "month", "month_name",
        "quarter", "year",
        "is_weekend", "is_month_end", "is_quarter_end", "is_year_end",
        "is_trading_day", "is_early_close",
        "trading_day_of_year", "trading_day_of_month",
        "CAST(prior_trading_day AS DATE) as prior_trading_day",
        "CAST(next_trading_day AS DATE) as next_trading_day",
    )
    spark_df.write.mode("overwrite").saveAsTable(TABLE_GOLD_DIM_DATE)
    print(f"[build_dim_date] Written {len(dim_date)} rows to {TABLE_GOLD_DIM_DATE}")


if __name__ == "__main__":
    main()