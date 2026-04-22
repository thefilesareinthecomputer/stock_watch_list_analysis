"""
Silver layer: Compute technical indicators for ALL historical dates.

Uses build_signal_series() from indicators.py to compute vectorized
indicators (RSI, MACD, Bollinger, OBV, MFI, ATR, MAs, EMAs) as full
pandas Series — producing one row per (symbol, as_of_date) instead of
one row per symbol.

Writes to silver.daily_signals via MERGE on (symbol, as_of_date).
"""
import sys
import os
try:
    _src_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
except NameError:
    _src_dir = os.path.join(os.getcwd(), "..")
sys.path.insert(0, os.path.normpath(_src_dir))

import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from common.config import (
    TABLE_SILVER_PRICES,
    TABLE_SILVER_FUNDAMENTALS,
    TABLE_SILVER_SIGNALS,
    TABLE_BRONZE_FUNDAMENTALS,
    TICKERS,
)
from common.indicators import build_signal_series


def main():
    spark = SparkSession.builder.getOrCreate()

    print("[build_silver_signals] Building historical signals...")

    # Read from Silver prices (canonical deduped)
    prices_df = spark.table(TABLE_SILVER_PRICES).toPandas()
    print(f"[build_silver_signals] Silver prices: {len(prices_df)} rows, "
          f"{prices_df['symbol'].nunique()} tickers")

    try:
        fundamentals_df = spark.table(TABLE_SILVER_FUNDAMENTALS).toPandas()
        print(f"[build_silver_signals] Silver fundamentals: {len(fundamentals_df)} rows")
    except Exception as e:
        print(f"[build_silver_signals] WARNING: Could not read fundamentals: {e}")
        fundamentals_df = pd.DataFrame()

    all_frames = []
    errors = []
    skipped = []

    # Try to load per-date fundamentals from bronze SCD2 (PIT)
    # For each (symbol, date), find the fundamentals version that was
    # current on that date using effective_from/effective_to.
    pit_fundamentals = pd.DataFrame()
    try:
        pit_fundamentals = spark.sql(f"""
            WITH price_dates AS (
                SELECT DISTINCT symbol, date AS as_of_date
                FROM {TABLE_SILVER_PRICES}
            ),
            pit AS (
                SELECT
                    sd.symbol,
                    CAST(sd.as_of_date AS STRING) AS as_of_date,
                    bf.trailingEps AS eps,
                    bf.trailingPE AS pe_ratio,
                    bf.forwardEps AS forward_eps,
                    bf.forwardPE AS forward_pe,
                    bf.dividendYield AS dividend_yield,
                    bf.fiveYearAvgDividendYield AS five_year_avg_yield,
                    ROW_NUMBER() OVER (
                        PARTITION BY sd.symbol, sd.as_of_date
                        ORDER BY bf.effective_from DESC
                    ) AS rn
                FROM {TABLE_BRONZE_FUNDAMENTALS} bf
                INNER JOIN price_dates sd
                    ON bf.symbol = sd.symbol
                    AND bf.effective_from <= CAST(sd.as_of_date AS STRING)
                    AND (bf.effective_to IS NULL OR bf.effective_to > CAST(sd.as_of_date AS STRING))
            )
            SELECT symbol, as_of_date, eps, pe_ratio, forward_eps,
                   forward_pe, dividend_yield, five_year_avg_yield
            FROM pit
            WHERE rn = 1
            ORDER BY symbol, as_of_date
        """).toPandas()
        if not pit_fundamentals.empty:
            print(f"[build_silver_signals] PIT fundamentals: {len(pit_fundamentals)} rows, "
                  f"{pit_fundamentals['symbol'].nunique()} tickers")
        else:
            print("[build_silver_signals] No PIT fundamentals matched (SCD2 data may not have date overlap)")
    except Exception as e:
        print(f"[build_silver_signals] PIT fundamentals not available: {e}")

    for symbol in TICKERS:
        try:
            ticker_prices = prices_df[prices_df["symbol"] == symbol].copy()

            if ticker_prices.empty:
                skipped.append((symbol, "no price data in Silver"))
                continue

            ticker_prices["date"] = pd.to_datetime(ticker_prices["date"])
            ticker_prices = ticker_prices.set_index("date").sort_index()

            if len(ticker_prices) < 200:
                skipped.append((symbol, f"only {len(ticker_prices)} rows (need 200)"))
                continue

            sym_info = {}
            sym_fundamentals = None
            if not fundamentals_df.empty and "symbol" in fundamentals_df.columns:
                matches = fundamentals_df[fundamentals_df["symbol"] == symbol]
                if not matches.empty:
                    sym_info = matches.iloc[0].to_dict()
                    sym_info = {k: (v if not (isinstance(v, float) and np.isnan(v)) else None)
                                for k, v in sym_info.items()}

            # Per-date fundamentals from gold PIT table (if available)
            if not pit_fundamentals.empty and "symbol" in pit_fundamentals.columns:
                sym_pit = pit_fundamentals[pit_fundamentals["symbol"] == symbol]
                if not sym_pit.empty:
                    sym_fundamentals = sym_pit

            # Returns DataFrame with one row per historical date
            signal_df = build_signal_series(
                ticker_prices, symbol, sym_info,
                fundamentals_df=sym_fundamentals)
            if signal_df is not None and not signal_df.empty:
                all_frames.append(signal_df)
            else:
                skipped.append((symbol, "build_signal_series returned empty"))

        except Exception as e:
            errors.append((symbol, str(e)))
            print(f"[build_silver_signals] ERROR processing {symbol}: {e}")

    if not all_frames:
        raise RuntimeError(
            f"[build_silver_signals] FATAL: No signals computed. "
            f"Errors: {errors}. Skipped: {skipped}"
        )

    silver_df = pd.concat(all_frames, ignore_index=True)
    print(f"[build_silver_signals] Computed {len(silver_df)} signal rows "
          f"for {silver_df['symbol'].nunique()} tickers")

    if skipped:
        print(f"[build_silver_signals] Skipped: {len(skipped)} — {skipped[:5]}...")
    if errors:
        print(f"[build_silver_signals] Errors: {len(errors)} — {errors[:5]}...")

    # Ensure as_of_date is string for Spark serialization
    silver_df["as_of_date"] = silver_df["as_of_date"].astype(str)

    # Convert NaN to None for Spark compatibility
    silver_df = silver_df.where(pd.notna(silver_df), None)
    for col in silver_df.columns:
        if silver_df[col].dtype == object:
            silver_df[col] = silver_df[col].astype(object).where(silver_df[col].notna(), None)

    # Use Row-based createDataFrame with explicit schema to bypass
    # PySpark Connect ChunkedArray bug AND CANNOT_DETERMINE_TYPE error
    # (all-None columns can't have types inferred without a schema)
    from pyspark.sql import Row
    from pyspark.sql.types import (
        StructType, StructField, StringType, DoubleType
    )

    SIGNALS_SCHEMA = StructType([
        StructField("symbol", StringType(), False),
        StructField("as_of_date", StringType(), False),
        StructField("trade_signal", StringType(), True),
        StructField("rsi", DoubleType(), True),
        StructField("macd", DoubleType(), True),
        StructField("macd_signal_line", DoubleType(), True),
        StructField("macd_histogram", DoubleType(), True),
        StructField("last_closing_price", DoubleType(), True),
        StructField("bollinger_upper", DoubleType(), True),
        StructField("bollinger_lower", DoubleType(), True),
        StructField("bollinger_pct_b", DoubleType(), True),
        StructField("bollinger_bandwidth", DoubleType(), True),
        StructField("obv", DoubleType(), True),
        StructField("mfi", DoubleType(), True),
        StructField("eps", DoubleType(), True),
        StructField("pe_ratio", DoubleType(), True),
        StructField("forward_eps", DoubleType(), True),
        StructField("forward_pe", DoubleType(), True),
        StructField("dividend_yield", DoubleType(), True),
        StructField("dividend_yield_gap", DoubleType(), True),
        StructField("dividend_yield_trap", DoubleType(), True),
        StructField("last_day_change_abs", DoubleType(), True),
        StructField("last_day_change_pct", DoubleType(), True),
        StructField("change_30d_pct", DoubleType(), True),
        StructField("change_90d_pct", DoubleType(), True),
        StructField("change_365d_pct", DoubleType(), True),
        StructField("ma_50", DoubleType(), True),
        StructField("ma_200", DoubleType(), True),
        StructField("ema_50", DoubleType(), True),
        StructField("ema_200", DoubleType(), True),
        StructField("atr_14d", DoubleType(), True),
    ])

    spark_rows = [Row(**{k: (None if isinstance(v, float) and pd.isna(v) else v)
                         for k, v in row.items()})
                  for _, row in silver_df.iterrows()]
    spark_df = spark.createDataFrame(spark_rows, schema=SIGNALS_SCHEMA)

    # Cast date string to proper date type (stored as string in pandas for
    # safe serialization, schema uses StringType to avoid type inference)
    spark_df = spark_df.withColumn("as_of_date", spark_df["as_of_date"].cast("date"))

    # Create table or MERGE
    table_exists = spark.catalog.tableExists(TABLE_SILVER_SIGNALS)

    if table_exists:
        existing_cols = {col.name for col in spark.table(TABLE_SILVER_SIGNALS).schema.fields}
        new_cols = set(spark_df.columns) - existing_cols
        if new_cols:
            print(f"[build_silver_signals] Adding {len(new_cols)} new columns: {new_cols}")
            for col in new_cols:
                try:
                    spark.sql(f"ALTER TABLE {TABLE_SILVER_SIGNALS} ADD COLUMN `{col}` DOUBLE")
                except Exception as e:
                    print(f"[build_silver_signals] Could not add column {col}: {e}")

    if not table_exists:
        spark_df.createOrReplaceTempView("signals_schema_infer")
        spark.sql(f"""
            CREATE TABLE {TABLE_SILVER_SIGNALS}
            USING DELTA
            AS SELECT * FROM signals_schema_infer
        """)
        try:
            spark.sql(f"ALTER TABLE {TABLE_SILVER_SIGNALS} CLUSTER BY (symbol, as_of_date)")
        except Exception as e:
            print(f"[build_silver_signals] Note: Could not add Liquid Clustering: {e}")
        print(f"[build_silver_signals] Created {TABLE_SILVER_SIGNALS}")
    else:
        spark_df.createOrReplaceTempView("signals_updates")
        all_cols = spark_df.columns
        update_set = ", ".join(f"t.`{c}` = s.`{c}`" for c in all_cols if c not in ("symbol", "as_of_date"))
        insert_cols = ", ".join(f"`{c}`" for c in all_cols)
        insert_vals = ", ".join(f"s.`{c}`" for c in all_cols)
        spark.sql(f"""
            MERGE INTO {TABLE_SILVER_SIGNALS} t
            USING signals_updates s
            ON t.symbol = s.symbol AND t.as_of_date = s.as_of_date
            WHEN MATCHED THEN UPDATE SET {update_set}
            WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
        """)
        print(f"[build_silver_signals] Merged {len(silver_df)} rows into {TABLE_SILVER_SIGNALS}")


if __name__ == "__main__":
    main()