"""
Gold layer: Fundamental snapshot fact table.

Grain: One row per security per trading date.
FKs: security_key (dim_security), date_key (dim_date)
Measures: market_cap, PE, EPS, dividend yield, margins, growth, etc.

Point-in-time correctness:
  Uses bronze.company_fundamentals (SCD2) to find which version of
  fundamentals was active on each trading date. For SCD2-tracked
  attributes (sector, industry, name), this provides true PIT data.
  For daily-measure fields (PE, EPS, margins), we use the current
  snapshot because SCD2 only creates new versions when attr_hash
  changes — not on every daily fetch. This is a known limitation.

Built from Bronze SCD2 fundamentals joined to dim_security and dim_date.
"""
import sys
import os
try:
    _src_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
except NameError:
    _src_dir = os.path.join(os.getcwd(), "..")
sys.path.insert(0, os.path.normpath(_src_dir))

from pyspark.sql import SparkSession
from common.config import (
    TABLE_BRONZE_FUNDAMENTALS,
    TABLE_SILVER_FUNDAMENTALS,
    TABLE_GOLD_DIM_SECURITY,
    TABLE_GOLD_DIM_DATE,
    TABLE_GOLD_FACT_FUNDAMENTALS,
    FUNDAMENTALS_NUMERIC_FIELDS,
)


def main():
    spark = SparkSession.builder.getOrCreate()

    print("[build_fact_fundamentals] Building fundamental snapshot fact (PIT)...")

    # Build column lists — only numeric measures go in fact table
    # String SCD2 attributes (sector, industry, etc.) are in dim_security
    measure_fields = [f for f in FUNDAMENTALS_NUMERIC_FIELDS
                      if f not in ("sharesOutstanding", "floatShares")]
    measure_cols_bronze = ", ".join(f"bf.`{col}`" for col in measure_fields)
    measure_cols_pit = ", ".join(f"pf.`{col}`" for col in measure_fields)

    # Check if bronze SCD2 table has data with effective_from/effective_to
    try:
        bronze_count = spark.table(TABLE_BRONZE_FUNDAMENTALS).count()
    except Exception:
        bronze_count = 0

    if bronze_count > 0:
        # Point-in-time: use bronze SCD2 fundamentals with effective dates
        # For each (symbol, trading_date), find the fundamentals version
        # that was current on that date using effective_from/effective_to
        print(f"[build_fact_fundamentals] Using PIT fundamentals from "
              f"{bronze_count} bronze rows")

        spark.sql(f"""
            CREATE OR REPLACE TABLE {TABLE_GOLD_FACT_FUNDAMENTALS} AS
            WITH trading_dates AS (
                SELECT date_key, date AS trading_date
                FROM {TABLE_GOLD_DIM_DATE}
                WHERE is_trading_day = true
            ),
            -- Find the most recent fundamentals version active on each date
            -- effective_from <= trading_date AND (effective_to IS NULL OR effective_to > trading_date)
            pit_fund AS (
                SELECT
                    bf.*,
                    td.date_key,
                    td.trading_date,
                    ROW_NUMBER() OVER (
                        PARTITION BY bf.symbol, td.date_key
                        ORDER BY bf.effective_from DESC
                    ) AS rn
                FROM {TABLE_BRONZE_FUNDAMENTALS} bf
                INNER JOIN trading_dates td
                    ON bf.effective_from <= CAST(td.trading_date AS STRING)
                    AND (bf.effective_to IS NULL OR bf.effective_to > CAST(td.trading_date AS STRING))
            )
            SELECT
                s.security_key,
                pf.date_key,
                pf.symbol,
                pf.trading_date AS snapshot_date,
                {measure_cols_pit}
            FROM (
                SELECT * FROM pit_fund WHERE rn = 1
            ) pf
            LEFT JOIN (
                SELECT security_key, symbol
                FROM {TABLE_GOLD_DIM_SECURITY}
                WHERE is_current = true
            ) s ON pf.symbol = s.symbol
            ORDER BY pf.symbol, pf.date_key
        """)
    else:
        # Fallback: no SCD2 data, use current snapshot for today only
        print("[build_fact_fundamentals] WARNING: No bronze SCD2 data. "
              "Falling back to current snapshot.")

        from datetime import date
        today = date.today()
        date_key = int(today.strftime("%Y%m%d"))

        spark.sql(f"""
            CREATE OR REPLACE TABLE {TABLE_GOLD_FACT_FUNDAMENTALS} AS
            SELECT
                s.security_key,
                {date_key} AS date_key,
                f.symbol,
                CAST('{today}' AS DATE) AS snapshot_date,
                {', '.join(f'f.`{col}`' for col in measure_fields)}
            FROM {TABLE_SILVER_FUNDAMENTALS} f
            LEFT JOIN (
                SELECT security_key, symbol
                FROM {TABLE_GOLD_DIM_SECURITY}
                WHERE is_current = true
            ) s ON f.symbol = s.symbol
            ORDER BY f.symbol
        """)

    count = spark.table(TABLE_GOLD_FACT_FUNDAMENTALS).count()
    print(f"[build_fact_fundamentals] {TABLE_GOLD_FACT_FUNDAMENTALS}: {count} rows")


if __name__ == "__main__":
    main()