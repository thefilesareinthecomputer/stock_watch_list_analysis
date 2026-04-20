"""
Gold layer: Fundamental snapshot fact table.

Grain: One row per security per snapshot date.
FKs: security_key (dim_security), date_key (dim_date)
Measures: market_cap, PE, EPS, dividend yield, margins, growth, etc.

Built from Silver fundamentals_current joined to dim_security.
"""
import sys
import os
try:
    _src_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
except NameError:
    _src_dir = os.path.join(os.getcwd(), "..")
sys.path.insert(0, os.path.normpath(_src_dir))

from datetime import date
from pyspark.sql import SparkSession
from common.config import (
    TABLE_SILVER_FUNDAMENTALS,
    TABLE_GOLD_DIM_SECURITY,
    TABLE_GOLD_FACT_FUNDAMENTALS,
    FUNDAMENTALS_NUMERIC_FIELDS,
    FUNDAMENTALS_STRING_FIELDS,
)


def main():
    spark = SparkSession.builder.getOrCreate()
    today = date.today()

    print("[build_fact_fundamentals] Building fundamental snapshot fact...")

    # Build column lists — only numeric measures go in fact table
    # String SCD2 attributes (sector, industry, etc.) are in dim_security
    measure_fields = [f for f in FUNDAMENTALS_NUMERIC_FIELDS
                      if f not in ("sharesOutstanding", "floatShares")]
    measure_cols = ", ".join(f"f.`{col}`" for col in measure_fields)

    # Snapshot date = today, date_key = YYYYMMDD
    date_key = int(today.strftime("%Y%m%d"))

    spark.sql(f"""
        CREATE OR REPLACE TABLE {TABLE_GOLD_FACT_FUNDAMENTALS} AS
        SELECT
            s.security_key,
            {date_key} AS date_key,
            f.symbol,
            CAST('{today}' AS DATE) AS snapshot_date,
            {measure_cols}
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