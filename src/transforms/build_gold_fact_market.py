"""
Gold layer: Market price fact table.

Grain: One row per security per trading day.
FKs: security_key (dim_security), date_key (dim_date)
Measures: open, high, low, close, volume + computed returns + risk metrics.

Built from Silver daily_prices, joined to dimensions for surrogate keys.
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
    TABLE_SILVER_PRICES,
    TABLE_GOLD_DIM_SECURITY,
    TABLE_GOLD_FACT_MARKET,
)


def main():
    spark = SparkSession.builder.getOrCreate()

    print("[build_fact_market] Building market price fact...")

    # Silver prices joined to dim_security for surrogate key
    # date_key = YYYYMMDD integer (Kimball convention)
    spark.sql(f"""
        CREATE OR REPLACE TABLE {TABLE_GOLD_FACT_MARKET} AS
        SELECT
            s.security_key,
            CAST(DATE_FORMAT(p.date, 'yyyyMMdd') AS INT) AS date_key,
            p.symbol,
            p.date,
            p.open,
            p.high,
            p.low,
            p.close,
            p.volume,
            -- Return calculations (lag-based, safe for 1st row = NULL)
            (p.close - LAG(p.close) OVER (PARTITION BY p.symbol ORDER BY p.date))
                / LAG(p.close) OVER (PARTITION BY p.symbol ORDER BY p.date)
                AS return_1d,
            (p.close - LAG(p.close, 5) OVER (PARTITION BY p.symbol ORDER BY p.date))
                / LAG(p.close, 5) OVER (PARTITION BY p.symbol ORDER BY p.date)
                AS return_5d,
            (p.close - LAG(p.close, 21) OVER (PARTITION BY p.symbol ORDER BY p.date))
                / LAG(p.close, 21) OVER (PARTITION BY p.symbol ORDER BY p.date)
                AS return_21d,
            (p.close - LAG(p.close, 63) OVER (PARTITION BY p.symbol ORDER BY p.date))
                / LAG(p.close, 63) OVER (PARTITION BY p.symbol ORDER BY p.date)
                AS return_63d,
            (p.close - LAG(p.close, 252) OVER (PARTITION BY p.symbol ORDER BY p.date))
                / LAG(p.close, 252) OVER (PARTITION BY p.symbol ORDER BY p.date)
                AS return_252d
        FROM {TABLE_SILVER_PRICES} p
        LEFT JOIN (
            SELECT security_key, symbol
            FROM {TABLE_GOLD_DIM_SECURITY}
            WHERE is_current = true
        ) s ON p.symbol = s.symbol
        ORDER BY p.symbol, p.date
    """)

    count = spark.table(TABLE_GOLD_FACT_MARKET).count()
    print(f"[build_fact_market] {TABLE_GOLD_FACT_MARKET}: {count} rows")


if __name__ == "__main__":
    main()