"""
Silver layer: Canonical deduped price data.

Reads Bronze daily_prices (append-only with duplicates), deduplicates
on (symbol, date) by taking the latest _ingest_ts, writes to
silver.daily_prices via MERGE.

Grain: One row per symbol per trading date.
"""
import sys
import os
try:
    _src_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
except NameError:
    _src_dir = os.path.join(os.getcwd(), "..")
sys.path.insert(0, os.path.normpath(_src_dir))

from pyspark.sql import SparkSession
from common.config import TABLE_BRONZE_PRICES, TABLE_SILVER_PRICES
from common.quality import check_silver_prices


def main():
    spark = SparkSession.builder.getOrCreate()

    print(f"[build_silver_prices] Reading from {TABLE_BRONZE_PRICES}...")

    # Dedup: keep latest ingest per (symbol, date)
    # Using SQL window function for clarity
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW silver_prices_deduped AS
        WITH ranked AS (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY symbol, date
                    ORDER BY _ingest_ts DESC
                ) AS rn
            FROM {TABLE_BRONZE_PRICES}
        )
        SELECT
            symbol, date, open, high, low, close, volume
        FROM ranked
        WHERE rn = 1
    """)

    # Create table if not exists with Liquid Clustering
    table_exists = spark.catalog.tableExists(TABLE_SILVER_PRICES)

    if not table_exists:
        spark.sql(f"""
            CREATE TABLE {TABLE_SILVER_PRICES}
            (
                symbol  STRING  NOT NULL,
                date    DATE    NOT NULL,
                open    DOUBLE,
                high    DOUBLE,
                low     DOUBLE,
                close   DOUBLE,
                volume  BIGINT
            )
            USING DELTA
            CLUSTER BY (symbol, date)
        """)
        print(f"[build_silver_prices] Created {TABLE_SILVER_PRICES} with Liquid Clustering")

    # MERGE: insert new (symbol, date) pairs, update existing ones
    spark.sql(f"""
        MERGE INTO {TABLE_SILVER_PRICES} t
        USING silver_prices_deduped s
        ON t.symbol = s.symbol AND t.date = s.date
        WHEN MATCHED THEN
            UPDATE SET
                t.open = s.open, t.high = s.high, t.low = s.low,
                t.close = s.close, t.volume = s.volume
        WHEN NOT MATCHED THEN
            INSERT (symbol, date, open, high, low, close, volume)
            VALUES (s.symbol, s.date, s.open, s.high, s.low, s.close, s.volume)
    """)

    count = spark.table(TABLE_SILVER_PRICES).count()
    print(f"[build_silver_prices] {TABLE_SILVER_PRICES}: {count} rows (MERGE complete)")

    # Quality checks
    silver_pd = spark.table(TABLE_SILVER_PRICES).toPandas()
    issues = check_silver_prices(silver_pd)
    for issue in issues:
        print(f"[build_silver_prices] QUALITY: {issue}")


if __name__ == "__main__":
    main()