"""
Gold layer: Congressional trades summary serving table.

Aggregates Silver congressional trades into a per-symbol summary showing:
  - Which congresspeople traded this stock
  - Trade type (buy/sell), amount range, most recent trade date
  - Net directional signal (buys vs sells count)

Grain: one row per (symbol, office_name, trade_type).
This keeps the serving table simple and queryable — join to daily_analytics
on symbol to see congressional activity alongside technical/fundamental data.

Query example:
    SELECT d.symbol, d.close, d.composite_score, c.office_name,
           c.trade_type, c.amount_range, c.last_trade_date
    FROM gold.daily_analytics d
    JOIN gold.congressional_trades_summary c ON d.symbol = c.symbol
    WHERE d.as_of_date = (SELECT MAX(as_of_date) FROM gold.daily_analytics)
    ORDER BY d.composite_score DESC
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
    TABLE_SILVER_CONGRESSIONAL,
    TABLE_GOLD_CONGRESSIONAL_SUMMARY,
)


def main():
    spark = SparkSession.builder.getOrCreate()

    print("[build_gold_congressional] Building congressional trades summary...")

    try:
        silver_count = spark.table(TABLE_SILVER_CONGRESSIONAL).count()
    except Exception as e:
        print(f"[build_gold_congressional] WARNING: Silver table not found: {e}")
        print("[build_gold_congressional] Skipping — congressional data not yet ingested.")
        return

    if silver_count == 0:
        print("[build_gold_congressional] Silver table is empty. Skipping.")
        return

    print(f"[build_gold_congressional] Silver: {silver_count} rows")

    spark.sql(f"""
        CREATE OR REPLACE TABLE {TABLE_GOLD_CONGRESSIONAL_SUMMARY} AS
        WITH trades AS (
            SELECT
                symbol,
                office_name,
                chamber,
                trade_type,
                amount_range,
                amount_midpoint,
                transaction_date,
                disclosure_date,
                owner
            FROM {TABLE_SILVER_CONGRESSIONAL}
            WHERE symbol IS NOT NULL
        ),
        aggregated AS (
            SELECT
                symbol,
                office_name,
                chamber,
                trade_type,
                owner,
                -- Most recent trade
                MAX(transaction_date) AS last_trade_date,
                -- Filing delay: days between trade and disclosure
                AVG(DATEDIFF(disclosure_date, transaction_date)) AS avg_filing_delay_days,
                -- Amount statistics
                MAX(amount_range) AS max_amount_range,
                AVG(amount_midpoint) AS avg_amount_midpoint,
                -- Trade count
                COUNT(*) AS trade_count,
                -- Total estimated value
                SUM(amount_midpoint) AS total_estimated_value
            FROM trades
            GROUP BY symbol, office_name, chamber, trade_type, owner
        )
        SELECT
            symbol,
            office_name,
            chamber,
            trade_type,
            owner,
            last_trade_date,
            avg_filing_delay_days,
            max_amount_range,
            avg_amount_midpoint,
            trade_count,
            total_estimated_value
        FROM aggregated
        ORDER BY symbol, last_trade_date DESC
    """)

    count = spark.table(TABLE_GOLD_CONGRESSIONAL_SUMMARY).count()
    print(f"[build_gold_congressional] Created {TABLE_GOLD_CONGRESSIONAL_SUMMARY}: "
          f"{count} rows")


if __name__ == "__main__":
    main()