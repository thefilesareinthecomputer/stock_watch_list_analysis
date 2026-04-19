"""
Gold layer: Analytical tables derived from Silver signals.

Creates/replaces:
  - gold.watchlist_ranked   — latest signals with composite ranks
  - gold.signal_history     — time series of key signals (for dashboards)

Creates if not exists:
  - gold.trade_log          — manual trade journal (you INSERT into this)

All Gold writes are idempotent (CREATE OR REPLACE / IF NOT EXISTS).
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
    TABLE_SILVER_SIGNALS,
    TABLE_GOLD_WATCHLIST,
    TABLE_GOLD_SIGNAL_HISTORY,
    TABLE_GOLD_TRADE_LOG,
)


def main():
    spark = SparkSession.builder.getOrCreate()

    silver_count = spark.table(TABLE_SILVER_SIGNALS).count()
    if silver_count == 0:
        raise RuntimeError("[build_gold] FATAL: Silver signals table is empty.")
    print(f"[build_gold] Silver table has {silver_count} total rows.")

    # ── 1. Ranked watchlist (latest day only) ─────────────
    spark.sql(f"""
        CREATE OR REPLACE TABLE {TABLE_GOLD_WATCHLIST} AS
        WITH latest AS (
            SELECT *
            FROM {TABLE_SILVER_SIGNALS}
            WHERE as_of_date = (
                SELECT MAX(as_of_date)
                FROM {TABLE_SILVER_SIGNALS}
            )
        )
        SELECT
            symbol,
            trade_signal,
            rsi,
            macd,
            macd_signal_line,
            last_closing_price,
            bollinger_upper,
            bollinger_lower,
            eps,
            pe_ratio,
            dividend_yield,
            last_day_change_abs,
            last_day_change_pct,
            change_30d_pct,
            change_90d_pct,
            change_365d_pct,
            ma_50,
            ma_200,
            as_of_date,
            RANK() OVER (ORDER BY COALESCE(rsi, 999) ASC) AS rsi_rank,
            RANK() OVER (ORDER BY COALESCE(pe_ratio, 999) ASC) AS value_rank,
            RANK() OVER (ORDER BY COALESCE(change_30d_pct, -999) DESC) AS momentum_rank,
            (
                RANK() OVER (ORDER BY COALESCE(rsi, 999) ASC) +
                RANK() OVER (ORDER BY COALESCE(pe_ratio, 999) ASC) +
                RANK() OVER (ORDER BY COALESCE(change_30d_pct, -999) DESC)
            ) AS composite_rank
        FROM latest
        ORDER BY composite_rank ASC
    """)
    print(f"[build_gold] Created/replaced {TABLE_GOLD_WATCHLIST}")

    # ── 2. Signal history (time series for dashboards) ────
    spark.sql(f"""
        CREATE OR REPLACE TABLE {TABLE_GOLD_SIGNAL_HISTORY} AS
        SELECT
            symbol,
            as_of_date,
            trade_signal,
            rsi,
            macd,
            macd_signal_line,
            last_closing_price,
            change_30d_pct,
            change_90d_pct,
            pe_ratio,
            eps,
            bollinger_upper,
            bollinger_lower,
            ma_50,
            ma_200,
            atr_14d
        FROM {TABLE_SILVER_SIGNALS}
        ORDER BY symbol, as_of_date
    """)
    print(f"[build_gold] Created/replaced {TABLE_GOLD_SIGNAL_HISTORY}")

    # ── 3. Trade log (manual journal, create once) ────────
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_GOLD_TRADE_LOG} (
            trade_date      DATE        COMMENT 'Date the trade was executed',
            symbol          STRING      COMMENT 'Ticker symbol',
            action          STRING      COMMENT 'BUY or SELL',
            shares          DECIMAL(12,4) COMMENT 'Number of shares',
            price           DECIMAL(12,4) COMMENT 'Execution price per share',
            total_cost      DECIMAL(14,2) COMMENT 'Total trade cost (shares * price)',
            rationale       STRING      COMMENT 'Why you made this trade',
            signal_at_entry STRING      COMMENT 'Model signal at time of trade (Buy/Sell/Hold)',
            rsi_at_entry    DOUBLE      COMMENT 'RSI value at time of trade',
            pe_at_entry     DOUBLE      COMMENT 'P/E ratio at time of trade',
            outcome_notes   STRING      COMMENT 'Post-trade notes (fill in later)'
        )
        COMMENT 'Manual trade journal. INSERT rows via SQL editor after making decisions.'
    """)
    print(f"[build_gold] Created (if not exists) {TABLE_GOLD_TRADE_LOG}")


if __name__ == "__main__":
    main()
