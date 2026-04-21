"""
Gold layer: Serving tables for dashboards and decision support.

Creates/replaces:
  - gold.daily_analytics     — wide denormalized master table (zero joins)
  - gold.watchlist_ranked    — latest signals with PERCENT_RANK composite scoring
  - gold.signal_alerts       — threshold-crossing alerts for today
  - gold.signal_history      — time series of key signals
  - gold.benchmark_compare   — cumulative return vs SPY/QQQ
  - gold.portfolio_candidates — filtered buckets (momentum, value, low-vol, etc.)

Creates if not exists:
  - gold.trade_log           — manual trade journal

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
    TABLE_SILVER_PRICES,
    TABLE_SILVER_FUNDAMENTALS,
    TABLE_GOLD_WATCHLIST,
    TABLE_GOLD_SIGNAL_ALERTS,
    TABLE_GOLD_SIGNAL_HISTORY,
    TABLE_GOLD_BENCHMARK_COMPARE,
    TABLE_GOLD_PORTFOLIO_CANDIDATES,
    TABLE_GOLD_TRADE_LOG,
    TABLE_GOLD_DAILY_ANALYTICS,
)


def main():
    spark = SparkSession.builder.getOrCreate()

    silver_count = spark.table(TABLE_SILVER_SIGNALS).count()
    if silver_count == 0:
        raise RuntimeError("[build_gold] FATAL: Silver signals table is empty.")
    print(f"[build_gold] Silver table has {silver_count} total rows.")

    # ── 1. Ranked watchlist (PERCENT_RANK 4-dim composite) ──
    spark.sql(f"""
        CREATE OR REPLACE TABLE {TABLE_GOLD_WATCHLIST} AS
        WITH latest AS (
            SELECT *
            FROM {TABLE_SILVER_SIGNALS}
            WHERE as_of_date = (
                SELECT MAX(as_of_date)
                FROM {TABLE_SILVER_SIGNALS}
            )
        ),
        scored AS (
            SELECT
                *,
                -- PERCENT_RANK normalization (0-1) per dimension
                PERCENT_RANK() OVER (ORDER BY COALESCE(change_30d_pct, 0) DESC) AS momentum_pct,
                PERCENT_RANK() OVER (ORDER BY COALESCE(pe_ratio, 999) ASC) AS value_pct,
                PERCENT_RANK() OVER (ORDER BY COALESCE(rsi, 50) ASC) AS risk_pct,
                PERCENT_RANK() OVER (ORDER BY COALESCE(mfi, 50) DESC) AS quality_pct
            FROM latest
        )
        SELECT
            symbol,
            trade_signal,
            rsi,
            macd,
            macd_signal_line,
            macd_histogram,
            last_closing_price,
            bollinger_upper,
            bollinger_lower,
            bollinger_pct_b,
            bollinger_bandwidth,
            obv,
            mfi,
            eps,
            pe_ratio,
            forward_eps,
            forward_pe,
            dividend_yield,
            dividend_yield_gap,
            dividend_yield_trap,
            last_day_change_abs,
            last_day_change_pct,
            change_30d_pct,
            change_90d_pct,
            change_365d_pct,
            ma_50,
            ma_200,
            ema_50,
            ema_200,
            as_of_date,
            momentum_pct,
            value_pct,
            risk_pct,
            quality_pct,
            -- Weighted composite (equal weights)
            (COALESCE(momentum_pct, 0.5) * 0.25 +
             COALESCE(value_pct, 0.5) * 0.25 +
             COALESCE(risk_pct, 0.5) * 0.25 +
             COALESCE(quality_pct, 0.5) * 0.25
            ) AS composite_score,
            RANK() OVER (ORDER BY (
                COALESCE(momentum_pct, 0.5) * 0.25 +
                COALESCE(value_pct, 0.5) * 0.25 +
                COALESCE(risk_pct, 0.5) * 0.25 +
                COALESCE(quality_pct, 0.5) * 0.25
            ) DESC) AS composite_rank
        FROM scored
        ORDER BY composite_score DESC
    """)
    print(f"[build_gold] Created/replaced {TABLE_GOLD_WATCHLIST}")

    # ── 2. Signal alerts ─────────────────────────────────────
    # Alerts use signal_history (full time series) for LAG-based crossover detection.
    # RSI alerts use latest snapshot (no LAG needed).
    # MACD/Bollinger/MA200 alerts use signal_history LAG partitioned by symbol.
    spark.sql(f"""
        CREATE OR REPLACE TABLE {TABLE_GOLD_SIGNAL_ALERTS} AS
        WITH latest AS (
            SELECT *
            FROM {TABLE_SILVER_SIGNALS}
            WHERE as_of_date = (
                SELECT MAX(as_of_date)
                FROM {TABLE_SILVER_SIGNALS}
            )
        ),
        -- MACD crossover: histogram flips sign between consecutive days
        macd_lag AS (
            SELECT symbol, as_of_date, macd_histogram,
                   LAG(macd_histogram) OVER (PARTITION BY symbol ORDER BY as_of_date) AS prev_macd_hist
            FROM {TABLE_SILVER_SIGNALS}
            WHERE macd_histogram IS NOT NULL
        ),
        -- MA200 crossover: price crosses above/below 200-day MA
        ma200_lag AS (
            SELECT symbol, as_of_date, last_closing_price, ma_200,
                   LAG(last_closing_price) OVER (PARTITION BY symbol ORDER BY as_of_date) AS prev_close,
                   LAG(ma_200) OVER (PARTITION BY symbol ORDER BY as_of_date) AS prev_ma200
            FROM {TABLE_SILVER_SIGNALS}
            WHERE ma_200 IS NOT NULL
        ),
        -- Bollinger squeeze: bandwidth is in the bottom 10th percentile
        -- of the last 126 trading days for that specific symbol
        bollinger_pct AS (
            SELECT h.symbol, h.as_of_date, h.bollinger_bandwidth,
                   PERCENT_RANK() OVER (
                       PARTITION BY h.symbol
                       ORDER BY h.bollinger_bandwidth
                   ) AS bw_percentile
            FROM {TABLE_SILVER_SIGNALS} h
            WHERE h.as_of_date >= DATE_SUB(
                       (SELECT MAX(as_of_date) FROM {TABLE_SILVER_SIGNALS}), 126)
              AND h.bollinger_bandwidth IS NOT NULL
        ),
        alerts AS (
            -- RSI overbought/oversold (from latest snapshot only)
            SELECT symbol, as_of_date,
                CASE
                    WHEN rsi < 30 THEN 'RSI_OVERSOLD'
                    WHEN rsi > 70 THEN 'RSI_OVERBOUGHT'
                END AS alert_type,
                rsi AS alert_value
            FROM latest
            WHERE rsi < 30 OR rsi > 70

            UNION ALL

            -- MACD crossover: histogram flips sign (bullish or bearish)
            SELECT m.symbol, m.as_of_date,
                'MACD_CROSSOVER' AS alert_type,
                m.macd_histogram AS alert_value
            FROM macd_lag m
            WHERE m.prev_macd_hist IS NOT NULL
              AND SIGN(m.macd_histogram) != SIGN(m.prev_macd_hist)

            UNION ALL

            -- Bollinger squeeze: bandwidth in bottom 10th percentile for symbol
            SELECT b.symbol, b.as_of_date,
                'BOLLINGER_SQUEEZE' AS alert_type,
                b.bollinger_bandwidth AS alert_value
            FROM bollinger_pct b
            WHERE b.bw_percentile <= 0.10
              AND b.as_of_date = (SELECT MAX(as_of_date) FROM {TABLE_SILVER_SIGNALS})

            UNION ALL

            -- MA200 crossover: price crosses above/below 200-day moving average
            SELECT symbol, as_of_date,
                CASE
                    WHEN last_closing_price > ma_200 AND prev_close <= prev_ma200
                    THEN 'MA200_CROSS_ABOVE'
                    WHEN last_closing_price < ma_200 AND prev_close >= prev_ma200
                    THEN 'MA200_CROSS_BELOW'
                END AS alert_type,
                last_closing_price AS alert_value
            FROM ma200_lag
            WHERE prev_close IS NOT NULL
              AND as_of_date = (SELECT MAX(as_of_date) FROM {TABLE_SILVER_SIGNALS})

            UNION ALL

            -- Dividend yield trap: trailing yield >> forward yield
            SELECT symbol, as_of_date,
                'DIVIDEND_YIELD_TRAP' AS alert_type,
                dividend_yield_gap AS alert_value
            FROM latest
            WHERE dividend_yield_trap = 1.0
        )
        SELECT symbol, as_of_date, alert_type, alert_value
        FROM alerts
        WHERE alert_type IS NOT NULL
        ORDER BY symbol, alert_type
    """)
    print(f"[build_gold] Created/replaced {TABLE_GOLD_SIGNAL_ALERTS}")

    # ── 3. Signal history ─────────────────────────────────────
    spark.sql(f"""
        CREATE OR REPLACE TABLE {TABLE_GOLD_SIGNAL_HISTORY} AS
        SELECT
            symbol,
            as_of_date,
            trade_signal,
            rsi,
            macd,
            macd_signal_line,
            macd_histogram,
            last_closing_price,
            bollinger_pct_b,
            bollinger_bandwidth,
            obv,
            mfi,
            change_30d_pct,
            change_90d_pct,
            pe_ratio,
            forward_pe,
            eps,
            forward_eps,
            bollinger_upper,
            bollinger_lower,
            ma_50,
            ma_200,
            ema_50,
            ema_200,
            atr_14d
        FROM {TABLE_SILVER_SIGNALS}
        ORDER BY symbol, as_of_date
    """)
    print(f"[build_gold] Created/replaced {TABLE_GOLD_SIGNAL_HISTORY}")

    # ── 4. Benchmark comparison ───────────────────────────────
    spark.sql(f"""
        CREATE OR REPLACE TABLE {TABLE_GOLD_BENCHMARK_COMPARE} AS
        WITH latest AS (
            SELECT symbol, as_of_date,
                last_closing_price,
                change_30d_pct,
                change_90d_pct,
                change_365d_pct
            FROM {TABLE_SILVER_SIGNALS}
            WHERE as_of_date = (
                SELECT MAX(as_of_date)
                FROM {TABLE_SILVER_SIGNALS}
            )
        ),
        spy_bench AS (
            SELECT change_30d_pct AS spy_change_30d,
                   change_90d_pct AS spy_change_90d,
                   change_365d_pct AS spy_change_365d
            FROM latest
            WHERE symbol = 'SPY'
            LIMIT 1
        ),
        qqq_bench AS (
            SELECT change_30d_pct AS qqq_change_30d,
                   change_90d_pct AS qqq_change_90d,
                   change_365d_pct AS qqq_change_365d
            FROM latest
            WHERE symbol = 'QQQ'
            LIMIT 1
        )
        SELECT
            l.symbol,
            l.as_of_date,
            l.last_closing_price,
            l.change_30d_pct,
            l.change_90d_pct,
            l.change_365d_pct,
            COALESCE(spy.spy_change_30d, 0) AS spy_change_30d,
            COALESCE(spy.spy_change_90d, 0) AS spy_change_90d,
            COALESCE(spy.spy_change_365d, 0) AS spy_change_365d,
            COALESCE(qqq.qqq_change_30d, 0) AS qqq_change_30d,
            COALESCE(qqq.qqq_change_90d, 0) AS qqq_change_90d,
            COALESCE(qqq.qqq_change_365d, 0) AS qqq_change_365d
        FROM latest l
        LEFT JOIN spy_bench spy
        LEFT JOIN qqq_bench qqq
        WHERE l.symbol NOT IN ('SPY', 'QQQ')
        ORDER BY l.change_365d_pct DESC NULLS LAST
    """)
    print(f"[build_gold] Created/replaced {TABLE_GOLD_BENCHMARK_COMPARE}")

    # ── 5. Portfolio candidates ───────────────────────────────
    spark.sql(f"""
        CREATE OR REPLACE TABLE {TABLE_GOLD_PORTFOLIO_CANDIDATES} AS
        WITH latest AS (
            SELECT *
            FROM {TABLE_SILVER_SIGNALS}
            WHERE as_of_date = (
                SELECT MAX(as_of_date)
                FROM {TABLE_SILVER_SIGNALS}
            )
        ),
        tagged AS (
            SELECT symbol, as_of_date, last_closing_price,
                rsi, pe_ratio, dividend_yield, change_30d_pct,
                bollinger_bandwidth, trade_signal,
                CASE
                    WHEN change_30d_pct IS NOT NULL AND change_30d_pct > 5 THEN 'top_momentum'
                    WHEN rsi < 30 THEN 'oversold'
                    WHEN pe_ratio IS NOT NULL AND pe_ratio < 15 AND dividend_yield > 0.02 THEN 'value_dividend'
                    WHEN bollinger_bandwidth IS NOT NULL AND
                         bollinger_bandwidth < (SELECT PERCENTILE_APPROX(bollinger_bandwidth, 0.25) FROM latest WHERE bollinger_bandwidth IS NOT NULL)
                         THEN 'low_volatility'
                    ELSE 'watch'
                END AS candidate_bucket
            FROM latest
        )
        SELECT symbol, as_of_date, last_closing_price, rsi, pe_ratio,
               dividend_yield, change_30d_pct, candidate_bucket
        FROM tagged
        WHERE candidate_bucket != 'watch'
        ORDER BY candidate_bucket, symbol
    """)
    print(f"[build_gold] Created/replaced {TABLE_GOLD_PORTFOLIO_CANDIDATES}")

    # ── 6. Trade log (manual journal, create once) ──────────
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

    # ── 7. Daily analytics — wide denormalized master table ──
    # Joins Silver signals + Silver prices + Silver fundamentals.
    # One row per (symbol, date). Zero joins needed for ad-hoc exploration.
    # Includes composite scoring (PERCENT_RANK) and boolean dividend_yield_trap.
    spark.sql(f"""
        CREATE OR REPLACE TABLE {TABLE_GOLD_DAILY_ANALYTICS} AS
        WITH prices AS (
            SELECT symbol, date, open, high, low, close, volume
            FROM {TABLE_SILVER_PRICES}
        ),
        signals AS (
            SELECT *
            FROM {TABLE_SILVER_SIGNALS}
        ),
        fund AS (
            SELECT symbol,
                shortName AS short_name, longName AS long_name,
                sector, industry, country, currency, exchange,
                marketCap AS market_cap,
                trailingPE AS trailing_pe, forwardPE AS forward_pe,
                priceToBook AS price_to_book,
                trailingEps AS trailing_eps, forwardEps AS forward_eps,
                dividendRate AS dividend_rate, dividendYield AS dividend_yield_raw,
                payoutRatio AS payout_ratio,
                beta, profitMargins AS profit_margins,
                operatingMargins AS operating_margins, grossMargins AS gross_margins,
                revenueGrowth AS revenue_growth, earningsGrowth AS earnings_growth,
                returnOnEquity AS return_on_equity, returnOnAssets AS return_on_assets,
                debtToEquity AS debt_to_equity, currentRatio AS current_ratio,
                freeCashflow AS free_cashflow
            FROM {TABLE_SILVER_FUNDAMENTALS}
        ),
        joined AS (
            SELECT
                s.symbol,
                s.as_of_date,
                -- Price data
                p.open, p.high, p.low, p.close, p.volume,
                -- Technical indicators
                s.trade_signal,
                s.rsi,
                s.macd, s.macd_signal_line, s.macd_histogram,
                s.bollinger_upper, s.bollinger_lower,
                s.bollinger_pct_b, s.bollinger_bandwidth,
                s.obv, s.mfi,
                s.ma_50, s.ma_200, s.ema_50, s.ema_200,
                s.atr_14d,
                -- Returns
                s.last_day_change_abs, s.last_day_change_pct,
                s.change_30d_pct, s.change_90d_pct, s.change_365d_pct,
                s.last_closing_price,
                -- Fundamentals (aliased to snake_case for usability)
                f.short_name, f.long_name, f.sector, f.industry,
                f.country, f.currency, f.exchange,
                f.market_cap, f.trailing_pe, f.forward_pe, f.price_to_book,
                f.trailing_eps, f.forward_eps,
                f.dividend_rate, f.payout_ratio,
                f.beta, f.profit_margins, f.operating_margins, f.gross_margins,
                f.revenue_growth, f.earnings_growth,
                f.return_on_equity, f.return_on_assets,
                f.debt_to_equity, f.current_ratio, f.free_cashflow,
                -- Dividend signals (convert DOUBLE trap flag to BOOLEAN)
                s.dividend_yield, s.dividend_yield_gap,
                CASE WHEN s.dividend_yield_trap = 1.0 THEN true ELSE false END AS dividend_yield_trap
            FROM signals s
            LEFT JOIN prices p
                ON s.symbol = p.symbol AND s.as_of_date = p.date
            LEFT JOIN fund f
                ON s.symbol = f.symbol
        ),
        scored AS (
            SELECT *,
                PERCENT_RANK() OVER (PARTITION BY as_of_date ORDER BY COALESCE(change_30d_pct, 0) DESC) AS momentum_pct,
                PERCENT_RANK() OVER (PARTITION BY as_of_date ORDER BY COALESCE(trailing_pe, 999) ASC) AS value_pct,
                PERCENT_RANK() OVER (PARTITION BY as_of_date ORDER BY COALESCE(rsi, 50) ASC) AS risk_pct,
                PERCENT_RANK() OVER (PARTITION BY as_of_date ORDER BY COALESCE(mfi, 50) DESC) AS quality_pct
            FROM joined
        )
        SELECT
            *,
            (COALESCE(momentum_pct, 0.5) * 0.25 +
             COALESCE(value_pct, 0.5) * 0.25 +
             COALESCE(risk_pct, 0.5) * 0.25 +
             COALESCE(quality_pct, 0.5) * 0.25
            ) AS composite_score
        FROM scored
        ORDER BY symbol, as_of_date
    """)
    print(f"[build_gold] Created/replaced {TABLE_GOLD_DAILY_ANALYTICS}")


if __name__ == "__main__":
    main()