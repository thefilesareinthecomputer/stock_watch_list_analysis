"""
Gold layer: Signal snapshot fact table.

Grain: One row per security per as-of date.
FKs: security_key (dim_security), date_key (dim_date)
Measures: RSI, MACD (line/signal/histogram), Bollinger (%B, BandWidth),
          OBV, MFI, moving averages, ATR, trade_signal, composite scores.

Built from Silver daily_signals joined to dim_security.
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
    TABLE_GOLD_DIM_SECURITY,
    TABLE_GOLD_FACT_SIGNALS,
)


def main():
    spark = SparkSession.builder.getOrCreate()

    print("[build_fact_signals] Building signal snapshot fact...")

    # Date_key from as_of_date (YYYYMMDD int, Kimball convention)
    # PERCENT_RANK is a window function requiring OVER clause
    spark.sql(f"""
        CREATE OR REPLACE TABLE {TABLE_GOLD_FACT_SIGNALS} AS
        WITH signal_data AS (
            SELECT
                s.security_key,
                sig.symbol,
                sig.as_of_date,
                sig.trade_signal,
                sig.rsi,
                sig.macd,
                sig.macd_signal_line,
                sig.macd_histogram,
                sig.bollinger_upper,
                sig.bollinger_lower,
                sig.bollinger_pct_b,
                sig.bollinger_bandwidth,
                sig.obv,
                sig.mfi,
                sig.last_closing_price,
                sig.ma_50,
                sig.ma_200,
                sig.ema_50,
                sig.ema_200,
                sig.atr_14d,
                sig.last_day_change_abs,
                sig.last_day_change_pct,
                sig.change_30d_pct,
                sig.change_90d_pct,
                sig.change_365d_pct,
                sig.eps,
                sig.pe_ratio,
                sig.forward_eps,
                sig.forward_pe,
                sig.dividend_yield,
                sig.dividend_yield_gap,
                -- dividend_yield_trap is DOUBLE (1.0/0.0), not BOOLEAN
                CASE WHEN sig.dividend_yield_trap = 1.0 THEN true ELSE false END AS dividend_yield_trap
            FROM {TABLE_SILVER_SIGNALS} sig
            LEFT JOIN (
                SELECT security_key, symbol
                FROM {TABLE_GOLD_DIM_SECURITY}
                WHERE is_current = true
            ) s ON sig.symbol = s.symbol
        ),
        scored AS (
            SELECT *,
                PERCENT_RANK() OVER (PARTITION BY as_of_date ORDER BY COALESCE(change_30d_pct, 0) DESC) AS momentum_pct,
                PERCENT_RANK() OVER (PARTITION BY as_of_date ORDER BY COALESCE(pe_ratio, 999) ASC) AS value_pct,
                PERCENT_RANK() OVER (PARTITION BY as_of_date ORDER BY COALESCE(rsi, 50) ASC) AS risk_pct,
                PERCENT_RANK() OVER (PARTITION BY as_of_date ORDER BY COALESCE(mfi, 50) DESC) AS quality_pct
            FROM signal_data
        )
        SELECT
            security_key,
            CAST(DATE_FORMAT(as_of_date, 'yyyyMMdd') AS INT) AS date_key,
            symbol,
            as_of_date,
            trade_signal,
            rsi,
            macd, macd_signal_line, macd_histogram,
            bollinger_upper, bollinger_lower,
            bollinger_pct_b, bollinger_bandwidth,
            obv, mfi,
            last_closing_price,
            ma_50, ma_200, ema_50, ema_200,
            atr_14d,
            last_day_change_abs, last_day_change_pct,
            change_30d_pct, change_90d_pct, change_365d_pct,
            eps, pe_ratio, forward_eps, forward_pe,
            dividend_yield, dividend_yield_gap, dividend_yield_trap,
            momentum_pct, value_pct, risk_pct, quality_pct,
            (COALESCE(momentum_pct, 0.5) * 0.25 +
             COALESCE(value_pct, 0.5) * 0.25 +
             COALESCE(risk_pct, 0.5) * 0.25 +
             COALESCE(quality_pct, 0.5) * 0.25
            ) AS composite_score
        FROM scored
        ORDER BY symbol, as_of_date
    """)

    count = spark.table(TABLE_GOLD_FACT_SIGNALS).count()
    print(f"[build_fact_signals] {TABLE_GOLD_FACT_SIGNALS}: {count} rows")


if __name__ == "__main__":
    main()