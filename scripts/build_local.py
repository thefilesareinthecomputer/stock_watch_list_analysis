"""Build local silver and gold from the DuckDB warehouse.

    uv run python scripts/build_local.py             # whole universe
    uv run python scripts/build_local.py --limit 10  # smoke test

Runs the SAME logic as Databricks, not a reimplementation of it:

  - indicators come from `common.indicators.build_signal_series`, the identical
    pandas function the Spark job calls, so parity there is by construction;
  - ranking comes from `scoring.components.percentile_sql`, the identical SQL
    string, proven equivalent across engines by tests/test_engine_parity.py.

What legitimately differs from Databricks: prices here are adjusted by our own
factors derived from raw closes (`common.adjustments`) rather than by yfinance's
self-rewriting adj_close, and fundamentals are absent locally, so `value_pct`
degenerates to a constant. Both are known and documented in
tasks/SPEC-LOCAL-WAREHOUSE.md.
"""
import argparse
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

import duckdb  # noqa: E402
import pandas as pd  # noqa: E402

from common.adjustments import adjusted_prices  # noqa: E402
from common.indicators import build_signal_series  # noqa: E402
from scoring.components import percentile_sql  # noqa: E402

WAREHOUSE = os.path.join(ROOT, "warehouse", "market.duckdb")


def build_signals(con, symbols):
    frames = []
    for i, symbol in enumerate(symbols, 1):
        raw = con.execute(
            "SELECT date, open, high, low, close, volume, dividend, split_ratio "
            "FROM bronze_prices WHERE symbol = ? ORDER BY date", [symbol]
        ).df()
        if len(raw) < 200:  # indicators need ~200 sessions of warmup
            continue

        adj = adjusted_prices(raw)
        # build_signal_series expects lowercase OHLCV, matching silver's schema
        # rather than yfinance's capitalised frame.
        ohlcv = pd.DataFrame({
            "open": adj["adj_open"], "high": adj["adj_high"],
            "low": adj["adj_low"], "close": adj["adj_close"],
            "volume": adj["volume"],
        })
        ohlcv.index = pd.DatetimeIndex(pd.to_datetime(adj["date"]))

        try:
            signals = build_signal_series(ohlcv, symbol, info={})
        except Exception as exc:  # one bad symbol must not sink the build
            print(f"  {symbol}: FAILED - {type(exc).__name__}: {exc}", flush=True)
            continue
        if signals is not None and not signals.empty:
            frames.append(signals)
        if i % 25 == 0:
            print(f"  {i}/{len(symbols)} symbols", flush=True)

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--limit", type=int)
    args = parser.parse_args()

    con = duckdb.connect(WAREHOUSE)
    symbols = [r[0] for r in con.execute(
        "SELECT DISTINCT symbol FROM bronze_prices ORDER BY symbol").fetchall()]
    if args.limit:
        symbols = symbols[:args.limit]

    print(f"building signals for {len(symbols)} symbols")
    signals = build_signals(con, symbols)
    if signals.empty:
        sys.exit("no signals produced")

    con.register("incoming_signals", signals)
    con.execute("CREATE OR REPLACE TABLE silver_signals AS "
                "SELECT * FROM incoming_signals")
    con.unregister("incoming_signals")

    # Gold ranking, using the same SQL Databricks runs.
    con.execute(f"""
        CREATE OR REPLACE TABLE gold_watchlist_ranked AS
        WITH latest AS (
            SELECT * FROM silver_signals
            WHERE as_of_date = (SELECT MAX(as_of_date) FROM silver_signals)
        ),
        scored AS (
            SELECT *, {percentile_sql()} FROM latest
        )
        SELECT *,
            (COALESCE(momentum_pct, 0.5) * 0.25 +
             COALESCE(value_pct, 0.5) * 0.25 +
             COALESCE(risk_pct, 0.5) * 0.25 +
             COALESCE(quality_pct, 0.5) * 0.25) AS composite_score,
            RANK() OVER (ORDER BY (
                COALESCE(momentum_pct, 0.5) * 0.25 +
                COALESCE(value_pct, 0.5) * 0.25 +
                COALESCE(risk_pct, 0.5) * 0.25 +
                COALESCE(quality_pct, 0.5) * 0.25) DESC) AS composite_rank
        FROM scored
    """)

    rows, syms, lo, hi = con.execute(
        "SELECT COUNT(*), COUNT(DISTINCT symbol), MIN(as_of_date), "
        "MAX(as_of_date) FROM silver_signals").fetchone()
    print(f"\nsilver_signals: {rows} rows, {syms} symbols, {lo} to {hi}")
    print(f"gold_watchlist_ranked: "
          f"{con.execute('SELECT COUNT(*) FROM gold_watchlist_ranked').fetchone()[0]} rows")
    print("\ntop 10 by composite_rank:")
    print(con.execute(
        "SELECT composite_rank, symbol, ROUND(composite_score,4) AS score, "
        "ROUND(momentum_pct,3) AS mom, ROUND(risk_pct,3) AS risk, "
        "ROUND(quality_pct,3) AS qual FROM gold_watchlist_ranked "
        "ORDER BY composite_rank LIMIT 10").df().to_string(index=False))
    con.close()


if __name__ == "__main__":
    main()
