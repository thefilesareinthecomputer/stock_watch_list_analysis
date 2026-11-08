"""Backfill raw prices and corporate actions into the local DuckDB warehouse.

    uv run python scripts/backfill.py              # full history (config.HISTORY_START_DATE)
    uv run python scripts/backfill.py --years 3    # shorter window
    uv run python scripts/backfill.py --limit 20   # smoke test

Stores RAW OHLCV plus dividends and splits - deliberately not adjusted close.
`auto_adjust=True` rescales the entire close history every time a dividend is
paid, so an adjusted series is not stable over time and cannot serve as the
evidence layer: a score computed last month silently changes this month. Owning
raw prices plus the actions that adjust them makes history immutable and lets
the adjustment be recomputed deterministically whenever it is needed.

Idempotent per (symbol, window): re-running deletes and re-inserts the rows it
fetched rather than duplicating them.
"""
import argparse
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(os.path.join(ROOT, ".env"))

import duckdb  # noqa: E402
import pandas as pd  # noqa: E402
import yfinance as yf  # noqa: E402

from common.config import TICKERS, BENCHMARK_TICKERS, HISTORY_START_DATE  # noqa: E402
from common.corporate_actions import record_actions  # noqa: E402
from common.positions import daily_symbols, load_positions  # noqa: E402
from common.quality import (  # noqa: E402
    completed_session_cutoff, last_trading_session,
)
from common.run_context import new_run_id, now_ts  # noqa: E402
from common.security import ensure_quote_types  # noqa: E402

WAREHOUSE = os.path.join(ROOT, "warehouse", "market.duckdb")
TABLE = "bronze_prices"
CHUNK = 25

SCHEMA = """
    CREATE TABLE IF NOT EXISTS bronze_prices (
        symbol VARCHAR,
        date DATE,
        open DOUBLE,
        high DOUBLE,
        low DOUBLE,
        close DOUBLE,          -- Yahoo close: split-adjusted, not dividend-adjusted
        adj_close DOUBLE,      -- yfinance's adjusted close, kept for reconciliation
        volume BIGINT,
        dividend DOUBLE,
        split_ratio DOUBLE,
        _run_id VARCHAR,
        _ingest_ts VARCHAR,
        _source_system VARCHAR,
        _source_event_ts VARCHAR,
        _load_type VARCHAR
    )
"""

RENAMES = {
    "Open": "open", "High": "high", "Low": "low", "Close": "close",
    "Adj Close": "adj_close", "Volume": "volume",
    "Dividends": "dividend", "Stock Splits": "split_ratio",
}


def _symbol_frame(raw, symbol):
    """One symbol's single-level frame from a download result.

    yfinance >= 0.2.51 returns MULTI-LEVEL columns even for a one-ticker
    request (CLAUDE-era gotcha 7), so select by ticker level whenever the
    columns are multi - a batch of exactly one symbol crashed the top-1000
    banking on its final batch (726 % 25 == 1) before this check existed.
    Raises KeyError when the symbol is absent.
    """
    if isinstance(raw.columns, pd.MultiIndex):
        return raw[symbol]
    return raw


def _fetch(batch, start):
    """Download a chunk and return one long-format frame."""
    raw = yf.download(batch, start=start, auto_adjust=False, actions=True,
                      group_by="ticker", progress=False, threads=False)
    if raw.empty:
        return pd.DataFrame()

    frames = []
    for symbol in batch:
        try:
            sub = _symbol_frame(raw, symbol)
        except KeyError:
            continue
        sub = sub.dropna(how="all")
        if sub.empty:
            continue
        sub = sub.rename(columns=RENAMES).reset_index()
        sub["symbol"] = symbol
        frames.append(sub)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def stale_from_source(con, symbols, previous_session):
    """[(symbol, newest bar date)] whose newest stored bar is older than
    `previous_session` - the source answered, but with old data. Rows
    coming back is not the same as being current."""
    marks = ", ".join("?" for _ in symbols)
    rows = con.execute(
        f"SELECT symbol, MAX(date) FROM {TABLE} WHERE symbol IN ({marks}) "
        f"GROUP BY symbol HAVING MAX(date) < ? ORDER BY symbol",
        [*symbols, previous_session]).fetchall()
    return [(s, str(d)) for s, d in rows]


def record_splits(con, frame, source="yfinance"):
    """Log every split in `frame` (symbol, date, split_ratio) as a
    corporate_actions event the first time it is seen. 0, NaN and 1 all mean
    "no split" in the source. A ratio that disagrees with the log prints a
    SPLIT RATIO CONFLICT line and the log keeps what it first recorded.
    Returns the number of new events."""
    if frame.empty or "split_ratio" not in frame:
        return 0
    ratio = pd.to_numeric(frame["split_ratio"], errors="coerce")
    splits = frame.loc[ratio.notna() & (ratio != 0) & (ratio != 1.0),
                       ["symbol", "date", "split_ratio"]]
    events = [{"symbol": s, "date": d, "ratio": r}
              for s, d, r in splits.itertuples(index=False)]
    inserted, conflicts = record_actions(con, events, source=source)
    for symbol, day, _, logged, reported in conflicts:
        print(f"  SPLIT RATIO CONFLICT {symbol} {day}: logged {logged:g}, "
              f"source reports {reported:g} - log unchanged", flush=True)
    return inserted


def backfill_prices(con, symbols, start):
    """Fetch and store raw history for symbols; idempotent per (symbol,
    window). Returns (rows_inserted, missing_symbols)."""
    con.execute(SCHEMA)
    # Splits stored before the event log existed are recorded from bronze;
    # idempotent, so an already-seeded log costs one query.
    record_splits(con, con.execute(
        f"SELECT symbol, date, split_ratio FROM {TABLE} "
        "WHERE split_ratio > 0 AND split_ratio <> 1").df())
    run_id, ingest_ts = new_run_id(), now_ts()
    total, missing = 0, []

    for i in range(0, len(symbols), CHUNK):
        batch = symbols[i:i + CHUNK]
        df = _fetch(batch, start)
        got = set(df["symbol"]) if not df.empty else set()
        missing += [s for s in batch if s not in got]

        if not df.empty:
            df["date"] = pd.to_datetime(df["Date"]).dt.date
            # A run during or before the session drags in a partial bar
            # for today; incomplete sessions never reach bronze.
            df = df[df["date"] <= completed_session_cutoff()]
            record_splits(con, df)

        if not df.empty:
            df["_run_id"] = run_id
            df["_ingest_ts"] = ingest_ts
            df["_source_system"] = "yfinance"
            df["_source_event_ts"] = df["date"].astype(str) + "T00:00:00Z"
            df["_load_type"] = "full"
            for col in ("dividend", "split_ratio", "adj_close"):
                if col not in df:
                    df[col] = None
            cols = [c for c in con.execute(f"DESCRIBE {TABLE}").df()["column_name"]]
            df = df[cols]

            con.register("incoming", df)
            con.execute(
                f"DELETE FROM {TABLE} WHERE symbol IN "
                f"(SELECT DISTINCT symbol FROM incoming) AND date >= '{start}'"
            )
            con.execute(f"INSERT INTO {TABLE} SELECT * FROM incoming")
            con.unregister("incoming")
            total += len(df)

        print(f"  {min(i + CHUNK, len(symbols))}/{len(symbols)} symbols, "
              f"{total} rows", flush=True)
    return total, missing


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--years", type=int, help="window instead of full history")
    parser.add_argument("--limit", type=int, help="only the first N symbols")
    args = parser.parse_args()

    start = HISTORY_START_DATE
    if args.years:
        start = str((pd.Timestamp.today() - pd.DateOffset(years=args.years)).date())

    symbols = daily_symbols(TICKERS, BENCHMARK_TICKERS, load_positions())
    if args.limit:
        symbols = symbols[:args.limit]

    os.makedirs(os.path.dirname(WAREHOUSE), exist_ok=True)
    con = duckdb.connect(WAREHOUSE)
    total, missing = backfill_prices(con, symbols, start)

    fetched = ensure_quote_types(con, symbols)
    if fetched:
        print(f"  stored quote types for {fetched} new symbols")

    rows, syms, lo, hi = con.execute(
        f"SELECT COUNT(*), COUNT(DISTINCT symbol), MIN(date), MAX(date) FROM {TABLE}"
    ).fetchone()
    print(f"\n{WAREHOUSE}")
    print(f"  {rows} rows, {syms} symbols, {lo} to {hi}")
    if missing:
        print(f"  NO DATA for {len(missing)}: {', '.join(sorted(missing))}")
    expected = last_trading_session(completed_session_cutoff())
    previous = last_trading_session(expected - pd.Timedelta(days=1))
    old = stale_from_source(con, symbols, str(previous))
    if old:
        print(f"  STALE FROM SOURCE (newest bar older than {previous}): "
              + ", ".join(f"{s} {d}" for s, d in old))
    con.close()


if __name__ == "__main__":
    main()
