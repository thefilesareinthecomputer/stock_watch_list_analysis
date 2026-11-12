"""Append traded prices and corporate actions to the local DuckDB warehouse.

    uv run python scripts/backfill.py              # new sessions; full history for new symbols
    uv run python scripts/backfill.py --years 3    # shorter history for NEW symbols only
    uv run python scripts/backfill.py --limit 20   # smoke test

bronze_prices holds the OHLCV each symbol actually traded at, plus dividends
and splits in the same terms - never an adjusted series. Yahoo serves history
restated into today's share terms and `auto_adjust=True` rescales it on every
dividend, so nothing served can be the evidence layer: a score computed last
month would silently change this month. Each fetched row is converted back to
traded terms through the corporate_actions split log (common.traded_prices)
before it is stored, and the adjustment is derived on demand.

Append-only. A symbol with history is fetched from a short overlap before its
newest stored session and only newer sessions are inserted; a symbol without
history is fetched from `start`. Stored rows are never rewritten. The run
refuses a warehouse whose bronze is still in served terms
(warehouse_meta.bronze_terms): run scripts/migrate_traded_prices.py first.
"""
import argparse
import math
import os
import sys
from datetime import date, timedelta

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(os.path.join(ROOT, ".env"))

import duckdb  # noqa: E402
import pandas as pd  # noqa: E402
import yfinance as yf  # noqa: E402

from common.config import TICKERS, BENCHMARK_TICKERS, HISTORY_START_DATE  # noqa: E402
from common.corporate_actions import record_actions, splits_for  # noqa: E402
from common.positions import daily_symbols, load_positions  # noqa: E402
from common.quality import (  # noqa: E402
    STALE_TOLERANCE, completed_session_cutoff, last_trading_session,
)
from common.run_context import new_run_id, now_ts  # noqa: E402
from common.security import ensure_quote_types  # noqa: E402
from common.source_revisions import TOLERANCE, record_revisions  # noqa: E402
from common.traded_prices import to_traded, unrestated_events, without  # noqa: E402
from common.warehouse_meta import (  # noqa: E402
    BRONZE_TERMS, TRADED, bronze_is_traded, set_meta,
)

WAREHOUSE = os.path.join(ROOT, "warehouse", "market.duckdb")
TABLE = "bronze_prices"
CHUNK = 25
# Sessions re-fetched before a symbol's newest stored row. They are never
# re-inserted; they are the window in which a source revision can be seen.
OVERLAP_DAYS = 7
# A symbol stored within this many days of the warehouse's newest bar was
# trading at the last good run, so the fetch owes it the expected session.
LIVE_WINDOW_DAYS = 7

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


def _require_traded_terms(con):
    """Appending onto Yahoo's restated history would mix two share terms in
    one table, so refuse. An empty table is trivially in traded terms."""
    if bronze_is_traded(con):
        return
    if con.execute(f"SELECT COUNT(*) FROM {TABLE}").fetchone()[0] == 0:
        set_meta(con, BRONZE_TERMS, TRADED)
        return
    sys.exit(f"{TABLE} is still in served (restated) terms; run "
             "scripts/migrate_traded_prices.py --backup-dir DIR first")


def _last_stored(con, symbols):
    """{symbol: newest stored session} for the symbols that have rows."""
    marks = ", ".join("?" for _ in symbols)
    return dict(con.execute(
        f"SELECT symbol, MAX(date) FROM {TABLE} WHERE symbol IN ({marks}) "
        "GROUP BY symbol", list(symbols)).fetchall())


def live_shortfall(before, after, expected, symbols):
    """(short, live): the live symbols still lacking the expected session.

    `before` and `after` are {symbol: newest stored session} around the
    fetch. A symbol is live unless it was already stored more than
    LIVE_WINDOW_DAYS before the warehouse's own newest bar: a name that
    stopped weeks ago is stale from source and returns nothing on every
    run, while an outage leaves every live name short. A symbol with nothing
    stored yet is live, so a new watchlist cannot pass an outage.
    """
    floor = (max(before.values()) - timedelta(days=LIVE_WINDOW_DAYS)
             if before else None)
    live = sorted(s for s in symbols
                  if s not in before or before[s] >= floor)
    short = [s for s in live if after.get(s) is None or after[s] < expected]
    return short, live


def _traded_rows(con, df):
    """Fetched rows converted to traded terms, symbol by symbol, using the
    split log (which record_splits has just brought up to date). A decisive
    split the source served unrestated is printed and left out."""
    frames = []
    for symbol, sub in df.groupby("symbol", sort=False):
        splits = splits_for(con, symbol)
        events = unrestated_events(sub, splits)
        for day, ratio, jump in events:
            print(f"  SPLIT NOT RESTATED BY SOURCE {symbol} {str(day)[:10]} ratio "
                  f"{ratio:g}: served close moved {math.exp(jump) - 1:+.1%}; "
                  "left as served", flush=True)
        frames.append(to_traded(sub, without(splits, events)))
    return pd.concat(frames, ignore_index=True)


def _log_revisions(con, df, last):
    """Overlap rows (sessions already stored) whose served close, in traded
    terms, differs from the stored close. Each is logged once in
    source_revisions and printed; bronze keeps the stored value."""
    newest = df["symbol"].map(last)
    overlap = df[newest.notna() & (df["date"] <= newest)]
    if overlap.empty:
        return []
    con.register("overlap", overlap[["symbol", "date", "close"]])
    diffs = con.execute(
        f"SELECT o.symbol, o.date, p.close, o.close FROM overlap o "
        f"JOIN {TABLE} p USING (symbol, date) "
        f"WHERE ABS(p.close - o.close) > {TOLERANCE} ORDER BY 1, 2").fetchall()
    con.unregister("overlap")
    new = record_revisions(con, diffs)
    for symbol, day, stored, source in new:
        print(f"  SOURCE REVISION {symbol} {day}: stored {stored:.4f}, source "
              f"now {source:.4f} - stored value kept", flush=True)
    return new


def _store_new_sessions(con, df, last, run_id, ingest_ts):
    """Insert the fetched rows bronze does not hold yet (newer sessions and
    gaps inside the fetched window); return the count. Rows already stored
    are never written."""
    symbols = sorted(set(df["symbol"]))
    marks = ", ".join("?" for _ in symbols)
    stored = set(con.execute(
        f"SELECT symbol, date FROM {TABLE} WHERE symbol IN ({marks}) "
        "AND date >= ?", [*symbols, df["date"].min()]).fetchall())
    keep = [(s, d) not in stored for s, d in zip(df["symbol"], df["date"])]
    new = df[keep].copy()
    if new.empty:
        return 0
    new["_run_id"] = run_id
    new["_ingest_ts"] = ingest_ts
    new["_source_system"] = "yfinance"
    new["_source_event_ts"] = new["date"].astype(str) + "T00:00:00Z"
    new["_load_type"] = new["symbol"].map(
        lambda s: "incremental" if s in last else "full")
    for col in ("dividend", "split_ratio", "adj_close"):
        if col not in new:
            new[col] = None
    cols = [c for c in con.execute(f"DESCRIBE {TABLE}").df()["column_name"]]
    con.register("incoming", new[cols])
    con.execute(f"INSERT INTO {TABLE} SELECT * FROM incoming")
    con.unregister("incoming")
    return len(new)


def backfill_prices(con, symbols, start):
    """Append each symbol's sessions newer than what is stored, in traded
    terms; a symbol with no history is loaded from `start`. Returns
    (rows_inserted, missing_symbols)."""
    con.execute(SCHEMA)
    _require_traded_terms(con)
    # Splits stored before the event log existed are recorded from bronze;
    # idempotent, so an already-seeded log costs one query.
    record_splits(con, con.execute(
        f"SELECT symbol, date, split_ratio FROM {TABLE} "
        "WHERE split_ratio > 0 AND split_ratio <> 1").df())
    run_id, ingest_ts = new_run_id(), now_ts()
    total, missing = 0, []

    last = _last_stored(con, symbols)
    known = [s for s in symbols if s in last]
    fresh = [s for s in symbols if s not in last]
    groups = []
    for i in range(0, len(known), CHUNK):
        batch = known[i:i + CHUNK]
        overlap = min(last[s] for s in batch) - timedelta(days=OVERLAP_DAYS)
        groups.append((batch, str(overlap)))
    for i in range(0, len(fresh), CHUNK):
        groups.append((fresh[i:i + CHUNK], start))

    done = 0
    for batch, batch_start in groups:
        df = _fetch(batch, batch_start)
        got = set(df["symbol"]) if not df.empty else set()
        missing += [s for s in batch if s not in got]

        if not df.empty:
            df["date"] = pd.to_datetime(df["Date"]).dt.date
            # A run during or before the session drags in a partial bar
            # for today; incomplete sessions never reach bronze.
            df = df[df["date"] <= completed_session_cutoff()]
        if not df.empty:
            record_splits(con, df)
            df = _traded_rows(con, df)
            _log_revisions(con, df, last)
            total += _store_new_sessions(con, df, last, run_id, ingest_ts)

        done += len(batch)
        print(f"  {done}/{len(symbols)} symbols, {total} rows", flush=True)
    return total, missing


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--years", type=int,
                        help="history window for symbols with no stored rows")
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
    con.execute(SCHEMA)
    before = _last_stored(con, symbols)
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
    short, live = live_shortfall(before, _last_stored(con, symbols),
                                 expected, symbols)
    con.close()
    # An outage returns empty frames, not an error. The daily chain retries
    # only on a non-zero exit, so a short fetch has to be one.
    if len(short) > STALE_TOLERANCE * len(live):
        print(f"  FETCH SHORT: {len(short)}/{len(live)} live symbols lack "
              f"{expected} - the source served nothing for them")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
