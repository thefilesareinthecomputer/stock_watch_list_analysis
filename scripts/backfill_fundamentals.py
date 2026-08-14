"""Backfill SEC EDGAR CompanyFacts into the local DuckDB warehouse.

    uv run python scripts/backfill_fundamentals.py             # whole watchlist
    uv run python scripts/backfill_fundamentals.py --limit 20  # smoke test
    uv run python scripts/backfill_fundamentals.py --universe  # banked symbols
                                                   # without facts (resumable)

Fetches per-CIK companyfacts JSON for every watchlist symbol that resolves to
an SEC registrant. ETFs and foreign listings without EDGAR filings are
reported and skipped - they are excluded from fundamentals ranking anyway.

Facts are stored raw (as-filed, with the `filed` date that makes them
point-in-time); concept selection happens in silver. Idempotent per symbol.
"""
import argparse
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(os.path.join(ROOT, ".env"))

import duckdb  # noqa: E402
import requests  # noqa: E402

from common.config import TICKERS  # noqa: E402
from common.edgar import (  # noqa: E402
    extract_facts, fetch_companyfacts, fetch_entity, resolve_cik_fallback,
    resolve_ciks, universe_backfill_targets, upsert_entity, upsert_facts,
    user_agent,
)
from common.run_context import new_run_id, now_ts  # noqa: E402

WAREHOUSE = os.path.join(ROOT, "warehouse", "market.duckdb")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--limit", type=int, help="only the first N symbols")
    parser.add_argument("--universe", action="store_true",
                        help="target banked symbols (bronze_prices) without "
                             "facts yet, instead of the watchlist")
    args = parser.parse_args()

    con = duckdb.connect(WAREHOUSE)
    if args.universe:
        symbols = universe_backfill_targets(con)
        print(f"universe mode: {len(symbols)} banked symbols without facts",
              flush=True)
    else:
        symbols = sorted(set(TICKERS))
    if args.limit:
        symbols = symbols[:args.limit]

    session = requests.Session()
    session.headers["User-Agent"] = user_agent()

    ciks = resolve_ciks(session)
    for symbol in [s for s in symbols if s not in ciks]:
        fallback = resolve_cik_fallback(session, symbol)
        if fallback:
            ciks[symbol] = fallback
    unresolved = [s for s in symbols if s not in ciks]
    resolved = [s for s in symbols if s in ciks]

    run_id, ingest_ts = new_run_id(), now_ts()
    total, empty = 0, []

    for i, symbol in enumerate(resolved, 1):
        entity = fetch_entity(session, ciks[symbol])
        if entity:
            upsert_entity(con, symbol, ciks[symbol], entity, ingest_ts)
        payload = fetch_companyfacts(session, ciks[symbol])
        if payload is None:
            empty.append(symbol)
            continue
        df = extract_facts(symbol, ciks[symbol], payload)
        if df.empty:
            empty.append(symbol)
            continue
        total += upsert_facts(con, df, run_id, ingest_ts)
        if i % 25 == 0:
            print(f"  {i}/{len(resolved)} symbols, {total} facts", flush=True)

    rows, syms, lo, hi = con.execute(
        "SELECT COUNT(*), COUNT(DISTINCT symbol), MIN(filed), MAX(filed) "
        "FROM bronze_fundamentals"
    ).fetchone()
    print(f"\n{WAREHOUSE}")
    print(f"  bronze_fundamentals: {rows} facts, {syms} symbols, "
          f"filed {lo} to {hi}")
    if unresolved:
        print(f"  NO CIK for {len(unresolved)}: {', '.join(unresolved)}")
    if empty:
        print(f"  NO FACTS for {len(empty)}: {', '.join(empty)}")
    con.close()


if __name__ == "__main__":
    main()
