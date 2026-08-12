"""Broad universe: inventory -> rolling window -> tier -> line of sight.

    uv run python scripts/backfill_universe.py               # full refresh
    uv run python scripts/backfill_universe.py --top-n 1000
    uv run python scripts/backfill_universe.py --skip-fetch  # rebuild only

Task 13 (line of sight). Refresh cadence: monthly, alongside the rebalance.
Steps, each idempotent:

1. Snapshot the NYSE/Nasdaq common-stock inventory (bronze_listings).
2. Fetch a rolling ~2-year adjusted close+volume window for the whole
   inventory plus the watchlist and held names (bronze_prices_broad) -
   OTC ADRs in the watchlist are not in the exchange directory but must
   still appear in the ranked output.
3. Rank by trailing median dollar volume; append enter/exit events for the
   top-N tier (universe_membership, PIT-reconstructable).
4. Build gold_line_of_sight: EVERY symbol ranked, size as a tag never a
   filter, `emerging` flagging small caps that signal they might get big.
5. Bank full 2010+ raw history in bronze_prices for tier members that lack
   it - so the scored pipeline can expand to them later without refetching.
"""
import argparse
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(os.path.join(ROOT, ".env"))

import duckdb  # noqa: E402

from backfill import backfill_prices  # noqa: E402
from common.config import TICKERS, BENCHMARK_TICKERS, HISTORY_START_DATE  # noqa: E402
from common.listings import refresh_listings  # noqa: E402
from common.positions import held_symbols, load_positions  # noqa: E402
from common.universe import (  # noqa: E402
    build_line_of_sight, fetch_broad_window, rank_dollar_volume,
    update_membership,
)

WAREHOUSE = os.path.join(ROOT, "warehouse", "market.duckdb")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--top-n", type=int, default=1000)
    parser.add_argument("--skip-fetch", action="store_true",
                        help="reuse the existing broad window")
    args = parser.parse_args()

    con = duckdb.connect(WAREHOUSE)
    held = held_symbols(load_positions())
    watchlist = sorted(set(TICKERS))

    if not args.skip_fetch:
        n = refresh_listings(con)
        # flush everywhere below: this runs for many minutes in the
        # background and piped stdout is block-buffered.
        print(f"bronze_listings: {n} common stocks", flush=True)
        inventory = [r[0] for r in con.execute(
            "SELECT symbol FROM bronze_listings ORDER BY symbol").fetchall()]
        symbols = sorted(set(inventory) | set(watchlist) | set(held))
        rows = fetch_broad_window(con, symbols)
        print(f"bronze_prices_broad: {rows} rows for {len(symbols)} symbols",
              flush=True)

    top = rank_dollar_volume(con, args.top_n)
    entered, exited = update_membership(con, top)
    print(f"universe_membership: top {args.top_n} tier, "
          f"{entered} entered, {exited} exited", flush=True)

    total, emerging = build_line_of_sight(con, watchlist, held)
    print(f"gold_line_of_sight: {total} symbols ranked, {emerging} emerging")
    print("\ntop emerging by momentum percentile:")
    print(con.execute("""
        SELECT symbol, ROUND(close, 2) AS close,
               ROUND(mom_12_1 * 100, 1) AS mom_pct_chg,
               ROUND(mom_pct, 3) AS mom_rank,
               ROUND(dv_acceleration, 2) AS dv_accel,
               CAST(dollar_volume_63d AS BIGINT) AS dollar_vol
        FROM gold_line_of_sight WHERE emerging
        ORDER BY mom_pct DESC LIMIT 15
    """).df().to_string(index=False))

    # Bank full history for tier members we do not hold raw prices for.
    have = {r[0] for r in con.execute(
        "SELECT DISTINCT symbol FROM bronze_prices").fetchall()}
    gap = sorted(set(top["symbol"]) - have)
    if gap:
        print(f"\nbanking full history for {len(gap)} tier members",
              flush=True)
        rows, missing = backfill_prices(con, gap, HISTORY_START_DATE)
        print(f"  {rows} rows added"
              + (f"; no data for {len(missing)}" if missing else ""))
    con.close()


if __name__ == "__main__":
    main()
