"""Is the warehouse current? Exit 0 fresh, 1 stale, 2 no warehouse.

    uv run python scripts/fresh.py

Fresh means bronze_prices' latest date equals the most recent completed
NYSE session (common.quality: completed_session_cutoff rolled back to a
trading day). The /recs skill runs this before any report and refreshes
on exit 1, so reports never silently read week-old prices.
"""
import os
import sys
from datetime import timedelta

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

from common.positions import held_symbols, load_positions  # noqa: E402
from common.quality import completed_session_cutoff, last_trading_session  # noqa: E402

WAREHOUSE = os.path.join(ROOT, "warehouse", "market.duckdb")


def check(warehouse=WAREHOUSE, now=None):
    """(latest_bar, expected_session) - fresh when equal."""
    import duckdb
    expected = last_trading_session(completed_session_cutoff(now))
    con = duckdb.connect(warehouse, read_only=True)
    latest = con.execute("SELECT MAX(date) FROM bronze_prices").fetchone()[0]
    con.close()
    return latest, expected


def held_status(warehouse, symbols, now=None):
    """Per held symbol, not the global max: {'behind': [...], 'dead': [...]}.

    'behind' = exactly one session short (a fetch glitch; the gate fails so
    a refresh runs). 'dead' = further behind or never priced (delisted or
    renamed; reported loudly, never blocking the other names).
    """
    import duckdb
    expected = last_trading_session(completed_session_cutoff(now))
    previous = last_trading_session(expected - timedelta(days=1))
    con = duckdb.connect(warehouse, read_only=True)
    marks = ", ".join("?" for _ in symbols)
    last = dict(con.execute(
        f"SELECT symbol, MAX(date) FROM bronze_prices WHERE symbol IN "
        f"({marks}) GROUP BY symbol", list(symbols)).fetchall()) \
        if symbols else {}
    con.close()
    behind = sorted(s for s in symbols if last.get(s) == previous)
    dead = sorted(s for s in symbols
                  if last.get(s) is None or last[s] < previous)
    return {"behind": behind, "dead": dead}


def main():
    if not os.path.exists(WAREHOUSE):
        print("no warehouse - run scripts/backfill.py")
        return 2
    latest, expected = check()
    if latest is None or str(latest) < str(expected):
        print(f"STALE: bronze through {latest}, last completed session "
              f"{expected} - refresh with backfill.py + build_local.py")
        return 1
    held = held_symbols(load_positions())
    status = held_status(WAREHOUSE, held) if held else {"behind": [],
                                                         "dead": []}
    if status["dead"]:
        print(f"DEAD HELD TICKERS (no price for 2+ sessions - check the "
              f"broker): {', '.join(status['dead'])}")
    if status["behind"]:
        print(f"STALE: held names one session behind {expected}: "
              f"{', '.join(status['behind'])} - refresh with backfill.py")
        return 1
    print(f"fresh: bronze through {latest} (last completed session "
          f"{expected}); every live held name is current")
    return 0


if __name__ == "__main__":
    sys.exit(main())
