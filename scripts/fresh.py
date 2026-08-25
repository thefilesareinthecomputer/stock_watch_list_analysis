"""Is the warehouse current? Exit 0 fresh, 1 stale, 2 no warehouse.

    uv run python scripts/fresh.py

Fresh means bronze_prices' latest date equals the most recent completed
NYSE session (common.quality: completed_session_cutoff rolled back to a
trading day). The /recs skill runs this before any report and refreshes
on exit 1, so reports never silently read week-old prices.
"""
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

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


def main():
    if not os.path.exists(WAREHOUSE):
        print("no warehouse - run scripts/backfill.py")
        return 2
    latest, expected = check()
    if latest is not None and str(latest) >= str(expected):
        print(f"fresh: bronze through {latest} (last completed session "
              f"{expected})")
        return 0
    print(f"STALE: bronze through {latest}, last completed session "
          f"{expected} - refresh with backfill.py + build_local.py")
    return 1


if __name__ == "__main__":
    sys.exit(main())
