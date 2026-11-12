"""Can today's data be trusted? Exit 0 yes, 1 no, 2 no warehouse.

    uv run python scripts/fresh.py

Exit 0 needs all of: bronze_prices' latest date equals the most recent
completed NYSE session (common.quality: completed_session_cutoff rolled back
to a trading day); no held name is one session behind; every live held name
has that session in silver_adjusted_prices; the daily symbols' stale rate is
within tolerance; no known close fails; no held name has an unruled
adjustment mismatch. A held name two or more sessions behind is a dead
listing: printed, never blocking.

The daily chain runs this after the build and before the guard, and the
/recs skill runs it before any report and refreshes on exit 1, so nothing
reads week-old or unproven prices silently.
"""
import os
import sys
from datetime import timedelta

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

from common.adjustments import divergent  # noqa: E402
from common.config import TICKERS, BENCHMARK_TICKERS  # noqa: E402
from common.known_closes import check_known_closes  # noqa: E402
from common.positions import (  # noqa: E402
    daily_symbols, held_symbols, load_positions,
)
from common.quality import (  # noqa: E402
    check_freshness, completed_session_cutoff, last_trading_session,
)
from common.source_defects import known_defects  # noqa: E402
from common.tables import has_table  # noqa: E402

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


def gate(warehouse, held, symbols, now=None, known_rows=None):
    """(problems, notes) beyond the bronze dates: each problem is a
    (check, detail) the guard must not run on; notes are printed only.

    `symbols` is the daily fetch set. Dead held names leave the critical set
    first: check_freshness fails on any stale critical symbol, and a dead
    listing must not block every other holding.
    """
    import duckdb
    expected = last_trading_session(completed_session_cutoff(now))
    dead = set(held_status(warehouse, held, now)["dead"])
    live = sorted(s for s in held if s not in dead)
    problems, notes = [], []
    con = duckdb.connect(warehouse, read_only=True)
    try:
        latest = con.execute("SELECT MAX(date) FROM bronze_prices").fetchone()[0]
        if latest is not None and latest > expected:
            problems.append(("future bar", f"bronze holds {latest}, after "
                             f"the last completed session {expected}"))

        if not has_table(con, "silver_adjusted_prices"):
            problems.append(("adjusted prices", "no silver_adjusted_prices "
                             "table - run build_local.py"))
        elif live:
            marks = ", ".join("?" for _ in live)
            adjusted = dict(con.execute(
                f"SELECT symbol, CAST(MAX(date) AS DATE) FROM "
                f"silver_adjusted_prices WHERE symbol IN ({marks}) "
                f"GROUP BY symbol", live).fetchall())
            short = [s for s in live
                     if adjusted.get(s) is None or adjusted[s] < expected]
            if short:
                problems.append(("adjusted prices", f"{len(short)} held "
                                 f"name(s) lack {expected}: {', '.join(short)}"))

        marks = ", ".join("?" for _ in symbols)
        stored = dict(con.execute(
            f"SELECT symbol, MAX(date) FROM bronze_prices WHERE symbol IN "
            f"({marks}) GROUP BY symbol", list(symbols)).fetchall()) \
            if symbols else {}
        errors, warnings = check_freshness(
            {s: stored.get(s) for s in symbols}, expected, set(live))
        problems += [("stale rate", e) for e in errors]
        notes += warnings

        known = check_known_closes(con, rows=known_rows)
        failed = [f"{s} {d}" for s, d, _, _, status in known
                  if status == "FAIL"]
        if failed:
            problems.append(("known closes", f"FAIL {len(failed)}: "
                             f"{', '.join(failed)}"))
        # A reference close this warehouse does not hold proves nothing. It
        # is said out loud and does not block: a clone with another
        # watchlist holds none of them.
        absent = sum(1 for row in known if row[4] == "absent")
        if absent:
            notes.append(f"known closes: {absent} of {len(known)} not in "
                         "this warehouse, not checked")

        ruled = known_defects("adjustment mismatch")
        new = [f"{s} {d:.1%}" for s, d in divergent(con, live)
               if s not in ruled]
        if new:
            problems.append(("adjustment mismatch", f"{len(new)} held "
                             f"name(s), not ruled: {', '.join(new)}"))
    finally:
        con.close()
    return problems, notes


def main():
    if not os.path.exists(WAREHOUSE):
        print("no warehouse - run scripts/backfill.py")
        return 2
    latest, expected = check()
    if latest is None or str(latest) < str(expected):
        print(f"STALE: bronze through {latest}, last completed session "
              f"{expected} - refresh with backfill.py + build_local.py")
        return 1
    positions = load_positions()
    held = held_symbols(positions)
    status = held_status(WAREHOUSE, held) if held else {"behind": [],
                                                         "dead": []}
    if status["dead"]:
        print(f"DEAD HELD TICKERS (no price for 2+ sessions - check the "
              f"broker): {', '.join(status['dead'])}")
    if status["behind"]:
        print(f"STALE: held names one session behind {expected}: "
              f"{', '.join(status['behind'])} - refresh with backfill.py")
        return 1
    problems, notes = gate(
        WAREHOUSE, held, daily_symbols(TICKERS, BENCHMARK_TICKERS, positions))
    for note in notes:
        print(note)
    for name, detail in problems:
        print(f"NOT TRUSTED: {name} - {detail}")
    if problems:
        return 1
    print(f"fresh: bronze through {latest} (last completed session "
          f"{expected}); every live held name is current, and the stale "
          f"rate, known closes and held adjustments pass")
    return 0


if __name__ == "__main__":
    sys.exit(main())
