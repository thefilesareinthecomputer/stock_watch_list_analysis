"""Daily data-proof report: can the data be trusted today? (STORY-000013)

    uv run python scripts/data_proof.py        # writes reports/data_proof_<session>.md

One page, written by the daily cadence after the build, listing each check
with its count and the symbols it names:

- vintage: newest bronze session against the last completed session, and
  whether bronze is in traded terms
- held names: dead (two or more sessions missing) and behind (one)
- stale from source: daily symbols whose newest bar predates the previous
  session even though the source answered
- source revisions: closes the source changed for sessions already stored,
  first seen in the last day (bronze keeps the stored value)
- known closes: bronze against the published closes in
  tests/fixtures/known_closes.json
- adjustment mismatch: symbols whose derived adjusted series diverges from
  Yahoo's adj_close beyond the build tolerance; a symbol ruled on in
  src/common/source_defects.json is listed as known, not as attention

A clean day states "all checks passed" with the vintage date. The exit code
is always 0: the report informs, the guard that follows decides.
"""
import os
import sys
from datetime import datetime, timedelta, timezone

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))
sys.path.insert(0, os.path.join(ROOT, "scripts"))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(os.path.join(ROOT, ".env"))

import duckdb  # noqa: E402

from common.adjustments import ADJ_TOLERANCE, divergent  # noqa: E402
from common.config import TICKERS, BENCHMARK_TICKERS  # noqa: E402
from common.known_closes import check_known_closes  # noqa: E402
from common.positions import daily_symbols, held_symbols, load_positions  # noqa: E402
from common.quality import completed_session_cutoff, last_trading_session  # noqa: E402
from common.source_defects import known_defects  # noqa: E402
from common.source_revisions import revisions_since  # noqa: E402
from common.tables import has_table  # noqa: E402
from common.warehouse_meta import bronze_is_traded  # noqa: E402

WAREHOUSE = os.path.join(ROOT, "warehouse", "market.duckdb")
REPORTS = os.path.join(ROOT, "reports")
POSITIONS = os.path.join(ROOT, "knowledge", "POSITIONS.md")
WARMUP_ROWS = 200          # below this the build computes no indicators


def _names(items, limit=25):
    shown = ", ".join(items[:limit])
    return shown + (f", +{len(items) - limit} more" if len(items) > limit else "")


def check_vintage(con, now=None):
    latest = con.execute("SELECT MAX(date) FROM bronze_prices").fetchone()[0]
    expected = last_trading_session(completed_session_cutoff(now))
    terms = "traded" if bronze_is_traded(con) else "SERVED (not migrated)"
    ok = latest is not None and latest >= expected and terms == "traded"
    return ("vintage", ok,
            f"bronze through {latest}, last completed session {expected}; "
            f"terms: {terms}", latest)


def check_held(con, held, now=None):
    if not held:
        return ("held names", True, "no positions file; nothing to check")
    expected = last_trading_session(completed_session_cutoff(now))
    previous = last_trading_session(expected - timedelta(days=1))
    marks = ", ".join("?" for _ in held)
    last = dict(con.execute(
        f"SELECT symbol, MAX(date) FROM bronze_prices WHERE symbol IN ({marks}) "
        "GROUP BY symbol", list(held)).fetchall())
    dead = sorted(s for s in held if last.get(s) is None or last[s] < previous)
    behind = sorted(s for s in held if last.get(s) == previous)
    parts = []
    if dead:
        parts.append(f"DEAD {len(dead)}: {_names(dead)}")
    if behind:
        parts.append(f"BEHIND {len(behind)}: {_names(behind)}")
    return ("held names", not parts,
            "; ".join(parts) or f"all {len(held)} held names current through {expected}")


def check_stale_from_source(con, symbols, now=None):
    if not symbols:
        return ("stale from source", True, "no daily symbols configured")
    expected = last_trading_session(completed_session_cutoff(now))
    previous = last_trading_session(expected - timedelta(days=1))
    marks = ", ".join("?" for _ in symbols)
    rows = con.execute(
        f"SELECT symbol, MAX(date) FROM bronze_prices WHERE symbol IN ({marks}) "
        "GROUP BY symbol HAVING MAX(date) < ? ORDER BY symbol",
        [*symbols, previous]).fetchall()
    named = [f"{s} {d}" for s, d in rows]
    return ("stale from source", not named,
            f"{len(named)}: {_names(named)}" if named
            else f"all {len(symbols)} daily symbols within one session of {expected}")


def check_revisions(con, now=None):
    now = now or datetime.now(timezone.utc)
    since = (now - timedelta(days=1)).isoformat().replace("+00:00", "Z")
    rows = revisions_since(con, since)
    named = [f"{s} {d} {stored:.2f}->{src:.2f}" for s, d, stored, src in rows]
    return ("source revisions (24h)", not named,
            f"{len(named)}: {_names(named)}" if named else "none")


def check_known(con):
    results = check_known_closes(con)
    present = [r for r in results if r[4] != "absent"]
    failed = [f"{s} {d} bronze {v:.2f} vs {p}" for s, d, p, v, st in present
              if st == "FAIL"]
    return ("known closes", not failed,
            f"FAIL {len(failed)}: {_names(failed)}" if failed
            else f"{len(present)}/{len(results)} present, all match within a cent")


def check_adjustment(con):
    if not has_table(con, "corporate_actions"):
        return ("adjustment mismatch", False, "no corporate_actions log")
    symbols = [s for (s,) in con.execute(
        f"SELECT symbol FROM bronze_prices GROUP BY symbol "
        f"HAVING COUNT(*) >= {WARMUP_ROWS} ORDER BY symbol").fetchall()]
    ruled = known_defects("adjustment mismatch")
    bad, known = [], []
    for symbol, divergence in divergent(con, symbols):
        if symbol in ruled:
            known.append(f"{symbol} {divergence:.1%} (ruled {ruled[symbol]['ruled']})")
        else:
            bad.append((divergence, f"{symbol} {divergence:.1%}"))
    bad.sort(reverse=True)
    named = [text for _, text in bad]
    detail = (f"{len(named)} new of {len(symbols)}: {_names(named)}" if named
              else f"no new mismatch across {len(symbols)} symbols")
    if known:
        detail += f"; known source defects {len(known)}: {_names(known)}"
    return (f"adjustment mismatch (> {ADJ_TOLERANCE:.0%} vs Yahoo adj_close)",
            not named, detail)


def run_checks(con, symbols, held, now=None):
    vintage = check_vintage(con, now)
    checks = [vintage[:3], check_held(con, held, now),
              check_stale_from_source(con, symbols, now), check_revisions(con, now),
              check_known(con), check_adjustment(con)]
    return checks, vintage[3]


def report_path(now=None):
    """The report file for the last completed session. The guard report from
    the same run is named by that session too, so a run has one date."""
    session = last_trading_session(completed_session_cutoff(now))
    return os.path.join(REPORTS, f"data_proof_{session}.md")


def render(checks, latest, session, written=None):
    lines = [f"# Data proof - {session}", "",
             f"Warehouse vintage: bronze through {latest}."
             + (f" Written {written}." if written else ""), "",
             "| check | status | detail |", "|---|---|---|"]
    for name, ok, detail in checks:
        lines.append(f"| {name} | {'ok' if ok else 'ATTENTION'} | {detail} |")
    lines.append("")
    failed = [name for name, ok, _ in checks if not ok]
    if failed:
        lines.append(f"{len(failed)} check(s) need attention: {', '.join(failed)}.")
    else:
        lines.append(f"All checks passed for the {latest} vintage.")
    return "\n".join(lines) + "\n"


def main():
    if not os.path.exists(WAREHOUSE):
        print("no warehouse - run scripts/backfill.py")
        return 0
    positions = load_positions(POSITIONS) if os.path.exists(POSITIONS) else None
    held = held_symbols(positions) if positions else []
    symbols = daily_symbols(TICKERS, BENCHMARK_TICKERS, positions)
    con = duckdb.connect(WAREHOUSE, read_only=True)
    try:
        checks, latest = run_checks(con, symbols, held)
    finally:
        con.close()
    path = report_path()
    session = os.path.basename(path)[len("data_proof_"):-len(".md")]
    text = render(checks, latest, session,
                  written=datetime.now().isoformat(timespec="seconds"))
    os.makedirs(REPORTS, exist_ok=True)
    with open(path, "w") as f:
        f.write(text)
    print(text, end="")
    print(f"written {path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
