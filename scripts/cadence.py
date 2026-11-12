"""Durable daily schedule: refresh, rebuild, gate, guard - when due.

Spec: tasks/SPEC-OPERATING-CADENCE.md and SPEC-GUARD item 3 (private).
launchd fires `run` daily at 18:00 local and at load; it no-ops unless the
latest completed XNYS session has no successful run yet, so a firing
missed while the Mac slept runs on the next wake. Every run is recorded in
logs/cadence.jsonl (gitignored) - the evidence a later watchdog reads - and,
passed or failed, as one dated section on the private vault's daily page.

    uv run python scripts/cadence.py run            # what launchd runs
    uv run python scripts/cadence.py install        # print the plist only
    uv run python scripts/cadence.py install --yes  # write it to
                                                    # ~/Library/LaunchAgents
                                                    # and load it
    uv run python scripts/cadence.py uninstall --yes

The monthly round is NOT in this chain: rebalance.py stays manual until
its lateness guard ships.
"""
import argparse
import os
import shutil
import subprocess
import sys
import time
from datetime import date, datetime
from xml.sax.saxutils import escape

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

from common.cadence import (  # noqa: E402
    RunLocked, append_ledger, daily_due, load_ledger, run_chain, run_lock,
)
from common.daily_page import (  # noqa: E402
    HEADER, guard_counts, proof_status, render_section, upsert_section,
)
from common.quality import (  # noqa: E402
    completed_session_cutoff, last_trading_session,
)
from common.tables import has_table  # noqa: E402

LOGS = os.path.join(ROOT, "logs")
LEDGER = os.path.join(LOGS, "cadence.jsonl")
LOCK = os.path.join(LOGS, "cadence.lock")
WAREHOUSE = os.path.join(ROOT, "warehouse", "market.duckdb")
REPORTS = os.path.join(ROOT, "reports")
POSITIONS = os.path.join(ROOT, "knowledge", "POSITIONS.md")
# The daily page lives in the private vault (gitignored, never synced).
PAGE = os.path.join(ROOT, "PROJECTS", "stocks", "agents-daily.md")
TEMPLATE = os.path.join(ROOT, "ops", "launchd", "cadence.plist.template")
LABEL = "local.stock-watch.cadence"

# Network first, with 5/15/45-minute retries: the network is often down
# right after wake. fresh.py gates the guard on a complete warehouse.
DAILY_STEPS = [
    {"name": "backfill", "script": "scripts/backfill.py",
     "retries": [300, 900, 2700]},
    {"name": "build", "script": "scripts/build_local.py"},
    # The one page that says whether today's data can be trusted.
    {"name": "proof", "script": "scripts/data_proof.py"},
    {"name": "fresh", "script": "scripts/fresh.py"},
    # --dry-run until delivery ships: recording an episode nobody is told
    # about would mark it fired and the user would never hear of it.
    {"name": "guard", "script": "scripts/guard.py", "args": ["--dry-run"]},
]


def _runner(log):
    def run(step):
        log.write(f"\n== {datetime.now().isoformat(timespec='seconds')} "
                  f"{step['name']}\n")
        log.flush()
        return subprocess.run([sys.executable, step["script"],
                               *step.get("args", [])], cwd=ROOT,
                              stdout=log, stderr=subprocess.STDOUT).returncode
    return run


def _written_since(path, started):
    """The file's text if this run wrote it, else None: a report left by an
    earlier attempt at the same session must not be read as this run's."""
    if not os.path.exists(path):
        return None
    if os.path.getmtime(path) < datetime.fromisoformat(started).timestamp():
        return None
    with open(path) as f:
        return f.read()


def data_ages():
    """[(label, date or None)] for the inputs whose age the page states."""
    ages = []
    if os.path.exists(WAREHOUSE):
        import duckdb
        con = duckdb.connect(WAREHOUSE, read_only=True)
        try:
            for label, table, column in (
                    ("prices", "bronze_prices", "date"),
                    ("fundamentals", "bronze_fundamentals", "_ingest_ts"),
                    ("monthly screen", "gold_line_of_sight", "as_of_date")):
                newest = (con.execute(
                    f"SELECT MAX({column}) FROM {table}").fetchone()[0]
                    if has_table(con, table) else None)
                ages.append((label, None if newest is None
                             else date.fromisoformat(str(newest)[:10])))
        finally:
            con.close()
    if os.path.exists(POSITIONS):
        ages.append(("positions file",
                     date.fromtimestamp(os.path.getmtime(POSITIONS))))
    return ages


def write_page(session, entry, ledger):
    """File this run's section on the daily page, passed or failed. Returns
    the page path, or None when there is no vault (a fresh clone)."""
    if not os.path.isdir(os.path.dirname(PAGE)):
        print(f"no vault at {os.path.dirname(PAGE)} - daily page skipped")
        return None
    proof = _written_since(
        os.path.join(REPORTS, f"data_proof_{session}.md"), entry["started"])
    guard = _written_since(
        os.path.join(REPORTS, f"guard_{session}.md"), entry["started"])
    covered = [e["session"] for e in ledger
               if e.get("job") == "daily" and e.get("exit_code") == 0]
    dry_run = any(s["name"] == "guard" and "--dry-run" in s.get("args", [])
                  for s in DAILY_STEPS)
    section = render_section(
        session, entry, proof_status(proof) if proof else None,
        guard_counts(guard) if guard else None, data_ages(), date.today(),
        last_covered=max(covered) if covered else None, dry_run=dry_run)
    page = HEADER
    if os.path.exists(PAGE):
        with open(PAGE) as f:
            page = f.read()
    # The page is not regenerable, so it is replaced whole: a failure while
    # building or writing the new text leaves the old page as it was.
    text = upsert_section(page, session, section)
    with open(PAGE + ".tmp", "w") as f:
        f.write(text)
    os.replace(PAGE + ".tmp", PAGE)
    return PAGE


def cmd_run():
    os.makedirs(LOGS, exist_ok=True)
    session = str(last_trading_session(completed_session_cutoff()))
    try:
        with run_lock(LOCK):
            if not daily_due(load_ledger(LEDGER), session):
                print(f"nothing due: session {session} already covered")
                return 0
            started = datetime.now().isoformat(timespec="seconds")
            with open(os.path.join(LOGS, "cadence.log"), "a") as log:
                code, failed = run_chain(DAILY_STEPS, _runner(log), time.sleep)
            entry = {
                "job": "daily", "session": session, "started": started,
                "finished": datetime.now().isoformat(timespec="seconds"),
                "exit_code": code, "step_failed": failed}
            append_ledger(LEDGER, entry)
            # Written after the chain, not as a step of it, so a failed run
            # still leaves a page. The page never changes the run's result.
            try:
                write_page(session, entry, load_ledger(LEDGER))
            except Exception as exc:
                print(f"daily page not written: {type(exc).__name__}: {exc}")
            print(f"daily {session}: "
                  + ("ok" if code == 0 else f"FAILED at {failed} ({code})"))
            return code
    except RunLocked as e:
        print(e)
        return 0


def render_plist(template, root, uv):
    # XML-escape the paths: a repo path containing & or < would otherwise
    # yield a plist launchctl rejects.
    return (template.replace("{{ROOT}}", escape(root))
            .replace("{{UV}}", escape(uv)).replace("{{LABEL}}", LABEL))


def _agent_path():
    return os.path.expanduser(f"~/Library/LaunchAgents/{LABEL}.plist")


def cmd_install(yes):
    uv = shutil.which("uv")
    if not uv:
        sys.exit("uv not found on PATH")
    with open(TEMPLATE) as f:
        plist = render_plist(f.read(), ROOT, uv)
    if not yes:
        print(plist)
        print(f"# dry run: rerun with --yes to write {_agent_path()} "
              "and load it")
        return 0
    os.makedirs(LOGS, exist_ok=True)
    with open(_agent_path(), "w") as f:
        f.write(plist)
    domain = f"gui/{os.getuid()}"
    subprocess.run(["launchctl", "bootout", domain, _agent_path()],
                   capture_output=True)
    return subprocess.run(["launchctl", "bootstrap", domain,
                           _agent_path()]).returncode


def cmd_uninstall(yes):
    if not yes:
        print(f"# dry run: rerun with --yes to unload and remove "
              f"{_agent_path()}")
        return 0
    subprocess.run(["launchctl", "bootout", f"gui/{os.getuid()}",
                    _agent_path()], capture_output=True)
    if os.path.exists(_agent_path()):
        os.remove(_agent_path())
    return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("run")
    for name in ("install", "uninstall"):
        p = sub.add_parser(name)
        p.add_argument("--yes", action="store_true")
    args = parser.parse_args()
    if args.cmd == "run":
        sys.exit(cmd_run())
    if args.cmd == "install":
        sys.exit(cmd_install(args.yes))
    sys.exit(cmd_uninstall(args.yes))


if __name__ == "__main__":
    main()
