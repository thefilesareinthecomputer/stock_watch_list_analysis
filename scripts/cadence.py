"""Durable daily schedule: refresh, rebuild, gate, guard - when due.

Spec: tasks/SPEC-OPERATING-CADENCE.md and SPEC-GUARD item 3 (private).
launchd fires `run` daily at 18:00 local and at load; it no-ops unless the
latest completed XNYS session has no successful run yet, so a firing
missed while the Mac slept runs on the next wake. Every run is recorded in
logs/cadence.jsonl (gitignored) - the evidence a later watchdog reads.

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
from datetime import datetime
from xml.sax.saxutils import escape

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

from common.cadence import (  # noqa: E402
    RunLocked, append_ledger, daily_due, load_ledger, run_chain, run_lock,
)
from common.quality import (  # noqa: E402
    completed_session_cutoff, last_trading_session,
)

LOGS = os.path.join(ROOT, "logs")
LEDGER = os.path.join(LOGS, "cadence.jsonl")
LOCK = os.path.join(LOGS, "cadence.lock")
TEMPLATE = os.path.join(ROOT, "ops", "launchd", "cadence.plist.template")
LABEL = "local.stock-watch.cadence"

# Network first, with 5/15/45-minute retries: the network is often down
# right after wake. fresh.py gates the guard on a complete warehouse.
DAILY_STEPS = [
    {"name": "backfill", "script": "scripts/backfill.py",
     "retries": [300, 900, 2700]},
    {"name": "build", "script": "scripts/build_local.py"},
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
            append_ledger(LEDGER, {
                "job": "daily", "session": session, "started": started,
                "finished": datetime.now().isoformat(timespec="seconds"),
                "exit_code": code, "step_failed": failed})
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
