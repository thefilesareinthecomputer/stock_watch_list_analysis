"""Daily cadence core: what is due, the step chain, the ledger, the lock.

Spec: tasks/SPEC-OPERATING-CADENCE.md (private) and SPEC-GUARD item 3.
The schedule died silently on 2026-08-24 because session-scoped timers
expired; this is the durable replacement, driven by launchd and judged
by "is it due", never "is it the right time", so a run missed while the
Mac slept happens on the next wake.

No model sits in this path. Every step is an idempotent script, so a run
cut short by sleep is simply rerun.
"""
import fcntl
from contextlib import contextmanager

from common.jsonl import append_jsonl, read_jsonl


class RunLocked(RuntimeError):
    """Another cadence run holds the lock."""


load_ledger = read_jsonl


def append_ledger(path, entry):
    """Append one run record; earlier lines are never rewritten."""
    append_jsonl(path, [entry])


def daily_due(ledger, session):
    """True unless a successful daily run already covered `session` (the
    latest completed XNYS session). A failed run leaves it due, so the
    next firing - or the next wake - retries."""
    return not any(e.get("job") == "daily" and e.get("session") == session
                   and e.get("exit_code") == 0 for e in ledger)


def run_chain(steps, runner, sleep):
    """Run steps in order; stop at the first failure.

    A step may carry `retries`: seconds to wait before each retry (network
    is often down right after wake). Returns (exit_code, failed step name
    or None). `runner(step)` returns the step's exit code.
    """
    for step in steps:
        waits = list(step.get("retries", []))
        code = runner(step)
        while code != 0 and waits:
            sleep(waits.pop(0))
            code = runner(step)
        if code != 0:
            return code, step["name"]
    return 0, None


@contextmanager
def run_lock(path):
    """Exclusive, non-blocking flock: one cadence run at a time. The kernel
    drops the lock if the process dies, so a killed run cannot leave a
    stale lock behind."""
    handle = open(path, "a")
    try:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RunLocked(f"another cadence run holds {path}") from None
        yield
    finally:
        handle.close()
