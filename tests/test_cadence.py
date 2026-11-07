"""Daily cadence (tasks/SPEC-OPERATING-CADENCE.md, SPEC-GUARD item 3):
due-not-timed runs, a stop-at-first-failure chain with network retries,
an append-only ledger, and a run lock. No network, no clock, no launchd.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import pytest

from common.cadence import (
    RunLocked, append_ledger, daily_due, load_ledger, run_chain, run_lock,
)


def _entry(session, exit_code=0, job="daily"):
    return {"job": job, "session": session, "exit_code": exit_code}


def test_daily_is_due_when_nothing_has_run_for_the_session():
    assert daily_due([], "2026-11-06") is True


def test_daily_is_not_due_twice_for_one_session():
    # RunAtLoad plus the 18:00 firing must not run the chain twice.
    assert daily_due([_entry("2026-11-06")], "2026-11-06") is False


def test_a_failed_run_leaves_the_session_due_for_the_next_firing():
    assert daily_due([_entry("2026-11-06", exit_code=1)], "2026-11-06") is True


def test_a_missed_day_catches_up_on_the_next_wake():
    # Asleep through Thursday's firing: Friday's run covers Friday only;
    # the chain always refreshes through the latest completed session.
    assert daily_due([_entry("2026-11-04")], "2026-11-06") is True


def test_chain_stops_at_the_first_failing_step():
    ran = []

    def runner(step):
        ran.append(step["name"])
        return 1 if step["name"] == "build" else 0

    steps = [{"name": "fetch"}, {"name": "build"}, {"name": "guard"}]
    assert run_chain(steps, runner, sleep=lambda s: None) == (1, "build")
    assert ran == ["fetch", "build"]          # guard never ran


def test_network_step_retries_with_backoff_then_succeeds():
    attempts, waits = [], []

    def runner(step):
        attempts.append(step["name"])
        return 0 if len(attempts) == 3 else 1

    steps = [{"name": "fetch", "retries": [300, 900, 2700]}]
    assert run_chain(steps, runner, sleep=waits.append) == (0, None)
    assert attempts == ["fetch"] * 3 and waits == [300, 900]


def test_ledger_is_append_only_jsonl(tmp_path):
    path = tmp_path / "cadence.jsonl"
    append_ledger(path, _entry("2026-11-05"))
    append_ledger(path, _entry("2026-11-06", exit_code=1))
    assert [e["session"] for e in load_ledger(path)] == [
        "2026-11-05", "2026-11-06"]
    assert load_ledger(tmp_path / "none.jsonl") == []


def test_plist_renders_paths_at_install_time_never_from_a_tracked_file():
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..",
                                    "scripts"))
    from cadence import TEMPLATE, render_plist

    with open(TEMPLATE) as f:
        template = f.read()
    assert "/Users/" not in template          # no machine path is tracked
    plist = render_plist(template, "/tmp/repo", "/opt/bin/uv")
    assert "{{" not in plist
    assert "<string>/tmp/repo</string>" in plist
    assert "/opt/bin/uv" in plist and "/usr/bin/caffeinate" in plist
    assert "<key>RunAtLoad</key>" in plist


def test_run_lock_refuses_a_second_concurrent_run(tmp_path):
    lock = tmp_path / "cadence.lock"
    with run_lock(lock):
        with pytest.raises(RunLocked):
            with run_lock(lock):
                pass
    with run_lock(lock):                      # released after the first
        pass
