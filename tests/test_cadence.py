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
    odd = render_plist(template, "/tmp/a&b", "/opt/bin/uv")
    assert "/tmp/a&amp;b" in odd and "/tmp/a&b<" not in odd


def test_run_lock_refuses_a_second_concurrent_run(tmp_path):
    lock = tmp_path / "cadence.lock"
    with run_lock(lock):
        with pytest.raises(RunLocked):
            with run_lock(lock):
                pass
    with run_lock(lock):                      # released after the first
        pass


# --- the daily page: written after the chain, passed or failed


def _scratch_cadence(tmp_path, monkeypatch, result, vault=True):
    """scripts/cadence.py pointed at scratch paths, its chain stubbed to
    return `result`. No real ledger, report, warehouse or vault is touched."""
    import datetime as dt

    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..",
                                    "scripts"))
    import cadence

    page = tmp_path / "vault" / "agents-daily.md"
    if vault:
        page.parent.mkdir()
    reports = tmp_path / "reports"
    reports.mkdir()
    logs = tmp_path / "logs"
    for name, value in (
            ("LOGS", str(logs)), ("LEDGER", str(logs / "cadence.jsonl")),
            ("LOCK", str(logs / "cadence.lock")), ("PAGE", str(page)),
            ("REPORTS", str(reports)),
            ("WAREHOUSE", str(tmp_path / "none.duckdb")),
            ("POSITIONS", str(tmp_path / "none.md"))):
        monkeypatch.setattr(cadence, name, value)
    monkeypatch.setattr(cadence, "last_trading_session",
                        lambda reference: dt.date(2026, 11, 11))
    monkeypatch.setattr(cadence, "run_chain", result)
    return cadence, page, reports


def test_a_failed_backfill_still_writes_a_dated_section(tmp_path, monkeypatch):
    cadence, page, _ = _scratch_cadence(
        tmp_path, monkeypatch, lambda steps, runner, sleep: (1, "backfill"))
    assert cadence.cmd_run() == 1
    text = page.read_text()
    assert "## 2026-11-11 - FAILED at backfill" in text
    assert "Last session covered: none" in text
    assert "Data checks: not run" in text and "Guard: not run" in text
    assert text.rstrip().splitlines()[-1].startswith("- [[notes-updates]]")


def test_a_passed_run_files_its_checks_and_a_rerun_replaces_the_section(
        tmp_path, monkeypatch):
    def chain(steps, runner, sleep):
        (reports / "data_proof_2026-11-11.md").write_text(
            "| check | status | detail |\n|---|---|---|\n"
            "| vintage | ok | x |\n| held names | ATTENTION | DEAD 1: AAA |\n")
        (reports / "guard_2026-11-11.md").write_text(
            "New today: AAA (drawdown)\n\n## Needs attention\n- AAA [x]\n")
        return 0, None

    cadence, page, reports = _scratch_cadence(tmp_path, monkeypatch, chain)
    assert cadence.cmd_run() == 0
    text = page.read_text()
    assert "## 2026-11-11 - passed" in text
    assert "1 of 2 ok; attention: held names" in text
    assert "1 new alerts, 1 holdings flagged (dry run: no alert recorded)" in text
    assert "AAA" not in text

    # The session is covered, so a second firing does nothing to the page.
    assert cadence.cmd_run() == 0
    assert page.read_text() == text


def test_a_report_left_by_an_earlier_attempt_is_not_read_as_this_runs(
        tmp_path, monkeypatch):
    cadence, page, reports = _scratch_cadence(
        tmp_path, monkeypatch, lambda steps, runner, sleep: (1, "backfill"))
    stale = reports / "data_proof_2026-11-11.md"
    stale.write_text("| vintage | ok | x |\n")
    os.utime(stale, (0, 0))
    assert cadence.cmd_run() == 1
    assert "Data checks: not run" in page.read_text()


def test_no_vault_skips_the_page_and_keeps_the_runs_result(
        tmp_path, monkeypatch, capsys):
    cadence, page, _ = _scratch_cadence(
        tmp_path, monkeypatch, lambda steps, runner, sleep: (0, None),
        vault=False)
    assert cadence.cmd_run() == 0
    assert not page.exists()
    assert "daily page skipped" in capsys.readouterr().out
