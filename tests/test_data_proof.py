"""The daily data-proof report: each check named, counted and dated."""
import os
import sys
from datetime import date, datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

import duckdb

from common.corporate_actions import record_actions
from common.source_revisions import record_revisions
from common.warehouse_meta import BRONZE_TERMS, TRADED, set_meta
import backfill
import data_proof

# 2026-11-06 (Friday) was the last completed session for a Saturday run.
NOW = datetime(2026, 11, 7, 20, 0, tzinfo=timezone.utc)


def _warehouse(stale_symbol=None, traded=True):
    con = duckdb.connect(":memory:")
    con.execute(backfill.SCHEMA)
    for symbol, last in [("AAA", "2026-11-06"), ("BBB", "2026-11-06"),
                         (stale_symbol, "2026-10-01")]:
        if symbol:
            con.execute("INSERT INTO bronze_prices (symbol, date, close, adj_close, "
                        "dividend, _run_id) VALUES (?, ?, 10.0, 10.0, 0.0, 'r')",
                        [symbol, last])
    if traded:
        set_meta(con, BRONZE_TERMS, TRADED)
    record_actions(con, [{"symbol": "AAA", "date": "2020-01-02", "ratio": 2.0}],
                   source="test")
    return con


def test_clean_day_states_all_checks_passed_with_the_vintage():
    con = _warehouse()
    checks, latest = data_proof.run_checks(con, ["AAA", "BBB"], [], now=NOW)
    assert latest == date(2026, 11, 6)
    assert all(ok for _, ok, _ in checks), checks
    text = data_proof.render(checks, latest, date(2026, 11, 7))
    assert "# Data proof - 2026-11-07" in text
    assert "All checks passed for the 2026-11-06 vintage." in text
    assert "| vintage | ok |" in text and "terms: traded" in text


def test_report_names_and_counts_every_problem():
    con = _warehouse(stale_symbol="OLD")
    record_revisions(con, [("BBB", date(2026, 11, 5), 10.0, 9.5)])
    con.execute("INSERT INTO bronze_prices (symbol, date, close) "
                "VALUES ('HELDX', '2026-10-20', 1.0)")
    checks, latest = data_proof.run_checks(
        con, ["AAA", "BBB", "OLD"], ["AAA", "HELDX", "GONE"], now=NOW)
    by_name = {name: (ok, detail) for name, ok, detail in checks}

    ok, detail = by_name["held names"]
    assert not ok and "DEAD 2: GONE, HELDX" in detail
    ok, detail = by_name["stale from source"]
    assert not ok and detail.startswith("1: OLD 2026-10-01")
    ok, detail = by_name["source revisions (24h)"]
    assert not ok and "1: BBB 2026-11-05 10.00->9.50" in detail

    text = data_proof.render(checks, latest, date(2026, 11, 7))
    assert "3 check(s) need attention: held names, stale from source, " \
           "source revisions (24h)." in text
    assert "All checks passed" not in text


def test_unmigrated_warehouse_fails_the_vintage_check():
    con = _warehouse(traded=False)
    checks, _ = data_proof.run_checks(con, ["AAA"], [], now=NOW)
    name, ok, detail = checks[0]
    assert name == "vintage" and not ok and "SERVED (not migrated)" in detail


def test_old_revisions_are_not_todays_news():
    con = _warehouse()
    con.execute("CREATE TABLE source_revisions (symbol VARCHAR, date DATE, "
                "stored_close DOUBLE, source_close DOUBLE, first_seen VARCHAR)")
    con.execute("INSERT INTO source_revisions VALUES "
                "('AAA', '2026-10-01', 10.0, 9.0, '2026-10-02T00:00:00Z')")
    checks, _ = data_proof.run_checks(con, ["AAA"], [], now=NOW)
    assert dict((n, ok) for n, ok, _ in checks)["source revisions (24h)"]


def test_cadence_runs_the_report_after_the_build():
    import cadence

    names = [s["name"] for s in cadence.DAILY_STEPS]
    assert names.index("proof") == names.index("build") + 1
    assert names.index("proof") < names.index("guard")
