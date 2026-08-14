"""Rebalance orchestration: settle -> report -> emit, and the refusals."""
import copy
import json
import os
import sys
from datetime import date

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

import duckdb
import pytest

from rebalance import due_round_date, off_cycle_round_date, run_rebalance
from scoring.calls import emit_round, read_rounds
from scoring.tiers import load_registry

JUNE, JULY = "2026-06-30", "2026-07-31"


def _registry(first_round_month="2026-06"):
    registry = copy.deepcopy(load_registry())
    registry["calls"]["first_round_month"] = first_round_month
    return registry


def _warehouse():
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE silver_signals (symbol VARCHAR, "
                "as_of_date TIMESTAMP, change_30d_pct DOUBLE, "
                "change_365d_pct DOUBLE)")
    con.execute("CREATE TABLE gold_candidate_signals (symbol VARCHAR, "
                "as_of_date TIMESTAMP, earnings_yield DOUBLE, "
                "gross_profitability DOUBLE, roe DOUBLE)")
    con.execute("CREATE TABLE bronze_entity (symbol VARCHAR, sic VARCHAR)")
    con.execute("CREATE TABLE bronze_security (symbol VARCHAR, "
                "quote_type VARCHAR)")
    con.execute("CREATE TABLE backtest_forward_returns (symbol VARCHAR, "
                "as_of_date TIMESTAMP, horizon INTEGER, fwd_return DOUBLE, "
                "excess_return DOUBLE)")
    for d in (JUNE, JULY):
        for i in range(12):
            con.execute("INSERT INTO silver_signals VALUES (?, ?, 0.0, ?)",
                        [f"S{i:02d}", d, i * 10.0])
            con.execute("INSERT INTO gold_candidate_signals VALUES "
                        "(?, ?, ?, 0.1, 0.1)", [f"S{i:02d}", d, i * 0.01])
    return con


def _decay_file(tmp_path):
    metrics = {str(h): {"ic": {"mean": 0.05, "t_stat": 4.0, "n_dates": 190,
                               "by_year": {"2025": 0.05}},
                        "excess_net_distribution":
                            {"mean": 0.02, "p10": -0.04, "p90": 0.09,
                             "n_dates": 190}}
               for h in (21, 63, 126)}
    path = tmp_path / "decay.json"
    path.write_text(json.dumps({"definition_hash": "abc",
                                "metrics": metrics}))
    return str(path)


def _run(con, tmp_path, today=date(2026, 8, 5), registry=None,
         off_cycle=False):
    return run_rebalance(
        con, registry or _registry(), today,
        calls_path=str(tmp_path / "calls_log.jsonl"),
        report_dir=str(tmp_path / "post_mortem"),
        decay_path=_decay_file(tmp_path),
        trial_log=str(tmp_path / "trial_log.jsonl"),
        off_cycle=off_cycle)


def _authorize(registry, vintage):
    registry["events"] = registry.get("events", []) + [
        {"date": "2026-08-12", "action": "authorize_off_cycle",
         "vintage": vintage, "reason": "test"}]
    return registry


def test_first_run_emits_and_reports_nothing_to_grade(tmp_path):
    status = _run(_warehouse(), tmp_path)
    assert status["emitted"] is True
    assert status["round_date"] == JULY  # latest completed month
    assert status["settled"] == 0 and status["report_written"] is True
    rounds = read_rounds(str(tmp_path / "calls_log.jsonl"))
    assert len(rounds) == 1
    assert rounds[0]["expectation"]["horizons"]["21"][
        "excess_net_haircut"]["mean"] == pytest.approx(0.01)
    with open(tmp_path / "trial_log.jsonl") as f:
        assert len(f.readlines()) == 1  # criterion 8: every round is a trial


def test_rerun_same_day_is_a_noop(tmp_path):
    con = _warehouse()
    _run(con, tmp_path)
    status = _run(con, tmp_path)
    assert status["emitted"] is False
    assert status["reason"] == "round already recorded"
    assert len(read_rounds(str(tmp_path / "calls_log.jsonl"))) == 1


def test_settlement_failure_refuses_the_emit(tmp_path):
    con = _warehouse()
    # A recorded round whose frozen expectation cannot grade a closed rung.
    emit_round({"as_of_date": JUNE, "methodology_version": "v2-local",
                "run_id": "r", "created_ts": "t",
                "expectation": {"horizons": {}},
                "calls": [{"symbol": "S11", "score": 1.0,
                           "score_percentile": 1.0,
                           "component_percentiles": {}, "prior_call": "none",
                           "call": "buy"}]},
               path=str(tmp_path / "calls_log.jsonl"))
    con.execute("INSERT INTO backtest_forward_returns VALUES "
                "('SPY', ?, 21, 0.01, 0.0)", [JUNE])
    with pytest.raises(ValueError, match="cannot grade"):
        _run(con, tmp_path)
    assert len(read_rounds(str(tmp_path / "calls_log.jsonl"))) == 1  # no emit
    assert not (tmp_path / "post_mortem").exists()  # aborted before report


def test_before_first_round_month_refuses_backdated_emit(tmp_path):
    status = _run(_warehouse(), tmp_path,
                  registry=_registry(first_round_month="2026-08"))
    assert status["emitted"] is False
    assert "prospectively" in status["reason"]


def test_mid_month_uses_completed_month_not_partial_current():
    con = _warehouse()
    con.execute("INSERT INTO silver_signals VALUES ('S00', '2026-08-04', "
                "0.0, 10.0)")  # partial August data
    round_date, reason = due_round_date(con, _registry()["calls"],
                                        date(2026, 8, 5))
    assert round_date == JULY and reason is None


def test_month_end_evening_emits_todays_round():
    con = _warehouse()
    con.execute("INSERT INTO silver_signals VALUES ('S00', '2026-08-31', "
                "0.0, 10.0)")
    round_date, _ = due_round_date(con, _registry()["calls"],
                                   date(2026, 8, 31))
    assert round_date == "2026-08-31"


def _mid_month_warehouse(vintage="2026-08-04"):
    con = _warehouse()
    for i in range(12):
        con.execute("INSERT INTO silver_signals VALUES (?, ?, 0.0, ?)",
                    [f"S{i:02d}", vintage, i * 10.0])
        con.execute("INSERT INTO gold_candidate_signals VALUES "
                    "(?, ?, ?, 0.1, 0.1)", [f"S{i:02d}", vintage, i * 0.01])
    return con


def test_off_cycle_refused_without_authorization_event(tmp_path):
    status = _run(_mid_month_warehouse(), tmp_path, off_cycle=True)
    assert status["emitted"] is False
    assert "authorize_off_cycle" in status["reason"]
    assert read_rounds(str(tmp_path / "calls_log.jsonl")) == []


def test_off_cycle_emits_latest_vintage_when_authorized(tmp_path):
    registry = _authorize(_registry(), "2026-08-04")
    status = _run(_mid_month_warehouse(), tmp_path, registry=registry,
                  off_cycle=True)
    assert status["emitted"] is True
    assert status["round_date"] == "2026-08-04"
    assert status["session_coverage"] == (12, 12)
    rounds = read_rounds(str(tmp_path / "calls_log.jsonl"))
    assert rounds[0]["off_cycle"] is True


def test_off_cycle_rerun_is_a_noop(tmp_path):
    con = _mid_month_warehouse()
    registry = _authorize(_registry(), "2026-08-04")
    _run(con, tmp_path, registry=registry, off_cycle=True)
    status = _run(con, tmp_path, registry=registry, off_cycle=True)
    assert status["emitted"] is False
    assert len(read_rounds(str(tmp_path / "calls_log.jsonl"))) == 1


def test_off_cycle_authorization_must_name_the_vintage():
    con = _mid_month_warehouse()
    registry = _authorize(_registry(), "2026-08-03")  # wrong date
    round_date, reason = off_cycle_round_date(con, registry)
    assert round_date is None and "authorize_off_cycle" in reason


def test_off_cycle_still_refuses_backdated_vintage():
    con = _mid_month_warehouse()
    registry = _authorize(_registry(first_round_month="2026-09"),
                          "2026-08-04")
    round_date, reason = off_cycle_round_date(con, registry)
    assert round_date is None and "prospectively" in reason


def test_off_cycle_state_carries_into_month_end_round(tmp_path):
    con = _mid_month_warehouse()
    registry = _authorize(_registry(), "2026-08-04")
    _run(con, tmp_path, registry=registry, off_cycle=True)
    con.execute("DELETE FROM backtest_forward_returns")  # nothing gradeable
    for i in range(12):
        con.execute("INSERT INTO silver_signals VALUES (?, '2026-08-31', "
                    "0.0, ?)", [f"S{i:02d}", i * 10.0])
        con.execute("INSERT INTO gold_candidate_signals VALUES "
                    "(?, '2026-08-31', ?, 0.1, 0.1)", [f"S{i:02d}", i * 0.01])
    status = _run(con, tmp_path, today=date(2026, 8, 31), registry=registry)
    assert status["emitted"] is True and status["round_date"] == "2026-08-31"
    rounds = read_rounds(str(tmp_path / "calls_log.jsonl"))
    top = [c for c in rounds[1]["calls"] if c["symbol"] == "S11"][0]
    assert top["prior_call"] == "buy"  # entered off-cycle, not reset
    assert top["call"] == "hold"
