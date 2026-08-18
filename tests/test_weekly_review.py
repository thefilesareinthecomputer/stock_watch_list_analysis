"""Weekly review verdicts: act only from sanctioned evidence.

Fixture pins per SPEC-EVENT-AWARENESS success criteria 1-2: every act
path has a fixture, a drawdown-only fixture yields no act, and the review
writes nothing outside reports/ plus its own state table.
"""
import os
import sys
from datetime import date

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

import duckdb
import pytest

from weekly_review import build_review, record_state, verdict
from common.trades import parse_trades
from scoring.tiers import load_registry

TODAY = date(2026, 8, 17)
LATEST = "2026-08-14"


def _con():
    """A held book of five names over a 13-symbol scoring universe.

    HELD0 scores at the bottom and is in position per the round (the one
    true exit breach); HELD2 scores low but was never bought (too-soon,
    never act); HELD1 scores near the top; DETER is deteriorating; FUNDX
    is held-untracked (screens only).
    """
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
    con.execute("CREATE TABLE bronze_prices (symbol VARCHAR, date DATE, "
                "close DOUBLE)")
    # Ascending 365d change by list position: HELD0 and HELD2 score at the
    # bottom, DETER and HELD1 near the top so their verdicts are driven by
    # their own evidence, never by the recompute.
    symbols = ["HELD0", "HELD2"] + [f"S{i:02d}" for i in range(9)] \
        + ["DETER", "HELD1"]
    for i, symbol in enumerate(symbols):
        con.execute("INSERT INTO silver_signals VALUES (?, ?, 0.0, ?)",
                    [symbol, LATEST, i * 10.0])
        con.execute(
            "INSERT INTO gold_candidate_signals VALUES (?, ?, ?, 0.1, 0.1)",
            [symbol, LATEST, i * 0.01])
    con.execute("CREATE TABLE gold_held_positions (symbol VARCHAR, "
                "accounts VARCHAR, total_quantity DOUBLE, tracked BOOLEAN, "
                "composite_rank INTEGER, call VARCHAR, "
                "deteriorating BOOLEAN, ret_3m DOUBLE, ret_12m DOUBLE)")
    con.execute("INSERT INTO gold_held_positions VALUES "
                "('HELD0', 'brokerage', 10, TRUE, 13, 'buy', FALSE, "
                " 0.01, 0.05), "
                "('HELD2', 'brokerage', 6, TRUE, 12, NULL, FALSE, "
                " 0.02, 0.04), "
                "('HELD1', 'roth ira', 4, TRUE, 2, 'hold', FALSE, "
                " 0.10, 0.30), "
                "('DETER', 'brokerage', 5, TRUE, 9, NULL, TRUE, "
                " -0.12, -0.30), "
                "('FUNDX', 'brokerage', 3, FALSE, NULL, NULL, NULL, "
                " NULL, NULL)")
    con.execute("INSERT INTO bronze_prices VALUES "
                "('HELD0', DATE '2026-08-14', 100.0), "
                "('HELD1', DATE '2026-08-14', 50.0), "
                "('DETER', DATE '2026-08-14', 12.0)")
    return con


# One recorded round: the system bought HELD0 and never called HELD2.
ROUNDS = [{"as_of_date": "2026-08-10", "methodology_version": "v2-local",
           "calls": [{"symbol": "HELD0", "call": "buy"},
                     {"symbol": "HELD1", "call": "hold"},
                     {"symbol": "HELD2", "call": "none"}]}]


def _build(con, trades=(), rounds=ROUNDS):
    return build_review(con, load_registry(), list(trades), TODAY, rounds)


def _section(report, title):
    return report.split(f"## {title}")[1].split("##")[0]


def test_exit_breach_acts_only_for_in_position_names():
    report, _ = _build(_con())
    act = _section(report, "Act")
    assert "HELD0" in act and "below exit percentile" in act
    assert "no call emitted" in act
    assert "HELD1" not in act  # top-ranked held name never breaches
    # HELD2 scores just as low but was never bought: the exit line is not
    # a stop rule for discretionary holdings (spec: no new sell criteria).
    assert "HELD2" not in act
    too_soon = _section(report, "Too soon")
    assert "HELD2" in too_soon and "never issued a buy" in too_soon


def test_deteriorating_newly_true_is_act_then_standing_is_too_soon():
    con = _con()
    report, state_rows = _build(con)
    assert "DETER: DETERIORATING newly true" in _section(report, "Act")

    record_state(con, TODAY, state_rows)
    later = date(2026, 8, 24)
    report2, _ = build_review(con, load_registry(), [], later, ROUNDS)
    assert "DETERIORATING newly true" not in _section(report2, "Act")
    assert "DETER" in _section(report2, "Too soon")
    assert "already surfaced" in _section(report2, "Too soon")


def test_harvest_flag_is_act_with_tax_label():
    trades = parse_trades([
        '{"date": "2024-05-01", "symbol": "DETER", "account": "brokerage", '
        '"side": "buy", "qty": 5, "price": 30.0, "seed": true}'])
    report, _ = _build(_con(), trades)
    act = _section(report, "Act")
    assert "loss harvest (tax, not alpha)" in act


def test_stale_held_name_is_data_integrity_act():
    con = _con()
    con.execute("DELETE FROM silver_signals "
                "WHERE symbol = 'HELD1' AND as_of_date = ?", [LATEST])
    con.execute("INSERT INTO silver_signals VALUES "
                "('HELD1', '2026-08-10', 0.0, 10.0)")
    report, _ = _build(con)
    assert "HELD1: data integrity: stale" in _section(report, "Act")


def test_drawdown_alone_never_justifies_act():
    """Success criterion 1: a name below its basis, in a tax-advantaged
    account (no harvest), not deteriorating, not breached, must punt -
    with the drawdown displayed in context only."""
    trades = parse_trades([
        '{"date": "2024-05-01", "symbol": "HELD1", "account": "roth ira", '
        '"side": "buy", "qty": 4, "price": 80.0, "seed": true}'])
    report, _ = _build(_con(), trades)
    assert "HELD1" not in _section(report, "Act")
    assert "HELD1" in _section(report, "Punt")
    assert "vs basis -37.5%" in _section(report, "Context per held name")


def test_untracked_held_name_punts_as_screens_only():
    report, _ = _build(_con())
    assert "FUNDX" in _section(report, "Punt")
    assert "FUNDX: brokerage, 3 sh, screens only" in report


def test_verdict_unit_partial_decline_is_too_soon():
    evidence = {"symbol": "X", "stale": False, "deteriorating": False,
                "exit_breach": False, "below_exit_line": False,
                "score_percentile": 0.7, "harvest": [],
                "ret_3m": 0.02, "ret_12m": -0.08, "drawdown": -0.5}
    state, reasons = verdict(evidence, {})
    assert state == "too-soon"
    assert "below the deteriorating bar" in reasons[0]


def test_review_writes_only_its_state_table():
    con = _con()
    before = {r[0] for r in con.execute(
        "SELECT table_name FROM information_schema.tables").fetchall()}
    _, state_rows = _build(con)
    record_state(con, TODAY, state_rows)
    after = {r[0] for r in con.execute(
        "SELECT table_name FROM information_schema.tables").fetchall()}
    assert after - before == {"weekly_review_state"}


def test_same_day_rerun_is_idempotent():
    con = _con()
    report1, state_rows = _build(con)
    record_state(con, TODAY, state_rows)
    report2, state_rows2 = _build(con)
    record_state(con, TODAY, state_rows2)
    assert report1 == report2
    assert con.execute(
        "SELECT COUNT(*) FROM weekly_review_state").fetchone()[0] \
        == len(state_rows)


def test_report_carries_caveats_and_absent_phases():
    report, _ = _build(_con())
    assert "survivor" in report.lower()  # CAVEATS verbatim
    assert "Monitor state: not built (Phase M)" in report


def test_empty_warehouse_degrades_to_a_report_not_a_crash():
    con = duckdb.connect(":memory:")
    report, state_rows = build_review(con, load_registry(), [], TODAY, [])
    assert "0 held names reviewed" in report
    assert state_rows == []
