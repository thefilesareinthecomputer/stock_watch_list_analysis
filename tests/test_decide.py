"""Decision report assembly: round, held overlay, harvest, advisory."""
import os
import sys
from datetime import date

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

import duckdb
import pytest

from decide import build_report
from common.trades import parse_trades

TODAY = date(2026, 8, 12)

ROUND = {
    "as_of_date": "2026-08-11", "methodology_version": "v2-local",
    "off_cycle": True, "run_id": "r", "created_ts": "t",
    "expectation": {
        "haircut": 0.5,
        "horizons": {"21": {"excess_net_haircut":
                            {"mean": 0.01, "p10": -0.02, "p90": 0.045}}},
    },
    "calls": [
        {"symbol": "AAA", "call": "buy", "prior_call": "none", "rank": 1,
         "score": 0.9, "score_percentile": 0.95,
         "component_percentiles": {"mom_12_1_pct": 0.99,
                                   "earnings_yield_pct": 0.80}},
        {"symbol": "BBB", "call": "none", "prior_call": "none", "rank": 5,
         "score": 0.2, "score_percentile": 0.40,
         "component_percentiles": {"mom_12_1_pct": 0.30,
                                   "earnings_yield_pct": 0.50}},
    ],
}


def _con():
    con = duckdb.connect()
    con.execute("CREATE TABLE gold_held_positions (symbol VARCHAR, "
                "accounts VARCHAR, total_quantity DOUBLE, tracked BOOLEAN, "
                "composite_rank INTEGER, call VARCHAR, "
                "deteriorating BOOLEAN)")
    con.execute("INSERT INTO gold_held_positions VALUES "
                "('AAA', 'brokerage', 10, TRUE, 1, 'buy', FALSE), "
                "('GDS', 'brokerage', 5, TRUE, 40, NULL, TRUE)")
    con.execute("CREATE TABLE gold_line_of_sight (symbol VARCHAR, "
                "close DOUBLE, deteriorating BOOLEAN, emerging BOOLEAN, "
                "mom_pct DOUBLE)")
    con.execute("INSERT INTO gold_line_of_sight VALUES "
                "('GDS', 12.0, TRUE, FALSE, 0.1), "
                "('NEWCO', 8.0, FALSE, TRUE, 0.97)")
    con.execute("CREATE TABLE bronze_prices (symbol VARCHAR, date DATE, "
                "close DOUBLE)")
    return con


def _trades():
    return parse_trades([
        '{"date": "2024-05-01", "symbol": "GDS", "account": "brokerage", '
        '"side": "buy", "qty": 5, "price": 30.0, "seed": true}'])


def test_report_refuses_without_a_recorded_round():
    with pytest.raises(ValueError, match="no recorded round"):
        build_report(_con(), [], [], TODAY)


def test_report_carries_round_calls_and_caveats():
    report = build_report(_con(), [ROUND], [], TODAY)
    assert "Round 2026-08-11 (OFF-CYCLE)" in report
    assert "1 buy" in report and "1 none" in report
    assert "- AAA: rank 1, percentile 0.950" in report
    assert "21 sessions: mean +1.00%" in report
    assert "survivor" in report.lower()  # caveats verbatim


def test_report_flags_deteriorating_held_position():
    report = build_report(_con(), [ROUND], [], TODAY)
    assert "GDS" in report and "DETERIORATING (down 3m, 6m, 12m)" in report


def test_report_harvest_section_with_wash_sale_reminder():
    report = build_report(_con(), [ROUND], _trades(), TODAY)
    assert "Loss harvest" in report
    assert "GDS (brokerage, 5 sh @ 30.00, now 12.00, -60.0%" in report
    assert "do not rebuy" in report


def test_report_labels_emerging_as_unvalidated():
    report = build_report(_con(), [ROUND], [], TODAY)
    assert "UNVALIDATED SCREEN" in report and "NEWCO" in report


def test_private_outputs_are_gitignored():
    root = os.path.join(os.path.dirname(__file__), "..")
    with open(os.path.join(root, ".gitignore")) as f:
        ignored = f.read()
    assert "reports/" in ignored and "trades.jsonl" in ignored
