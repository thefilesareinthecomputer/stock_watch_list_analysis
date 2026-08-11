"""Settlement: closed rungs grade against the vintage's own expectation."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import duckdb
import pytest

from backtest.settlement import settle

DATE = "2026-01-30"


def _expectation(mean, p10):
    return {"source_sha256": "cafe01",
            "horizons": {str(h): {"excess_net_haircut":
                                  {"mean": mean, "p10": p10, "p90": 0.10}}
                         for h in (21, 63, 126)}}


def _round(expectation, date=DATE):
    calls = []
    for i in range(10):
        calls.append({"symbol": f"S{i}", "score": float(i),
                      "score_percentile": i / 9.0,
                      "component_percentiles": {"mom_12_1_pct": i / 9.0,
                                                "earnings_yield_pct": 0.5},
                      "prior_call": "none",
                      "call": "buy" if i == 9 else "none"})
    return {"as_of_date": date, "methodology_version": "v2-local",
            "expectation": expectation, "calls": calls}


def _warehouse(horizons=(21, 63), date=DATE):
    """Forward returns for S0..S9 plus the benchmark, only at the given
    horizons - a rung without a benchmark row is an open window."""
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE backtest_forward_returns (symbol VARCHAR, "
                "as_of_date TIMESTAMP, horizon INTEGER, fwd_return DOUBLE, "
                "excess_return DOUBLE)")
    for h in horizons:
        for i in range(10):
            con.execute("INSERT INTO backtest_forward_returns VALUES "
                        "(?, ?, ?, ?, ?)",
                        [f"S{i}", date, h, i / 100.0, i / 100.0 - 0.045])
        con.execute("INSERT INTO backtest_forward_returns VALUES "
                    "('SPY', ?, ?, 0.045, 0.0)", [date, h])
    return con


def test_only_closed_rungs_settle():
    results = settle(_warehouse(), [_round(_expectation(0.05, -0.01))],
                     cost_bps=0.0)
    assert [s["horizon"] for s in results] == [21, 63]  # 126 still open


def test_realized_excess_ic_and_drift_flags_hand_checked():
    s = settle(_warehouse(), [_round(_expectation(0.05, -0.01))],
               cost_bps=0.0)[0]
    # Top decile is S9 alone (percentile 1.0): 0.09 - mean(0.00..0.09).
    assert s["realized_excess_net"] == pytest.approx(0.045)
    assert s["realized_ic"] == pytest.approx(1.0)
    assert s["attribution"]["mom_12_1_pct"] == pytest.approx(1.0)
    assert s["n_symbols"] == 10 and s["n_missing"] == 0
    assert s["below_haircut_mean"] is True    # 0.045 < 0.05
    assert s["below_haircut_p10"] is False    # 0.045 >= -0.01
    assert s["expectation_source"] == "cafe01"


def test_each_vintage_graded_against_its_own_frozen_expectation():
    con = _warehouse()
    modest = _round(_expectation(0.01, -0.02))
    ambitious = _round(_expectation(0.08, 0.05))
    results = settle(con, [modest, ambitious], cost_bps=0.0)
    at_21 = [s for s in results if s["horizon"] == 21]
    assert at_21[0]["below_haircut_mean"] is False  # 0.045 >= 0.01
    assert at_21[1]["below_haircut_mean"] is True   # 0.045 <  0.08
    assert at_21[1]["below_haircut_p10"] is True    # 0.045 <  0.05


def test_costs_charge_the_top_decile_leg():
    free = settle(_warehouse(), [_round(_expectation(0.0, -0.1))],
                  cost_bps=0.0)[0]
    paid = settle(_warehouse(), [_round(_expectation(0.0, -0.1))],
                  cost_bps=10.0)[0]
    gap = free["realized_excess_net"] - paid["realized_excess_net"]
    assert gap == pytest.approx(2 * 10.0 / 10_000.0, abs=1e-9)


def test_delisted_symbol_counts_as_missing_not_fabricated():
    con = _warehouse()
    con.execute("DELETE FROM backtest_forward_returns WHERE symbol = 'S3'")
    s = settle(con, [_round(_expectation(0.0, -0.1))], cost_bps=0.0)[0]
    assert s["n_symbols"] == 9 and s["n_missing"] == 1


def test_expectation_missing_a_closed_rung_raises():
    broken = _expectation(0.05, -0.01)
    del broken["horizons"]["63"]
    with pytest.raises(ValueError, match="horizon 63"):
        settle(_warehouse(), [_round(broken)], cost_bps=0.0)
