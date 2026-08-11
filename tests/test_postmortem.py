"""Post-mortem: drift fixtures in, suggestions (or silence) out."""
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from backtest.postmortem import drift_state, write_report

DRIFT = {"below_mean_rounds": 5, "below_p10_rounds": 3, "fold_t_bar": 2.0}


def _settlement(date, below_mean=False, below_p10=False, mom_ic=0.20):
    return {"as_of_date": date, "methodology_version": "v2-local",
            "horizon": 21, "n_symbols": 300, "n_missing": 0,
            "realized_excess_net": -0.01 if below_mean else 0.02,
            "realized_ic": 0.05,
            "expected_mean_haircut": 0.015, "expected_p10_haircut": -0.013,
            "below_haircut_mean": below_mean, "below_haircut_p10": below_p10,
            "attribution": {"mom_12_1_pct": mom_ic,
                            "earnings_yield_pct": 0.18},
            "expectation_source": "cafe01"}


def _dates(n):
    return [f"2027-{m:02d}-28" for m in range(1, n + 1)]


def test_healthy_run_suggests_nothing():
    settlements = [_settlement(d) for d in _dates(6)]
    drift = drift_state(settlements, DRIFT)
    assert drift["suggestions"] == []
    assert drift["trailing_below_mean"] == 0


def test_single_miss_is_noise_not_a_suggestion():
    settlements = [_settlement(d) for d in _dates(5)] \
        + [_settlement("2027-06-28", below_mean=True, below_p10=True)]
    assert drift_state(settlements, DRIFT)["suggestions"] == []


def test_five_consecutive_below_mean_suggests_review():
    settlements = [_settlement(_dates(7)[0])] \
        + [_settlement(d, below_mean=True) for d in _dates(7)[1:6]]
    drift = drift_state(settlements, DRIFT)
    assert drift["trailing_below_mean"] == 5
    assert any(s["action"] == "review" for s in drift["suggestions"])


def test_recovery_resets_the_consecutive_count():
    settlements = [_settlement(d, below_mean=True) for d in _dates(4)] \
        + [_settlement("2027-05-28")]  # one good settlement breaks the run
    drift = drift_state(settlements, DRIFT)
    assert drift["trailing_below_mean"] == 0
    assert drift["suggestions"] == []


def test_three_consecutive_below_p10_suggests_review():
    settlements = [_settlement(d) for d in _dates(3)] \
        + [_settlement(d, below_mean=True, below_p10=True)
           for d in _dates(6)[3:]]
    drift = drift_state(settlements, DRIFT)
    assert any("p10" in s["reason"] for s in drift["suggestions"])


def test_insignificant_signal_ic_suggests_demotion_review():
    # Momentum attribution oscillates around zero across 6 settlements;
    # earnings yield stays strong. Only momentum is flagged.
    ics = [0.02, -0.03, 0.01, -0.02, 0.03, -0.01]
    settlements = [_settlement(d, mom_ic=ic)
                   for d, ic in zip(_dates(6), ics)]
    drift = drift_state(settlements, DRIFT)
    demotions = [s for s in drift["suggestions"]
                 if s["action"] == "demotion_review"]
    assert [s["signal"] for s in demotions] == ["mom_12_1_pct"]


def test_report_is_immutable_and_symbol_free(tmp_path):
    settlements = [_settlement(d) for d in _dates(2)]
    drift = drift_state(settlements, DRIFT)
    assert write_report("2027-03-31", settlements, drift,
                        directory=str(tmp_path)) is True
    # Second write for the same date: no-op, first report stands.
    assert write_report("2027-03-31", [], {"trailing_below_mean": 9,
                                           "trailing_below_p10": 9,
                                           "signal_t": {},
                                           "suggestions": []},
                        directory=str(tmp_path)) is False
    with open(tmp_path / "2027-03-31.json") as f:
        payload = json.load(f)
    assert payload["n_vintages_settled"] == 2
    md = (tmp_path / "2027-03-31.md").read_text()
    assert "nothing to learn yet (n=2)" in md
    assert "noise" in md and "Survivorship" in md
