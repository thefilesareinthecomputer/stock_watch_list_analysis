"""Metrics: a synthetic perfect predictor must score exactly right."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import numpy as np
import pandas as pd
import pytest

from backtest.metrics import (
    decile_means, evaluate, hit_rate, ic_by_date, ic_summary, monotonicity,
    turnover,
)

DATES = ["2024-01-31", "2024-02-29", "2024-03-28"]


def _frame(signal_fn, n_symbols=20):
    """excess_return varies by symbol index; signal set by signal_fn(i)."""
    rows = []
    for date in DATES:
        for i in range(n_symbols):
            excess = (i - n_symbols / 2) / 100.0
            rows.append({"symbol": f"S{i:02d}", "as_of_date": pd.Timestamp(date),
                         "signal": signal_fn(i), "fwd_return": excess + 0.01,
                         "excess_return": excess})
    return pd.DataFrame(rows)


def test_perfect_predictor_scores_ic_exactly_one():
    df = _frame(lambda i: float(i))
    ics = ic_by_date(df)
    assert len(ics) == len(DATES)
    assert (ics == 1.0).all()
    summary = ic_summary(ics)
    assert summary["mean"] == 1.0
    assert summary["t_stat"] == np.inf


def test_anti_predictor_scores_ic_minus_one():
    ics = ic_by_date(_frame(lambda i: -float(i)))
    assert (ics == -1.0).all()


def test_perfect_predictor_deciles_are_monotonic():
    deciles = decile_means(_frame(lambda i: float(i)))
    assert len(deciles) == 10
    assert monotonicity(deciles) == pytest.approx(1.0)
    assert deciles.iloc[-1] > deciles.iloc[0]


def test_constant_signal_yields_no_ic_dates():
    assert ic_by_date(_frame(lambda i: 1.0)).empty
    assert np.isnan(ic_summary(ic_by_date(_frame(lambda i: 1.0)))["mean"])


def test_hit_rate_is_one_when_top_decile_all_beat_benchmark():
    assert hit_rate(_frame(lambda i: float(i))) == 1.0
    assert hit_rate(_frame(lambda i: -float(i))) == 0.0


def test_turnover_zero_when_top_set_is_stable():
    assert turnover(_frame(lambda i: float(i))) == 0.0


def test_turnover_one_when_top_set_fully_rotates():
    rows = []
    for d, date in enumerate(DATES):
        for i in range(10):
            # A different pair tops the ranking every date.
            signal = 100.0 + i if (i // 2) == d else float(i)
            rows.append({"symbol": f"S{i}", "as_of_date": pd.Timestamp(date),
                         "signal": signal, "fwd_return": 0.0,
                         "excess_return": i / 100.0})
    assert turnover(pd.DataFrame(rows)) == 1.0


def test_costs_reduce_net_excess_by_exactly_the_round_trip():
    df = _frame(lambda i: float(i))
    result = evaluate(df, cost_bps=10.0)
    gap = (result["excess_vs_equal_weight_gross"]
           - result["excess_vs_equal_weight_net"])
    assert gap == pytest.approx(2 * 10.0 / 10_000.0, abs=1e-12)


def test_ic_summary_reports_per_year_folds():
    df = _frame(lambda i: float(i))
    df.loc[df["as_of_date"] == pd.Timestamp("2024-03-28"), "as_of_date"] = \
        pd.Timestamp("2025-03-28")
    summary = ic_summary(ic_by_date(df))
    assert set(summary["by_year"]) == {2024, 2025}
