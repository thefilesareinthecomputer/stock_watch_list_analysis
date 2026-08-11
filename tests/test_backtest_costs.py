"""Cost model: the zero-cost/costed gap is exactly the modelled points."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import pandas as pd
import pytest

from backtest.costs import net_return, round_trip_cost


def test_zero_cost_exceeds_costed_by_exactly_the_modelled_bps():
    gross = 0.0842
    assert net_return(gross, cost_bps=0.0) == gross
    gap = net_return(gross, cost_bps=0.0) - net_return(gross, cost_bps=10.0)
    assert gap == pytest.approx(2 * 10.0 / 10_000.0, abs=1e-15)


def test_costs_apply_elementwise_to_series():
    gross = pd.Series([0.10, -0.05, 0.0])
    net = net_return(gross, cost_bps=25.0)
    assert ((gross - net).round(12) == round(round_trip_cost(25.0), 12)).all()


def test_round_trip_is_two_sides():
    assert round_trip_cost(10.0) == pytest.approx(0.002)
