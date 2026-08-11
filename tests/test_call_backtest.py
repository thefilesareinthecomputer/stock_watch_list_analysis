"""Call replay measurement: set spread and in-position turnover."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import duckdb
import pandas as pd
import pytest

from backtest.call_backtest import call_set_excess, in_position_turnover

D1, D2 = pd.Timestamp("2026-01-30"), pd.Timestamp("2026-02-27")


def _calls(rows):
    return pd.DataFrame(rows, columns=["symbol", "as_of_date", "call"])


def test_call_set_excess_splits_in_position_from_out():
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE backtest_forward_returns (symbol VARCHAR, "
                "as_of_date TIMESTAMP, horizon INTEGER, excess_return DOUBLE)")
    for symbol, excess in (("A", 0.05), ("B", 0.03), ("C", -0.02),
                           ("D", -0.04)):
        con.execute("INSERT INTO backtest_forward_returns VALUES "
                    "(?, ?, 126, ?)", [symbol, D1, excess])
    calls = _calls([("A", D1, "buy"), ("B", D1, "hold"),
                    ("C", D1, "sell"), ("D", D1, "none")])
    by_date = call_set_excess(con, calls, 126)
    row = by_date.iloc[0]
    assert row["in_position"] == pytest.approx(0.04)
    assert row["out"] == pytest.approx(-0.03)
    assert row["spread"] == pytest.approx(0.07)
    assert row["n_in"] == 2 and row["n_out"] == 2


def test_open_windows_are_absent_not_zero():
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE backtest_forward_returns (symbol VARCHAR, "
                "as_of_date TIMESTAMP, horizon INTEGER, excess_return DOUBLE)")
    con.execute("INSERT INTO backtest_forward_returns VALUES "
                "('A', ?, 126, 0.05)", [D1])
    calls = _calls([("A", D1, "buy"), ("A", D2, "hold")])  # D2 window open
    assert list(call_set_excess(con, calls, 126).index) == [D1]


def test_turnover_zero_when_held_set_stable_one_when_rotated():
    stable = _calls([("A", D1, "buy"), ("B", D1, "buy"),
                     ("A", D2, "hold"), ("B", D2, "hold")])
    assert in_position_turnover(stable) == 0.0
    rotated = _calls([("A", D1, "buy"), ("B", D1, "buy"),
                      ("C", D2, "buy"), ("D", D2, "buy")])
    assert in_position_turnover(rotated) == 1.0
    # Sells drop out of the set; half the D2 set is new.
    half = _calls([("A", D1, "buy"), ("B", D1, "buy"),
                   ("A", D2, "hold"), ("B", D2, "sell"), ("C", D2, "buy")])
    assert in_position_turnover(half) == 0.5
