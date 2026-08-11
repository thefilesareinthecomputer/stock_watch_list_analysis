"""Forward returns: next-open fills, hand-checked arithmetic, honest gaps."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import duckdb
import pytest

from backtest.returns import build_forward_returns

DATES = ["2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05",
         "2024-01-08", "2024-01-09"]


def _con(series):
    """series: {symbol: [adj_open per date in DATES, may be shorter]}"""
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE silver_adjusted_prices "
                "(symbol VARCHAR, date DATE, adj_open DOUBLE, adj_close DOUBLE)")
    for symbol, opens in series.items():
        for date, opn in zip(DATES, opens):
            con.execute("INSERT INTO silver_adjusted_prices VALUES (?, ?, ?, ?)",
                        [symbol, date, opn, opn])
    return con


def _rows(con, symbol, horizon):
    return con.execute(
        "SELECT * FROM backtest_forward_returns "
        "WHERE symbol = ? AND horizon = ? ORDER BY as_of_date",
        [symbol, horizon]).df()


def test_hand_computed_return_matches_to_6dp():
    con = _con({"X": [100.0, 102.0, 99.0, 107.5, 103.25, 111.0]})
    build_forward_returns(con, horizons=(2,), benchmark="X")

    rows = _rows(con, "X", 2)
    first = rows.iloc[0]
    # Signal dated 2024-01-02: enter at the 01-03 open (102.0), exit two
    # sessions later at the 01-05 open (107.5).
    assert str(first["as_of_date"])[:10] == "2024-01-02"
    assert str(first["entry_date"])[:10] == "2024-01-03"
    assert str(first["exit_date"])[:10] == "2024-01-05"
    assert first["fwd_return"] == pytest.approx(107.5 / 102.0 - 1, abs=1e-6)


def test_entry_is_strictly_after_the_signal_date():
    con = _con({"X": [100.0, 102.0, 99.0, 107.5, 103.25, 111.0]})
    build_forward_returns(con, horizons=(1, 2), benchmark="X")

    rows = con.execute("SELECT * FROM backtest_forward_returns "
                       "WHERE entry_date <= as_of_date").df()
    assert rows.empty


def test_truncated_horizons_are_absent_not_fabricated():
    con = _con({"X": [100.0, 102.0, 99.0, 107.5, 103.25, 111.0]})
    build_forward_returns(con, horizons=(2,), benchmark="X")

    # Six sessions: entry needs t+1, exit t+3. The last as_of that can
    # settle is index 2 (2024-01-04). Nothing after it may appear.
    rows = _rows(con, "X", 2)
    assert str(rows["as_of_date"].max())[:10] == "2024-01-04"
    assert len(rows) == 3


def test_excess_is_against_the_same_window():
    con = _con({
        "X":   [100.0, 100.0, 110.0, 121.0, 133.1, 146.41],
        "SPY": [100.0, 100.0, 105.0, 110.25, 115.7625, 121.550625],
    })
    build_forward_returns(con, horizons=(2,), benchmark="SPY")

    row = _rows(con, "X", 2).iloc[0]
    assert row["fwd_return"] == pytest.approx(0.21, abs=1e-9)
    assert row["benchmark_return"] == pytest.approx(0.1025, abs=1e-9)
    assert row["excess_return"] == pytest.approx(0.1075, abs=1e-9)


def test_benchmark_excess_against_itself_is_zero():
    con = _con({"SPY": [100.0, 101.0, 103.0, 99.0, 104.0, 108.0]})
    build_forward_returns(con, horizons=(1, 2), benchmark="SPY")

    rows = con.execute("SELECT excess_return FROM backtest_forward_returns").df()
    assert not rows.empty
    assert (rows["excess_return"].abs() < 1e-12).all()
