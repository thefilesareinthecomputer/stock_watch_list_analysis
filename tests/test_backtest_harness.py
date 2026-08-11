"""The trust checks: a harness is only usable once it fails to find fake edge.

Two families. Synthetic tests pin the frame assembly. Warehouse tests run
the known-answer and look-ahead checks against the real 16-year history and
skip when the local warehouse is absent (it does not travel between
machines).
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import duckdb
import numpy as np
import pandas as pd
import pytest

from backtest.harness import evaluation_frame
from backtest.metrics import evaluate, ic_by_date, ic_summary
from backtest.returns import build_forward_returns

WAREHOUSE = os.path.join(os.path.dirname(__file__), "..", "warehouse",
                         "market.duckdb")
needs_warehouse = pytest.mark.skipif(
    not os.path.exists(WAREHOUSE), reason="local warehouse not built")


# --- synthetic: frame assembly ---------------------------------------------

def _tiny_warehouse():
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE silver_adjusted_prices "
                "(symbol VARCHAR, date DATE, adj_open DOUBLE, adj_close DOUBLE)")
    dates = pd.bdate_range("2024-01-01", "2024-03-29")
    rng = np.random.default_rng(7)
    for symbol, drift in [("AAA", 1.001), ("BBB", 0.999), ("SPY", 1.0)]:
        price = 100.0
        for date in dates:
            price *= drift * (1 + rng.normal(0, 0.005))
            con.execute("INSERT INTO silver_adjusted_prices VALUES (?, ?, ?, ?)",
                        [symbol, str(date.date()), price, price])
    con.execute("CREATE TABLE sigs AS SELECT symbol, date AS as_of_date, "
                "adj_close AS my_signal FROM silver_adjusted_prices")
    build_forward_returns(con, horizons=(5,), benchmark="SPY")
    return con


def test_frame_uses_month_end_dates_and_excludes_benchmarks():
    con = _tiny_warehouse()
    df = evaluation_frame(con, "sigs", "my_signal", 5)

    # One eval date per month with a settled window, none of them SPY.
    assert not df.empty
    per_month = df.groupby(df["as_of_date"].dt.to_period("M"))["as_of_date"]
    assert (per_month.nunique() == 1).all()
    assert "SPY" not in set(df["symbol"])
    # Each eval date is that month's LAST settled session for the benchmark.
    settled = con.execute(
        "SELECT MAX(as_of_date) FROM backtest_forward_returns "
        "WHERE symbol = 'SPY' AND horizon = 5").fetchone()[0]
    assert df["as_of_date"].max() == pd.Timestamp(settled)


def test_frame_rejects_malformed_identifiers():
    con = _tiny_warehouse()
    with pytest.raises(ValueError):
        evaluation_frame(con, "sigs; DROP TABLE sigs", "my_signal", 5)
    with pytest.raises(ValueError):
        evaluation_frame(con, "sigs", "my_signal, 1 AS x", 5)


# --- warehouse: known-answer and look-ahead --------------------------------

@pytest.fixture(scope="module")
def wcon():
    con = duckdb.connect(WAREHOUSE, read_only=True)
    yield con
    con.close()


@needs_warehouse
def test_known_answer_benchmark_excess_is_exactly_zero(wcon):
    # The benchmark evaluated as its own universe: no signal, however good,
    # could manufacture excess where there is none.
    rows = wcon.execute(
        "SELECT excess_return FROM backtest_forward_returns "
        "WHERE symbol = 'SPY'").df()
    assert not rows.empty
    assert (rows["excess_return"].abs() < 1e-12).all()


@needs_warehouse
def test_known_answer_random_signal_finds_no_edge(wcon):
    # A seeded random signal across 16 years and ~300 names must score a
    # mean IC near zero. A harness that finds edge in noise is broken.
    df = evaluation_frame(wcon, "silver_signals", "rsi", 126)
    rng = np.random.default_rng(0)
    df["signal"] = rng.random(len(df))

    summary = ic_summary(ic_by_date(df))
    assert summary["n_dates"] > 100
    assert abs(summary["mean"]) < 0.02
    assert abs(summary["t_stat"]) < 4.0


@needs_warehouse
def test_look_ahead_leak_is_detected_and_shift_degrades_it(wcon):
    # A signal that IS the future excess return scores IC == 1 by
    # construction. Misaligning it by one evaluation period must collapse
    # every metric; if it did not, information would be leaking backwards
    # through the join. Horizon 21 on monthly dates: consecutive windows
    # barely overlap, so the shifted leak has almost nothing left. At 126
    # the same shift only dents the IC (~0.8), because consecutive windows
    # share ~85% of their sessions - overlap, not leakage.
    df = evaluation_frame(wcon, "silver_signals", "rsi", 21)
    df["signal"] = df["excess_return"]
    leaked = evaluate(df)
    assert leaked["ic"]["mean"] == pytest.approx(1.0)
    assert leaked["monotonicity"] == pytest.approx(1.0)
    assert leaked["hit_rate"] > 0.99

    shifted = df.sort_values(["symbol", "as_of_date"]).copy()
    shifted["signal"] = shifted.groupby("symbol")["signal"].shift(1)
    shifted = shifted.dropna(subset=["signal"])
    degraded = evaluate(shifted)

    assert degraded["ic"]["mean"] < 0.5
    assert degraded["monotonicity"] < leaked["monotonicity"]
    assert degraded["hit_rate"] < leaked["hit_rate"]
    assert (degraded["excess_vs_equal_weight_gross"]
            < leaked["excess_vs_equal_weight_gross"])
