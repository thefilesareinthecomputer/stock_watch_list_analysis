"""Tests for deriving adjusted prices from raw prices plus corporate actions.

Two levels. Synthetic cases pin the arithmetic exactly. Then, if the local
warehouse exists, the derived series is reconciled against yfinance's own
adj_close across real dividend payers and real splits - which is the check that
matters, because it proves we can stop depending on a series that rewrites
itself every time a dividend lands.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import pandas as pd
import pytest

from common.adjustments import (
    adjusted_prices, adjustment_factor, max_divergence,
)

WAREHOUSE = os.path.join(os.path.dirname(__file__), "..", "warehouse", "market.duckdb")


def _frame(rows):
    return pd.DataFrame(rows, columns=["date", "close", "dividend", "split_ratio"])


def test_no_actions_means_no_adjustment():
    df = _frame([("2026-01-01", 100.0, 0.0, 0.0),
                 ("2026-01-02", 101.0, 0.0, 0.0)])
    assert list(adjustment_factor(df)) == [1.0, 1.0]


def test_latest_row_is_never_adjusted():
    """Only actions after a date affect it, so the most recent row is always 1."""
    df = _frame([("2026-01-01", 100.0, 0.0, 0.0),
                 ("2026-01-02", 100.0, 5.0, 0.0)])
    assert adjustment_factor(df).iloc[-1] == 1.0


def test_split_halves_prior_prices():
    """A 2-for-1 split means prices before it must halve to stay comparable."""
    df = _frame([("2026-01-01", 100.0, 0.0, 0.0),
                 ("2026-01-02", 50.0, 0.0, 2.0)])
    out = adjusted_prices(df)
    assert out["adj_close"].iloc[0] == pytest.approx(50.0)
    assert out["adj_close"].iloc[1] == pytest.approx(50.0)


def test_split_already_in_the_prices_is_not_applied_again():
    """Yahoo's close is usually split-adjusted already: no jump across the
    split date means the split is in the prices, and dividing again would
    fabricate a 2x jump."""
    df = _frame([("2026-01-01", 100.0, 0.0, 0.0),
                 ("2026-01-02", 101.0, 0.0, 2.0)])
    assert list(adjustment_factor(df)) == [1.0, 1.0]


def _with_adj(rows):
    return pd.DataFrame(rows, columns=["date", "close", "adj_close",
                                       "dividend", "split_ratio"])


def test_yahoo_adjusted_column_decides_a_small_spinoff_ratio():
    # A 1.066 "split" (a spin-off) on a day the stock also fell 5%: the
    # price jump alone cannot tell; Yahoo's adj/close ratio staying flat
    # proves the close already reflects it.
    df = _with_adj([("2026-01-01", 100.0, 99.0, 0.0, 0.0),
                    ("2026-01-02", 95.0, 94.05, 0.0, 1.066)])
    assert adjustment_factor(df).iloc[0] == pytest.approx(1.0)


def test_yahoo_adjusted_column_confirms_a_raw_split():
    # Raw close halves; Yahoo's adjusted close does not -> apply the split.
    df = _with_adj([("2026-01-01", 100.0, 50.0, 0.0, 0.0),
                    ("2026-01-02", 50.0, 50.0, 0.0, 2.0)])
    assert adjustment_factor(df).iloc[0] == pytest.approx(0.5)


def test_divergence_from_the_source_is_measured_not_assumed():
    # Our derived series vs Yahoo's own adjusted close: a double-applied
    # split shows up as a 50% divergence and must be reported, not hidden.
    good = _with_adj([("2026-01-01", 100.0, 100.0, 0.0, 0.0),
                      ("2026-01-02", 101.0, 101.0, 0.0, 0.0)])
    assert max_divergence(good) == pytest.approx(0.0)
    wrong = _with_adj([("2026-01-01", 100.0, 50.0, 0.0, 0.0),
                       ("2026-01-02", 101.0, 101.0, 0.0, 0.0)])
    assert max_divergence(wrong) == pytest.approx(1.0)  # ours 100 vs 50


def test_dividend_reduces_prior_prices_proportionally():
    df = _frame([("2026-01-01", 100.0, 0.0, 0.0),
                 ("2026-01-02", 100.0, 1.0, 0.0)])
    # Prior close 100, dividend 1 -> factor 0.99 on everything before it.
    assert adjustment_factor(df).iloc[0] == pytest.approx(0.99)


def test_actions_compound_across_time():
    df = _frame([("2026-01-01", 100.0, 0.0, 0.0),
                 ("2026-01-02", 100.0, 1.0, 0.0),
                 ("2026-01-03", 50.0, 0.0, 2.0)])
    # Earliest row sees both: 0.99 from the dividend, then 1/2 from the split.
    assert adjustment_factor(df).iloc[0] == pytest.approx(0.99 / 2)


def test_dividend_on_first_row_cannot_be_applied():
    """No prior close to measure against - must not produce NaN."""
    df = _frame([("2026-01-01", 100.0, 2.0, 0.0),
                 ("2026-01-02", 100.0, 0.0, 0.0)])
    assert adjustment_factor(df).notna().all()


@pytest.mark.skipif(not os.path.exists(WAREHOUSE),
                    reason="local warehouse not built; run scripts/backfill.py")
@pytest.mark.parametrize("symbol", ["AAPL", "MSFT", "JNJ", "KO", "PG",
                                    "XOM", "T", "VZ", "ABBV", "CVX",
                                    "APH", "CRWD", "NFLX"])
def test_derived_series_reconciles_with_yfinance(symbol):
    """The real check: our factor should reproduce yfinance's adj_close.

    Tolerance is 0.5%. Exact agreement is not expected - yfinance rounds and
    occasionally revises its own actions - but systematic divergence would mean
    the adjustment is wrong and every indicator built on it would be too.

    Yahoo's column is captured BEFORE adjusted_prices() runs: that function
    overwrites adj_close with ours, and comparing ours with ours is how a
    double-applied split passed this test for months.
    """
    import duckdb

    con = duckdb.connect(WAREHOUSE, read_only=True)
    df = con.execute(
        "SELECT date, close, adj_close, dividend, split_ratio FROM bronze_prices "
        "WHERE symbol = ? ORDER BY date", [symbol]
    ).df()
    con.close()
    if df.empty:
        pytest.skip(f"{symbol} not in the warehouse")

    reference = df.sort_values("date")["adj_close"].astype(float).to_numpy()
    out = adjusted_prices(df)
    derived = (out["close"].astype(float) * out["adj_factor"]).to_numpy()
    reference = pd.Series(reference)
    derived = pd.Series(derived)
    mask = reference.notna() & (reference > 0)
    relative_error = ((derived[mask] - reference[mask]).abs() / reference[mask])

    assert relative_error.max() < 0.005, (
        f"{symbol}: worst divergence {relative_error.max():.4%} on "
        f"{int(mask.sum())} rows"
    )
