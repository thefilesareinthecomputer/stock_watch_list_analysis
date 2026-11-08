"""Tests for deriving adjusted prices from traded prices plus corporate actions.

Two levels. Synthetic cases pin the arithmetic exactly. Then, if the local
warehouse exists and is in traded terms, the derived series is reconciled
against yfinance's own adj_close across real dividend payers and real splits -
which is the check that matters, because it proves we can stop depending on a
series that rewrites itself every time a dividend lands.
"""
import os
import sys
from datetime import date

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import pandas as pd
import pytest

from common.adjustments import (
    adjusted_prices, adjustment_factor, max_divergence,
)

WAREHOUSE = os.path.join(os.path.dirname(__file__), "..", "warehouse", "market.duckdb")


def _frame(rows):
    return pd.DataFrame(rows, columns=["date", "close", "dividend"])


def test_no_actions_means_no_adjustment():
    df = _frame([("2026-01-01", 100.0, 0.0),
                 ("2026-01-02", 101.0, 0.0)])
    assert list(adjustment_factor(df, [])) == [1.0, 1.0]


def test_latest_row_is_never_adjusted():
    """Only actions after a date affect it, so the most recent row is always 1."""
    df = _frame([("2026-01-01", 100.0, 0.0),
                 ("2026-01-02", 100.0, 5.0)])
    assert adjustment_factor(df, []).iloc[-1] == 1.0


def test_split_halves_prior_prices():
    """A 2-for-1 split means traded prices before it must halve to stay comparable."""
    df = _frame([("2026-01-01", 100.0, 0.0),
                 ("2026-01-02", 50.0, 0.0)])
    out = adjusted_prices(df, [(date(2026, 1, 2), 2.0)])
    assert out["adj_close"].iloc[0] == pytest.approx(50.0)
    assert out["adj_close"].iloc[1] == pytest.approx(50.0)


def test_reverse_split_multiplies_prior_prices():
    df = _frame([("2026-01-01", 10.0, 0.0),
                 ("2026-01-02", 100.0, 0.0)])
    assert adjustment_factor(df, [(date(2026, 1, 2), 0.1)]).iloc[0] == \
        pytest.approx(10.0)


def test_split_on_a_session_the_frame_lacks_still_applies_to_earlier_rows():
    df = _frame([("2026-01-01", 100.0, 0.0),
                 ("2026-01-05", 50.0, 0.0)])
    assert list(adjustment_factor(df, [(date(2026, 1, 3), 2.0)])) == [0.5, 1.0]


def test_split_after_every_row_affects_all_of_them():
    df = _frame([("2026-01-01", 100.0, 0.0),
                 ("2026-01-02", 100.0, 0.0)])
    # The log knows a split the frame has not reached yet: nothing in the
    # frame is after it, so nothing is adjusted - the frame is self-consistent.
    assert list(adjustment_factor(df, [(date(2026, 1, 9), 2.0)])) == [1.0, 1.0]


def test_dividend_reduces_prior_prices_proportionally():
    df = _frame([("2026-01-01", 100.0, 0.0),
                 ("2026-01-02", 100.0, 1.0)])
    # Prior close 100, dividend 1 -> factor 0.99 on everything before it.
    assert adjustment_factor(df, []).iloc[0] == pytest.approx(0.99)


def test_actions_compound_across_time():
    df = _frame([("2026-01-01", 100.0, 0.0),
                 ("2026-01-02", 100.0, 1.0),
                 ("2026-01-03", 50.0, 0.0)])
    # Earliest row sees both: 0.99 from the dividend, then 1/2 from the split.
    assert adjustment_factor(df, [(date(2026, 1, 3), 2.0)]).iloc[0] == \
        pytest.approx(0.99 / 2)


def test_dividend_paid_on_a_split_day_is_measured_in_that_days_terms():
    # A 1-for-2 reverse split and a 15.125 distribution on the same day,
    # prior close 28.55 in pre-split terms (57.10 in the new terms). The
    # dividend ratio is 1 - 15.125 / 57.10, not 1 - 15.125 / 28.55.
    df = _frame([("2011-12-20", 28.55, 0.0),
                 ("2011-12-21", 27.89, 15.125)])
    factor = adjustment_factor(df, [(date(2011, 12, 21), 0.5)])
    assert factor.iloc[0] == pytest.approx((1 - 15.125 / 57.10) / 0.5)


def test_dividend_on_first_row_cannot_be_applied():
    """No prior close to measure against - must not produce NaN."""
    df = _frame([("2026-01-01", 100.0, 2.0),
                 ("2026-01-02", 100.0, 0.0)])
    assert adjustment_factor(df, []).notna().all()


def test_factor_keeps_the_callers_index():
    df = _frame([("2026-01-01", 100.0, 0.0), ("2026-01-02", 50.0, 0.0)])
    df.index = [7, 9]
    assert list(adjustment_factor(df, [(date(2026, 1, 2), 2.0)]).index) == [7, 9]


def _with_adj(rows, run_id=None):
    out = pd.DataFrame(rows, columns=["date", "close", "adj_close", "dividend"])
    if run_id is not None:
        out["_run_id"] = run_id
    return out


def test_divergence_from_the_source_is_measured_not_assumed():
    # Our derived series vs Yahoo's own adjusted close: a double-applied
    # split shows up as a 50% divergence and must be reported, not hidden.
    good = _with_adj([("2026-01-01", 100.0, 100.0, 0.0),
                      ("2026-01-02", 101.0, 101.0, 0.0)])
    assert max_divergence(good, []) == pytest.approx(0.0)
    wrong = _with_adj([("2026-01-01", 100.0, 50.0, 0.0),
                       ("2026-01-02", 101.0, 101.0, 0.0)])
    assert max_divergence(wrong, []) == pytest.approx(1.0)  # ours 100 vs 50


def test_divergence_judges_each_row_at_its_capture_vintage():
    # Rows 1-2 were captured in run A, when no dividend was known; Yahoo's
    # adj_close equalled the close. Row 3 (run B) brought a dividend that
    # today's factor applies to rows 1-2. Against the vintage-A yardstick
    # that is not a divergence; against today's factor it would read 1%.
    df = _with_adj([("2026-01-01", 100.0, 100.0, 0.0),
                    ("2026-01-02", 100.0, 100.0, 0.0),
                    ("2026-01-03", 100.0, 100.0, 1.0)],
                   run_id=["A", "A", "B"])
    assert max_divergence(df, []) == pytest.approx(0.0)
    stale_as_if_current = df.drop(columns="_run_id")
    assert max_divergence(stale_as_if_current, []) == pytest.approx(0.01)


def test_divergence_at_vintage_still_catches_a_wrong_split():
    # Both rows captured together in traded terms; Yahoo's adj_close halves
    # the first. A log that lacks the split leaves ours unhalved: 100% off.
    df = _with_adj([("2026-01-01", 100.0, 50.0, 0.0),
                    ("2026-01-02", 50.0, 50.0, 0.0)], run_id="A")
    assert max_divergence(df, [(date(2026, 1, 2), 2.0)]) == pytest.approx(0.0)
    assert max_divergence(df, []) == pytest.approx(1.0)


def _traded_warehouse_available():
    if not os.path.exists(WAREHOUSE):
        return False
    import duckdb

    from common.warehouse_meta import bronze_is_traded

    con = duckdb.connect(WAREHOUSE, read_only=True)
    try:
        return bronze_is_traded(con)
    finally:
        con.close()


@pytest.mark.skipif(not _traded_warehouse_available(),
                    reason="local warehouse missing or not yet migrated to "
                           "traded closes (scripts/migrate_traded_prices.py)")
@pytest.mark.parametrize("symbol", ["AAPL", "MSFT", "JNJ", "KO", "PG",
                                    "XOM", "T", "VZ", "ABBV", "CVX",
                                    "APH", "CRWD", "NFLX"])
def test_derived_series_reconciles_with_yfinance(symbol):
    """The real check: our factor should reproduce yfinance's adj_close.

    Tolerance is 0.5%. Exact agreement is not expected - yfinance rounds and
    occasionally revises its own actions - but systematic divergence would mean
    the adjustment is wrong and every indicator built on it would be too.

    Yahoo's column is read straight from bronze and compared through
    max_divergence, which judges each row at its capture vintage: comparing
    ours with ours is how a double-applied split passed this test for months.
    """
    import duckdb

    from common.corporate_actions import splits_for

    con = duckdb.connect(WAREHOUSE, read_only=True)
    df = con.execute(
        "SELECT date, close, adj_close, dividend, _run_id FROM bronze_prices "
        "WHERE symbol = ? ORDER BY date", [symbol]
    ).df()
    splits = splits_for(con, symbol)
    con.close()
    if df.empty:
        pytest.skip(f"{symbol} not in the warehouse")

    worst = max_divergence(df, splits)
    assert worst < 0.005, f"{symbol}: worst divergence {worst:.4%}"
