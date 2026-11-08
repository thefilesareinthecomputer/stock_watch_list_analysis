"""Converting Yahoo's restated rows back to the terms they traded in.

Yahoo divides every pre-split open/high/low/close and dividend by each later
split and multiplies volume by it (verified 2026-11-07 against the source:
AAPL 2020-08-28 served 124.807503, traded 499.23; the 2020-08-07 dividend
served 0.205, declared 0.82; volume served 198.0M, traded 49.5M). The
conversion undoes exactly that, using the corporate_actions log as the
list of splits.
"""
import os
import sys
from datetime import date

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import pandas as pd
import pytest

from common.traded_prices import later_split_factor, to_traded


def _frame(dates, close=100.0, volume=1000, dividend=0.0):
    n = len(dates)
    return pd.DataFrame({
        "date": [date.fromisoformat(d) for d in dates],
        "open": [close] * n, "high": [close + 1] * n, "low": [close - 1] * n,
        "close": [close] * n, "adj_close": [close * 0.9] * n,
        "volume": [volume] * n, "dividend": [dividend] * n,
    })


def test_forward_split_after_a_date_scales_prices_up_and_volume_down():
    df = _frame(["2020-08-28"], close=100.0, volume=400, dividend=0.25)
    out = to_traded(df, [(date(2020, 8, 31), 4.0)])
    assert out.loc[0, ["open", "high", "low", "close"]].tolist() == [
        400.0, 404.0, 396.0, 400.0]
    assert out.loc[0, "volume"] == 100
    assert out.loc[0, "dividend"] == 1.0
    assert out.loc[0, "adj_close"] == 90.0        # Yahoo's series is left as served


def test_reverse_split_after_a_date_divides_prices():
    df = _frame(["2024-06-10"], close=100.0, volume=100)
    out = to_traded(df, [(date(2024, 6, 11), 0.1)])    # 1-for-10
    assert out.loc[0, "close"] == pytest.approx(10.0)
    assert out.loc[0, "volume"] == 1000


def test_only_splits_strictly_after_the_row_apply():
    df = _frame(["2020-08-28", "2020-08-31", "2020-09-01"], close=100.0)
    out = to_traded(df, [(date(2020, 8, 31), 4.0)])
    assert out["close"].tolist() == [400.0, 100.0, 100.0]


def test_multiple_later_splits_compound():
    df = _frame(["2014-01-02", "2014-06-09", "2020-08-31"], close=10.0)
    splits = [(date(2014, 6, 9), 7.0), (date(2020, 8, 31), 4.0)]
    assert later_split_factor(df["date"], splits).tolist() == [28.0, 4.0, 1.0]
    assert to_traded(df, splits)["close"].tolist() == [280.0, 40.0, 10.0]


def test_no_splits_is_the_identity():
    df = _frame(["2026-01-02", "2026-01-05"])
    out = to_traded(df, [])
    pd.testing.assert_frame_equal(out, df)


def test_aapl_2020_08_28_converts_to_the_traded_close():
    # Served value captured from yfinance on 2026-11-07; 499.23 is the
    # close Apple traded at that day, as published outside Yahoo.
    df = _frame(["2020-08-28"], close=124.807503, volume=187630000)
    out = to_traded(df, [(date(2020, 8, 31), 4.0)])
    assert abs(out.loc[0, "close"] - 499.23) < 0.01
    assert out.loc[0, "volume"] == 46907500


def test_aph_before_its_2026_split_doubles():
    # Served 2026-09-02 close with the 2026-09-03 2-for-1 logged. The
    # external proof of this close is STORY-000010's fixture.
    df = _frame(["2026-09-02"], close=80.040001)
    out = to_traded(df, [(date(2026, 9, 3), 2.0)])
    assert abs(out.loc[0, "close"] - 160.08) < 0.01


def test_missing_volume_stays_missing():
    df = _frame(["2020-08-28"], close=100.0)
    df["volume"] = pd.array([None], dtype="Int64")
    out = to_traded(df, [(date(2020, 8, 31), 4.0)])
    assert pd.isna(out.loc[0, "volume"]) and out.loc[0, "close"] == 400.0


def test_timestamp_and_date_inputs_agree():
    df = _frame(["2020-08-28"])
    as_ts = df.assign(date=pd.to_datetime(df["date"]))
    splits = [(pd.Timestamp("2020-08-31"), 4.0)]
    assert to_traded(df, splits)["close"].tolist() == \
        to_traded(as_ts, splits)["close"].tolist() == [400.0]
