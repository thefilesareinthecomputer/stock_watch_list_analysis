"""Backfill symbol selection: multi-level columns even for one ticker."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

import pandas as pd
import pytest

from backfill import _symbol_frame

DATES = pd.bdate_range(end="2026-08-10", periods=3)


def _multi(symbols):
    return pd.concat(
        {s: pd.DataFrame({"Close": [1.0] * 3, "Volume": [10] * 3},
                         index=DATES) for s in symbols}, axis=1)


def test_multi_level_single_symbol_selects_by_ticker():
    # The 726 % 25 == 1 crash: a one-symbol batch still has ticker-level
    # columns and must be selected, not passed through.
    frame = _symbol_frame(_multi(["AAA"]), "AAA")
    assert list(frame.columns) == ["Close", "Volume"]


def test_multi_level_batch_selects_each_symbol():
    raw = _multi(["AAA", "BBB"])
    assert list(_symbol_frame(raw, "BBB").columns) == ["Close", "Volume"]
    with pytest.raises(KeyError):
        _symbol_frame(raw, "MISSING")


def test_single_level_frame_passes_through():
    flat = pd.DataFrame({"Close": [1.0] * 3, "Volume": [10] * 3}, index=DATES)
    assert _symbol_frame(flat, "AAA") is flat


def test_backfill_drops_partial_bar_for_unfinished_session(monkeypatch):
    # The 2026-08-12 incident: a pre-open run stored 313 partial same-day
    # bars. Rows dated past the completed-session cutoff never land.
    import datetime

    import duckdb

    import backfill

    def fake_fetch(batch, start):
        return pd.DataFrame({
            "Date": [pd.Timestamp("2026-08-10"), pd.Timestamp("2099-01-01")],
            "symbol": "AAA", "open": 1.0, "high": 1.0, "low": 1.0,
            "close": 1.0, "adj_close": 1.0, "volume": 10,
            "dividend": 0.0, "split_ratio": 0.0})

    monkeypatch.setattr(backfill, "_fetch", fake_fetch)
    con = duckdb.connect(":memory:")
    total, missing = backfill.backfill_prices(con, ["AAA"], "2026-08-01")
    dates = [r[0] for r in con.execute(
        "SELECT date FROM bronze_prices ORDER BY date").fetchall()]
    assert dates == [datetime.date(2026, 8, 10)]
    assert total == 1 and missing == []
