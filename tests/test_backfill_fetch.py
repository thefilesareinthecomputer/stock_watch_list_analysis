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
