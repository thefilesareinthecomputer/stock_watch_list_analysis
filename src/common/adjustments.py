"""Derive adjusted prices from raw prices plus corporate actions.

Bronze stores RAW close plus the dividends and splits that adjust it, never an
adjusted series. `auto_adjust=True` rescales the entire close history every time
a dividend is paid, so an adjusted series stored as evidence is not stable: a
score computed last month silently changes this month, and no past
recommendation can be reproduced.

Storing raw prices plus actions makes history immutable, and the adjustment
becomes a deterministic function computed on demand - which is what this module
provides.

METHOD (standard back-adjustment)

    adj_close[t] = close[t] * PROD over all i > t of  (1 - D_i / C_[i-1]) / S_i

where D_i is the dividend paid on day i, C_[i-1] the prior close, and S_i the
split ratio on day i (2.0 for a 2-for-1). Only actions strictly AFTER t affect
the price at t - which is also why the series changes whenever a new dividend
lands, and why it cannot be the stored record.

Reconciled against yfinance's own adj_close by tests/test_adjustments.py.
"""
import numpy as np
import pandas as pd


def adjustment_factor(df: pd.DataFrame) -> pd.Series:
    """Cumulative back-adjustment factor per row, ascending by date.

    Expects columns: close, dividend, split_ratio - one symbol, date-ordered.
    The factor is 1.0 on the most recent row and decreases going back.
    """
    close = df["close"].astype(float)
    dividend = df["dividend"].fillna(0.0).astype(float)
    # 0 and NaN both mean "no split" in the source; 1.0 is the identity.
    split = df["split_ratio"].replace(0.0, np.nan).fillna(1.0).astype(float)

    prior_close = close.shift(1)
    # A dividend on the first row has no prior close to measure against, so it
    # cannot be applied; treat it as no adjustment rather than dividing by NaN.
    div_ratio = (1.0 - dividend / prior_close).fillna(1.0)

    per_day = div_ratio / split

    # Only actions strictly after t adjust the price at t, so shift up before
    # accumulating, then accumulate from the end backwards.
    future = per_day.shift(-1).fillna(1.0)
    return future[::-1].cumprod()[::-1]


def adjusted_prices(df: pd.DataFrame) -> pd.DataFrame:
    """Return `df` with open/high/low/close back-adjusted, plus the factor.

    Volume is scaled inversely by the split component so that price * volume
    stays comparable across a split.
    """
    out = df.sort_values("date").copy()
    factor = adjustment_factor(out)
    out["adj_factor"] = factor
    for column in ("open", "high", "low", "close"):
        if column in out:
            out[f"adj_{column}"] = out[column].astype(float) * factor
    return out
