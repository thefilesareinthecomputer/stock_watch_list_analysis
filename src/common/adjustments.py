"""Derive adjusted prices from stored closes plus corporate actions.

Bronze stores Yahoo's close (already split-adjusted, verified 2026-11-07) plus
the dividends and splits, never the dividend-adjusted series. Splits are applied
only where the data shows the close is raw across them (`_split_is_raw`). `auto_adjust=True` rescales the entire close history every time
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


def _split_is_raw(df, close, split):
    """Per row: True where the stored close is RAW across that row's split.

    Evidence, strongest first:
    1. Yahoo's adj_close, when stored: adj/close shifts by the ratio across
       a split only if close is raw; flat means close already absorbed it.
       This stays decisive for small spin-off ratios (1.07) on volatile
       days, where the price jump alone cannot tell.
    2. The price jump: close[t-1] / close[t] near the ratio means raw; near
       1 means adjusted.
    Each test picks whichever hypothesis is closer in log space.
    """
    log_r = np.log(split)
    jump = np.log(close.shift(1) / close)
    if "adj_close" in df:
        k = (df["adj_close"].astype(float) / close)
        k_shift = np.log(k / k.shift(1))      # ~log r if raw, ~0 if adjusted
        evidence = k_shift.where(k_shift.notna(), jump)
    else:
        evidence = jump
    raw = (evidence - log_r).abs() < evidence.abs()
    return raw.fillna(False) | (split == 1.0)


def adjustment_factor(df: pd.DataFrame) -> pd.Series:
    """Cumulative back-adjustment factor per row, ascending by date.

    Expects columns: close, dividend, split_ratio - one symbol, date-ordered.
    The factor is 1.0 on the most recent row and decreases going back.
    """
    close = df["close"].astype(float)
    dividend = df["dividend"].fillna(0.0).astype(float)
    # 0 and NaN both mean "no split" in the source; 1.0 is the identity.
    split = df["split_ratio"].replace(0.0, np.nan).fillna(1.0).astype(float)
    # Apply a split only where the stored close is shown to be raw across
    # it. Yahoo normally serves split-ADJUSTED closes, and dividing those
    # again fabricated 2x-4x jumps on every split (found 2026-11-07).
    split = split.where(_split_is_raw(df, close, split), 1.0)

    prior_close = close.shift(1)
    # A dividend on the first row has no prior close to measure against, so it
    # cannot be applied; treat it as no adjustment rather than dividing by NaN.
    div_ratio = (1.0 - dividend / prior_close).fillna(1.0)

    per_day = div_ratio / split

    # Only actions strictly after t adjust the price at t, so shift up before
    # accumulating, then accumulate from the end backwards.
    future = per_day.shift(-1).fillna(1.0)
    return future[::-1].cumprod()[::-1]


def max_divergence(df: pd.DataFrame) -> float:
    """Worst relative gap between our adjusted close and the source's own
    adj_close (Yahoo), over rows where the source has one. The build
    reports every symbol above a threshold, so a wrong adjustment is seen
    the day it appears instead of hiding inside every indicator."""
    ordered = df.sort_values("date")
    reference = ordered["adj_close"].astype(float).to_numpy()
    ours = (ordered["close"].astype(float)
            * adjustment_factor(ordered)).to_numpy()
    mask = ~np.isnan(reference) & (reference > 0)
    if not mask.any():
        return 0.0
    return float(np.max(np.abs(ours[mask] - reference[mask]) / reference[mask]))


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
