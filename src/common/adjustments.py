"""Derive adjusted prices from traded closes plus corporate actions.

bronze_prices holds the OHLCV each symbol actually traded at, plus dividends
in the same share terms; splits live in the corporate_actions log. Neither
input is ever rewritten, so the adjusted series is a deterministic function
of two stable inputs, computed on demand. Yahoo's `auto_adjust=True` series
rescales the entire history every time a dividend is paid, which is why an
adjusted series can never be the stored evidence: a score computed last month
would silently change this month, and no past recommendation could be
reproduced.

METHOD (standard back-adjustment)

    adj_close[t] = close[t] * PROD over all i > t of  (1 - D_i / C_[i-1]) / S_i

where D_i is the dividend paid on day i, C_[i-1] the prior close, and S_i the
split ratio effective on day i (2.0 for a 2-for-1, taken from the log). Only
actions strictly AFTER t affect the price at t.

RECONCILIATION. Yahoo's own adj_close is stored with each row as captured and
is the independent yardstick. It knows only the actions up to its capture
vintage, so `max_divergence` compares it with our factor truncated at that
vintage (the newest session in the row's `_run_id`), not with today's factor:
a row captured a year ago is checked against what Yahoo said a year ago.
tests/test_adjustments.py runs this against the real warehouse.
"""
import numpy as np
import pandas as pd


def _split_series(dates, splits):
    """Per-row split ratio: the logged ratio on the first session on or after
    each split date, 1.0 everywhere else. A split effective on a session the
    frame lacks still divides every earlier row."""
    when = pd.to_datetime(pd.Series(dates)).reset_index(drop=True)
    ratio = pd.Series(1.0, index=when.index)
    for day, r in splits:
        hits = when.index[when >= pd.Timestamp(day)]
        if len(hits):
            ratio.iloc[hits[0]] *= float(r)
    return ratio


def adjustment_factor(df: pd.DataFrame, splits) -> pd.Series:
    """Cumulative back-adjustment factor per row, ascending by date.

    `df`: one symbol, date-ordered, columns date, close, dividend, in traded
    terms. `splits`: [(date, ratio)] from corporate_actions. The factor is
    1.0 on the most recent row and decreases going back.
    """
    close = df["close"].astype(float).reset_index(drop=True)
    dividend = df["dividend"].fillna(0.0).astype(float).reset_index(drop=True)
    split = _split_series(df["date"], splits)

    prior_close = close.shift(1)
    # A dividend on the first row has no prior close to measure against, so it
    # cannot be applied; treat it as no adjustment rather than dividing by NaN.
    div_ratio = (1.0 - dividend / prior_close).fillna(1.0)
    per_day = div_ratio / split

    # Only actions strictly after t adjust the price at t, so shift up before
    # accumulating, then accumulate from the end backwards.
    future = per_day.shift(-1).fillna(1.0)
    factor = future[::-1].cumprod()[::-1]
    factor.index = df.index
    return factor


def max_divergence(df: pd.DataFrame, splits) -> float:
    """Worst relative gap between our adjusted close and the source's own
    adj_close over rows where the source has one, each row judged against
    the actions known at its capture vintage (see module docstring). The
    build reports every symbol above a threshold, so a wrong adjustment is
    seen the day it appears instead of hiding inside every indicator."""
    ordered = df.sort_values("date").reset_index(drop=True)
    reference = ordered["adj_close"].astype(float).to_numpy()
    factor = adjustment_factor(ordered, splits)
    ours = ordered["close"].astype(float) * factor
    if "_run_id" in ordered:
        newest_in_run = ordered.groupby("_run_id")["date"].transform("idxmax")
        ours = ours / factor.iloc[newest_in_run.to_numpy()].to_numpy()
    ours = ours.to_numpy()
    mask = ~np.isnan(reference) & (reference > 0)
    if not mask.any():
        return 0.0
    return float(np.max(np.abs(ours[mask] - reference[mask]) / reference[mask]))


def adjusted_prices(df: pd.DataFrame, splits) -> pd.DataFrame:
    """Return `df` with open/high/low/close back-adjusted, plus the factor.

    Volume is scaled inversely by the split component so that price * volume
    stays comparable across a split.
    """
    out = df.sort_values("date").copy()
    factor = adjustment_factor(out, splits)
    out["adj_factor"] = factor
    for column in ("open", "high", "low", "close"):
        if column in out:
            out[f"adj_{column}"] = out[column].astype(float) * factor
    return out
