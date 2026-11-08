"""Convert served rows back to the terms they traded in.

Yahoo serves history restated into the share terms current AT THE TIME OF
THE FETCH: every pre-split open, high, low, close and dividend is divided
by each later split the source knows, and volume is multiplied by it.
Verified against the source on 2026-11-07 with AAPL 2020-08-28 (served
124.807503, traded 499.23), its 2020-08-07 dividend (served 0.205,
declared 0.82) and that day's volume (served 198.0M, traded 49.5M). Bronze
stores what traded, so every served row is run through `to_traded` with
the splits from corporate_actions before it is stored.

Two facts bound which splits a served row has absorbed:

- Its capture vintage. A row captured in run R is restated only for the
  splits dated on or before R's newest session (`vintage_dates`); a split
  after that capture was not known to it. Rows banked once and never
  refetched sit at an old vintage, which is how three symbols' pre-split
  history was found in traded terms already (2026-11-08).
- The source's own behavior. Across a decisive split (ratio at least 15%
  away from 1) a restated series shows no jump; a series the source did
  not restate jumps by about the ratio. `unrestated_events` names those,
  and they are left out of the factor.

Only splits dated strictly AFTER a row affect it: the split-date row is
already in post-split terms. A reverse split is a ratio below 1, so the
same multiplication divides. adj_close is Yahoo's own series and the
reconciliation yardstick; it is never converted.
"""
import math

import numpy as np
import pandas as pd

PRICE_COLUMNS = ("open", "high", "low", "close", "dividend")
DECISIVE_LOG_RATIO = math.log(1.15)


def vintage_dates(df):
    """Per row, the newest session in its capture run (`_run_id`); None
    when the frame carries no runs, meaning it was fetched just now and is
    restated for every logged split."""
    if "_run_id" not in df:
        return None
    dates = pd.to_datetime(df["date"])
    return dates.groupby(df["_run_id"]).transform("max")


def later_split_factor(dates, splits, vintage=None):
    """Series aligned to `dates`: the product of every split ratio dated
    strictly after each date and, when `vintage` is given, on or before
    that row's vintage; 1.0 where there is none."""
    when = pd.to_datetime(pd.Series(dates))
    known = None if vintage is None else pd.to_datetime(
        pd.Series(vintage)).set_axis(when.index)
    factor = pd.Series(1.0, index=when.index)
    for day, ratio in splits:
        applies = when < pd.Timestamp(day)
        if known is not None:
            applies &= known >= pd.Timestamp(day)
        factor = factor.where(~applies, factor * float(ratio))
    return factor


def unrestated_events(df, splits):
    """Decisive splits the source did not restate: the served close jumps
    by about the ratio across the split date instead of staying put.
    Returns [(date, ratio, jump)] with the log jump, oldest first. A split
    the before-row's capture never knew is not judged here; the vintage
    rule covers it."""
    if df.empty:
        return []
    ordered = df.sort_values("date")
    dates = pd.to_datetime(ordered["date"]).to_numpy()
    closes = ordered["close"].astype(float).to_numpy()
    vintage = vintage_dates(ordered)
    vintage = None if vintage is None else vintage.to_numpy()
    out = []
    for day, ratio in splits:
        log_ratio = math.log(float(ratio))
        if abs(log_ratio) < DECISIVE_LOG_RATIO:
            continue
        split_day = np.datetime64(pd.Timestamp(day))
        before = np.flatnonzero(dates < split_day)
        after = np.flatnonzero(dates >= split_day)
        if not len(before) or not len(after):
            continue
        i, j = before[-1], after[0]
        if vintage is not None and vintage[i] < split_day:
            continue
        if not (closes[i] > 0 and closes[j] > 0):
            continue
        jump = math.log(closes[i] / closes[j])
        if abs(jump - log_ratio) < abs(jump):
            out.append((day, float(ratio), jump))
    return out


def to_traded(df, splits):
    """Copy of `df` (one symbol) with prices and dividends in traded terms
    and volume in traded shares. `splits` is [(date, ratio)] as returned by
    corporate_actions.splits_for; pass it through `unrestated_events` first
    and drop what it names. Columns that are absent are skipped."""
    out = df.copy()
    if not splits or out.empty:
        return out
    factor = later_split_factor(out["date"], splits, vintage_dates(out)).to_numpy()
    for column in PRICE_COLUMNS:
        if column in out:
            out[column] = out[column].astype(float) * factor
    if "volume" in out:
        volume = out["volume"].astype("Float64") / factor
        out["volume"] = volume.round().astype("Int64")
    return out


def without(splits, events):
    """`splits` minus the events `unrestated_events` named."""
    skip = {(pd.Timestamp(d), float(r)) for d, r, _ in events}
    return [(d, r) for d, r in splits if (pd.Timestamp(d), float(r)) not in skip]
