"""Convert Yahoo's restated rows back to the terms they traded in.

Yahoo serves history in today's share terms: every pre-split open, high,
low, close and dividend is divided by each later split and volume is
multiplied by it. Verified against the source on 2026-11-07 with AAPL
2020-08-28 (served 124.807503, traded 499.23), its 2020-08-07 dividend
(served 0.205, declared 0.82) and that day's volume (served 198.0M, traded
49.5M). Bronze stores what traded, so every served row is run through
`to_traded` with the splits from corporate_actions before it is stored.

Only splits dated strictly AFTER a row affect it: the split-date row is
already in post-split terms. A reverse split is a ratio below 1, so the
same multiplication divides. adj_close is Yahoo's own series and the
reconciliation yardstick; it is never converted.
"""
import pandas as pd

PRICE_COLUMNS = ("open", "high", "low", "close", "dividend")


def later_split_factor(dates, splits):
    """Series aligned to `dates`: the product of every split ratio dated
    strictly after each date, 1.0 where there is none."""
    when = pd.to_datetime(pd.Series(dates))
    factor = pd.Series(1.0, index=when.index)
    for day, ratio in splits:
        factor = factor.where(when >= pd.Timestamp(day), factor * float(ratio))
    return factor


def to_traded(df, splits):
    """Copy of `df` (one symbol) with prices and dividends in traded terms
    and volume in traded shares. `splits` is [(date, ratio)] as returned by
    corporate_actions.splits_for. Columns that are absent are skipped."""
    out = df.copy()
    if not splits or out.empty:
        return out
    factor = later_split_factor(out["date"], splits).to_numpy()
    for column in PRICE_COLUMNS:
        if column in out:
            out[column] = out[column].astype(float) * factor
    if "volume" in out:
        volume = out["volume"].astype("Float64") / factor
        out["volume"] = volume.round().astype("Int64")
    return out
