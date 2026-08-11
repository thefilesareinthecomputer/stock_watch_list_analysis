"""Evaluation metrics: does a signal rank the cross-section correctly?

Everything is computed per as_of_date across the universe, then aggregated
over dates - the walk-forward direction (SPEC-SIGNAL-TIERS §2). Per-date
results are what make fold-level (per-year) reporting possible; a pooled
full-sample number is exactly the alphalens error the spec forbids.

Input frame contract, one horizon at a time:
    symbol, as_of_date, signal, fwd_return, excess_return

The information coefficient is the Spearman rank correlation between the
signal and the realized forward EXCESS return: rank correlation because a
signal's job here is ordering, not magnitude.
"""
import numpy as np
import pandas as pd

from backtest.costs import net_return

TOP_DECILE = 9
DECILES = 10


def _spearman(a, b):
    # Pearson on average ranks == Spearman, without the scipy dependency
    # pandas' method="spearman" would pull in.
    return a.rank().corr(b.rank())


def ic_by_date(df):
    """Per-date Spearman IC of signal vs excess return. NaN dates dropped."""
    def _ic(group):
        if group["signal"].nunique() < 2 or group["excess_return"].nunique() < 2:
            return np.nan
        return _spearman(group["signal"], group["excess_return"])
    series = df.groupby("as_of_date").apply(_ic, include_groups=False)
    return series.dropna()


def ic_summary(ics):
    """Mean, t-stat and per-year means for a per-date IC series.

    The t-stat assumes independent dates. Monthly evaluation dates with a
    multi-month forecast window OVERLAP, inflating t by roughly the square
    root of the windows-per-horizon; judge long horizons on fold-level t
    across the yearly means, not on this number.
    """
    n = len(ics)
    if n == 0:
        return {"mean": np.nan, "t_stat": np.nan, "n_dates": 0, "by_year": {}}
    mean, std = ics.mean(), ics.std(ddof=1)
    t = np.inf * np.sign(mean) if (std == 0 or n < 2) else mean / std * np.sqrt(n)
    by_year = ics.groupby(pd.DatetimeIndex(ics.index).year).mean()
    return {"mean": mean, "t_stat": t, "n_dates": n,
            "by_year": by_year.round(4).to_dict()}


def _with_deciles(df):
    """Decile per date by signal rank: 0 worst signal, 9 best."""
    out = df.copy()
    ranks = out.groupby("as_of_date")["signal"].rank(method="first")
    counts = out.groupby("as_of_date")["signal"].transform("size")
    # Ceiling form: the best rank lands in the top decile at ANY universe
    # size (a floor form leaves decile 9 empty below ten symbols).
    out["decile"] = np.ceil(ranks * DECILES / counts).astype(int) - 1
    return out


def decile_means(df):
    """Mean forward excess return per decile: mean of per-date decile means."""
    d = _with_deciles(df)
    per_date = d.groupby(["as_of_date", "decile"])["excess_return"].mean()
    return per_date.groupby("decile").mean()


def monotonicity(deciles):
    """Spearman rho of decile index vs mean excess: 1.0 is perfectly ordered."""
    if len(deciles) < 2:
        return np.nan
    return _spearman(deciles.reset_index(drop=True),
                     pd.Series(range(len(deciles)), dtype=float))


def hit_rate(df):
    """Share of top-decile picks that beat the benchmark."""
    top = _with_deciles(df).query("decile == @TOP_DECILE")
    return np.nan if top.empty else (top["excess_return"] > 0).mean()


def turnover(df):
    """Mean one-sided top-decile turnover between consecutive dates."""
    d = _with_deciles(df)
    tops = {date: set(g.loc[g["decile"] == TOP_DECILE, "symbol"])
            for date, g in d.groupby("as_of_date")}
    dates = sorted(tops)
    rates = [1.0 - len(tops[a] & tops[b]) / len(tops[b])
             for a, b in zip(dates, dates[1:]) if tops[b]]
    return np.nan if not rates else float(np.mean(rates))


def top_decile_excess_by_date(df, cost_bps=0.0):
    """Excess of the top decile vs the equal-weight universe, per date.

    Costs charge the top-decile leg a full round trip per window; the
    equal-weight comparison stays gross, which is conservative against us.
    """
    d = _with_deciles(df)
    return d.groupby("as_of_date").apply(
        lambda g: net_return(
            g.loc[g["decile"] == TOP_DECILE, "fwd_return"].mean(), cost_bps)
        - g["fwd_return"].mean(),
        include_groups=False)


def top_decile_excess(df, cost_bps=0.0):
    """Mean of the per-date top-decile excess series."""
    return float(top_decile_excess_by_date(df, cost_bps).mean())


def excess_distribution(df, cost_bps=0.0):
    """The walk-forward distribution of top-decile net excess.

    This is what a frozen expectation quotes (SPEC-BUY-SELL-CALLS): a call
    round is graded against the mean/p10/p90 of this distribution, not
    against a point estimate, so a single bad window can be told apart from
    drift.
    """
    per_date = top_decile_excess_by_date(df, cost_bps)
    if per_date.empty:
        return {"mean": np.nan, "p10": np.nan, "p90": np.nan, "n_dates": 0}
    return {"mean": float(per_date.mean()),
            "p10": float(per_date.quantile(0.10)),
            "p90": float(per_date.quantile(0.90)),
            "n_dates": int(len(per_date))}


def evaluate(df, cost_bps=0.0):
    """Assemble the full metric set for one signal at one horizon."""
    ics = ic_by_date(df)
    deciles = decile_means(df)
    return {
        "ic": ic_summary(ics),
        "decile_means": deciles.round(5).to_dict(),
        "monotonicity": monotonicity(deciles),
        "hit_rate": hit_rate(df),
        "turnover": turnover(df),
        "excess_vs_equal_weight_gross": top_decile_excess(df, 0.0),
        "excess_vs_equal_weight_net": top_decile_excess(df, cost_bps),
        "excess_net_distribution": excess_distribution(df, cost_bps),
    }
