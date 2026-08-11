"""Measure the historical state machine: does holding what it says to hold
beat being out of it, and does it churn?

Success criteria 1 and 9 of SPEC-BUY-SELL-CALLS. The in-position set is
{buy, hold}, the out set {sell, none}; a `buy` means most likely to beat
the benchmark, so the two sets' forward EXCESS returns are the test.
Excess is gross here - turnover is reported separately against the ~50%
one-sided monthly bound (Novy-Marx & Velikov 2016), which is what decides
whether costs would eat the spread.
"""
import pandas as pd

IN_POSITION = ("buy", "hold")


def call_set_excess(con, calls, horizon):
    """Per-date mean forward excess of the in-position set vs the out set.

    Returns a frame indexed by as_of_date with columns in_position, out,
    spread, n_in, n_out. Dates whose forward window is open are absent.
    """
    realized = con.execute(
        "SELECT symbol, as_of_date, excess_return "
        "FROM backtest_forward_returns WHERE horizon = ?", [horizon]).df()
    frame = calls.merge(realized, on=["symbol", "as_of_date"], how="inner")
    if frame.empty:
        return pd.DataFrame(
            columns=["in_position", "out", "spread", "n_in", "n_out"])
    frame["in_pos"] = frame["call"].isin(IN_POSITION)

    def _per_date(g):
        inside = g.loc[g["in_pos"], "excess_return"]
        outside = g.loc[~g["in_pos"], "excess_return"]
        return pd.Series({"in_position": inside.mean(),
                          "out": outside.mean(),
                          "spread": inside.mean() - outside.mean(),
                          "n_in": len(inside), "n_out": len(outside)})
    return frame.groupby("as_of_date").apply(_per_date, include_groups=False)


def in_position_turnover(calls):
    """Mean one-sided turnover of the in-position set between consecutive
    dates: the share of the current set that was not held last time."""
    d = calls[calls["call"].isin(IN_POSITION)]
    sets = {date: set(g["symbol"]) for date, g in d.groupby("as_of_date")}
    dates = sorted(sets)
    rates = [1.0 - len(sets[a] & sets[b]) / len(sets[b])
             for a, b in zip(dates, dates[1:]) if sets[b]]
    return float(pd.Series(rates).mean()) if rates else float("nan")
