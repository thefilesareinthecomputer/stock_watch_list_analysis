"""Settlement: what actually happened to every gradeable call vintage.

Each recorded round is graded at every horizon rung whose forward window
has closed, against ITS OWN frozen expectation - never against the current
registry or the latest decay results (SPEC-BUY-SELL-CALLS "Never").
Settlement is a pure recomputation from immutable inputs (the call log and
forward returns derived from raw prices), so re-settling a vintage always
reproduces the same verdict.

The graded quantity matches the frozen one exactly: top-decile
(score_percentile >= 0.90) forward return net of a round-trip cost, minus
the equal-weight mean of the round's eligible universe - the same
definition backtest.metrics.excess_distribution records. Realized IC is
the Spearman of score vs realized excess; per-signal attribution is the
same Spearman per component percentile, which is what lets a post-mortem
say WHICH leg of the composite drifted.
"""
import json

import pandas as pd

from backtest.costs import DEFAULT_COST_BPS, net_return
from backtest.metrics import _spearman
from backtest.returns import BENCHMARK
from scoring.expectations import EXPECTATION_HORIZONS

TOP_PERCENTILE = 0.90


def settle(con, rounds, cost_bps=DEFAULT_COST_BPS):
    """Grade every (round, rung) pair with a closed window.

    Returns settlements ordered by (as_of_date, horizon). Skipping an
    unclosed rung is not failure; a closed rung that cannot be graded
    (expectation missing the rung) raises, because emitting past it would
    silently break the settle-before-emit guarantee.
    """
    settlements = []
    for entry in rounds:
        for horizon in EXPECTATION_HORIZONS:
            closed = con.execute(
                "SELECT 1 FROM backtest_forward_returns "
                "WHERE symbol = ? AND as_of_date = ? AND horizon = ?",
                [BENCHMARK, entry["as_of_date"], horizon]).fetchone()
            if not closed:
                continue
            settlements.append(
                _settle_rung(con, entry, horizon, cost_bps))
    return settlements


def _settle_rung(con, entry, horizon, cost_bps):
    expectation = entry["expectation"]
    rung = expectation.get("horizons", {}).get(str(horizon))
    if rung is None:
        raise ValueError(
            f"round {entry['as_of_date']}: frozen expectation has no "
            f"horizon {horizon} - cannot grade this vintage")

    calls = pd.DataFrame(entry["calls"])
    realized = con.execute(
        "SELECT symbol, fwd_return, excess_return "
        "FROM backtest_forward_returns "
        "WHERE as_of_date = ? AND horizon = ? AND symbol IN "
        f"({', '.join('?' for _ in calls['symbol'])})",
        [entry["as_of_date"], horizon, *calls["symbol"]]).df()
    frame = calls.merge(realized, on="symbol", how="inner")

    top = frame[frame["score_percentile"] >= TOP_PERCENTILE]
    realized_excess = (net_return(top["fwd_return"].mean(), cost_bps)
                       - frame["fwd_return"].mean())
    attribution = {
        component: float(_spearman(
            frame["component_percentiles"].map(lambda p: p[component]),
            frame["excess_return"]))
        for component in frame["component_percentiles"].iloc[0]
    } if len(frame) else {}

    expected = rung["excess_net_haircut"]
    return {
        "as_of_date": entry["as_of_date"],
        "methodology_version": entry["methodology_version"],
        "horizon": horizon,
        "n_symbols": int(len(frame)),
        "n_missing": int(len(calls) - len(frame)),
        "realized_excess_net": float(realized_excess),
        "realized_ic": float(_spearman(frame["score"],
                                       frame["excess_return"])),
        "expected_mean_haircut": expected["mean"],
        "expected_p10_haircut": expected["p10"],
        "below_haircut_mean": bool(realized_excess < expected["mean"]),
        "below_haircut_p10": bool(realized_excess < expected["p10"]),
        "attribution": attribution,
        "expectation_source": expectation.get("source_sha256", ""),
    }
