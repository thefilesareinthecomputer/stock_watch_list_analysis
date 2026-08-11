"""Validate the call state machine on held-out history.

    uv run python scripts/validate_calls.py

Replays the hysteresis state machine (every symbol starting at `none`)
over the walk-forward monthly dates and measures success criteria 1 and 9
of SPEC-BUY-SELL-CALLS: the in-position set must beat the out set on
forward excess, and in-position turnover must sit below the ~50%
one-sided monthly bound. Aggregates only are recorded in
results/calls_validation.json - never symbols, which stay private.
"""
import argparse
import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

import duckdb  # noqa: E402
import pandas as pd  # noqa: E402

from backtest.call_backtest import call_set_excess, in_position_turnover  # noqa: E402
from backtest.harness import CAVEATS  # noqa: E402
from backtest.returns import BENCHMARK  # noqa: E402
from backtest.trials import log_trial  # noqa: E402
from scoring.calls import round_scores, simulate_calls  # noqa: E402
from scoring.tiers import load_registry  # noqa: E402

WAREHOUSE = os.path.join(ROOT, "warehouse", "market.duckdb")
RESULTS = os.path.join(ROOT, "results", "calls_validation.json")
TURNOVER_BOUND = 0.50


def monthly_dates(con):
    """Last session of each month with a settled 21d benchmark window -
    the shortest rung, so the replay covers as much history as possible."""
    return [str(r[0])[:10] for r in con.execute(
        "SELECT MAX(as_of_date) FROM backtest_forward_returns "
        "WHERE symbol = ? AND horizon = 21 "
        "GROUP BY DATE_TRUNC('month', as_of_date) ORDER BY 1",
        [BENCHMARK]).fetchall()]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--horizons", type=int, nargs="+",
                        default=[21, 63, 126])
    args = parser.parse_args()

    registry = load_registry()
    con = duckdb.connect(WAREHOUSE, read_only=True)
    dates = monthly_dates(con)
    log_trial("call_state_machine", "v2_composite_hysteresis", args.horizons,
              0.0, "Task 11 criteria 1+9: in-position vs out forward excess "
              "and turnover of the hysteresis state machine, all-none start")

    scores = round_scores(con, registry, dates)
    calls = simulate_calls(scores, registry["calls"])
    turnover = in_position_turnover(calls)

    per_horizon = {}
    for horizon in args.horizons:
        by_date = call_set_excess(con, calls, horizon)
        by_year = by_date.groupby(
            pd.DatetimeIndex(by_date.index).year)["spread"].mean()
        n = len(by_date)
        t = float(by_date["spread"].mean() / by_date["spread"].std()
                  * (n ** 0.5)) if n > 1 else float("nan")
        per_horizon[str(horizon)] = {
            "n_dates": n,
            "in_position_mean_excess": float(by_date["in_position"].mean()),
            "out_mean_excess": float(by_date["out"].mean()),
            "spread_mean": float(by_date["spread"].mean()),
            "spread_t_overlap_inflated": t,
            "spread_by_year": by_year.round(4).to_dict(),
            "mean_n_in": float(by_date["n_in"].mean()),
        }
    con.close()

    result = {
        "methodology_version": registry["methodology_version"],
        "calls_config": registry["calls"],
        "eval_dates": {"n": len(dates), "first": dates[0], "last": dates[-1]},
        "in_position_turnover_monthly": turnover,
        "turnover_bound": TURNOVER_BOUND,
        "turnover_ok": turnover < TURNOVER_BOUND,
        "horizons": per_horizon,
    }
    with open(RESULTS, "w") as f:
        json.dump(result, f, indent=2, sort_keys=True)
        f.write("\n")

    print(f"{len(dates)} monthly rounds, {dates[0]} to {dates[-1]}")
    print(f"in-position turnover {turnover:.3f} one-sided monthly "
          f"(bound {TURNOVER_BOUND}) -> {'OK' if turnover < TURNOVER_BOUND else 'FAIL'}")
    print(f"\n{'horizon':>8} {'dates':>6} {'in-pos':>9} {'out':>9} "
          f"{'spread':>9} {'t*':>7}")
    for horizon, m in per_horizon.items():
        print(f"{horizon:>8} {m['n_dates']:>6} "
              f"{m['in_position_mean_excess']:>9.4f} "
              f"{m['out_mean_excess']:>9.4f} {m['spread_mean']:>9.4f} "
              f"{m['spread_t_overlap_inflated']:>7.2f}")
    print("\n  t* is overlap-inflated at horizons > 21 (plan gotcha 0e); "
          "judge on spread_by_year folds in the results file.")
    print(f"\nrecorded in {RESULTS}")
    print(f"\n{CAVEATS}")


if __name__ == "__main__":
    main()
