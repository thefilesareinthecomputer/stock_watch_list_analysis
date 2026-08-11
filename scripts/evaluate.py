"""Evaluate a signal against forward returns - the one-screen verdict.

    uv run python scripts/evaluate.py silver_signals change_30d_pct
    uv run python scripts/evaluate.py gold_candidate_signals earnings_yield_pct
    uv run python scripts/evaluate.py --candidates    # all three candidates

Monthly evaluation dates, benchmark-relative, costed at a flat round trip.
Per-year IC folds are printed for the provisional 126-session forecast
window so a variant's stability is visible, never just its pooled mean.
"""
import argparse
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

import duckdb  # noqa: E402

from backtest.costs import DEFAULT_COST_BPS  # noqa: E402
from backtest.harness import CAVEATS, evaluation_frame  # noqa: E402
from backtest.metrics import evaluate  # noqa: E402
from backtest.returns import HORIZONS  # noqa: E402
from backtest.trials import log_trial, trial_count  # noqa: E402

WAREHOUSE = os.path.join(ROOT, "warehouse", "market.duckdb")

CANDIDATES = [("gold_candidate_signals", c) for c in
              ("earnings_yield_pct", "gross_profitability_pct", "roe_pct")]

FOLD_HORIZON = 126  # the provisional forecast window


def report(con, table, signal_col, cost_bps, intent):
    # The trial is on the books BEFORE any result exists; a crash below
    # this line still counts the attempt (plan.md task 9b).
    number = log_trial(table, signal_col, HORIZONS, cost_bps, intent)
    print(f"\n=== {table}.{signal_col} | costs {cost_bps:g} bps/side "
          f"| trial #{number} ===")
    header = (f"{'horizon':>8} {'dates':>6} {'IC':>8} {'t':>7} "
              f"{'monot':>6} {'hit':>6} {'turn':>6} {'gross':>8} {'net':>8}")
    print(header)
    folds = None
    for horizon in HORIZONS:
        frame = evaluation_frame(con, table, signal_col, horizon)
        if frame.empty:
            print(f"{horizon:>8}  no evaluable rows")
            continue
        m = evaluate(frame, cost_bps)
        print(f"{horizon:>8} {m['ic']['n_dates']:>6} {m['ic']['mean']:>8.4f} "
              f"{m['ic']['t_stat']:>7.2f} {m['monotonicity']:>6.2f} "
              f"{m['hit_rate']:>6.2f} {m['turnover']:>6.2f} "
              f"{m['excess_vs_equal_weight_gross']:>8.4f} "
              f"{m['excess_vs_equal_weight_net']:>8.4f}")
        if horizon == FOLD_HORIZON:
            folds = m["ic"]["by_year"]
    if folds:
        print(f"\n  IC by year at {FOLD_HORIZON} sessions "
              "(stability matters more than the mean):")
        years = sorted(folds)
        print("  " + "  ".join(f"{y}:{folds[y]:+.3f}" for y in years))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("table", nargs="?")
    parser.add_argument("signal", nargs="?")
    parser.add_argument("--candidates", action="store_true",
                        help="evaluate all candidate-tier signals")
    parser.add_argument("--cost-bps", type=float, default=DEFAULT_COST_BPS)
    parser.add_argument("--intent", default="",
                        help="what question this evaluation is asking "
                             "(recorded in the trial log; keep it public-safe)")
    args = parser.parse_args()

    targets = CANDIDATES if args.candidates else [(args.table, args.signal)]
    if not all(t and s for t, s in targets):
        parser.error("give TABLE SIGNAL, or --candidates")

    con = duckdb.connect(WAREHOUSE, read_only=True)
    for table, signal_col in targets:
        report(con, table, signal_col, args.cost_bps, args.intent)
    print(f"\n{CAVEATS}")
    print(f"\ntrials attempted to date: {trial_count()} (trial_log.jsonl)")
    con.close()


if __name__ == "__main__":
    main()
