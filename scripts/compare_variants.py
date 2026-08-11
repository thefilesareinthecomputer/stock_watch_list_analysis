"""Compare scoring variants side by side - one command, recorded results.

    uv run python scripts/compare_variants.py
    uv run python scripts/compare_variants.py --horizons 126 252
    uv run python scripts/compare_variants.py --variants path/to/variants.json

Every variant run logs a trial BEFORE its result exists. Results land in
results/variants/<name>.json with the full definition, its hash, and a data
fingerprint - so a recorded result names its exact methodology, and
re-running an unchanged variant on unchanged data reproduces the file
byte for byte (task 9's acceptance criterion).
"""
import argparse
import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

import duckdb  # noqa: E402

from backtest.costs import DEFAULT_COST_BPS  # noqa: E402
from backtest.harness import CAVEATS, evaluation_frame  # noqa: E402
from backtest.metrics import evaluate  # noqa: E402
from backtest.returns import HORIZONS  # noqa: E402
from backtest.trials import TRIAL_LOG, log_trial  # noqa: E402
from scoring.variants import (  # noqa: E402
    VARIANTS_PATH, definition_hash, load_variants, score_table,
)

WAREHOUSE = os.path.join(ROOT, "warehouse", "market.duckdb")
RESULTS_DIR = os.path.join(ROOT, "results", "variants")


def data_fingerprint(con):
    """What the evaluation saw: enough to explain a changed result."""
    rows, symbols, max_date = con.execute(
        "SELECT COUNT(*), COUNT(DISTINCT symbol), MAX(as_of_date) "
        "FROM silver_signals").fetchone()
    return {"silver_signal_rows": rows, "symbols": symbols,
            "max_as_of_date": str(max_date)[:10]}


def run(con, variants, horizons, cost_bps, log_path=TRIAL_LOG):
    """Evaluate each variant at each horizon; trial logged before results."""
    results = {}
    for variant in variants:
        log_trial(f"variant:{variant['name']}",
                  definition_hash(variant)[:12], horizons, cost_bps,
                  variant.get("intent", ""), path=log_path)
        per_horizon = {}
        for horizon in horizons:
            table = score_table(con, variant, horizon)
            frame = evaluation_frame(con, table, "score", horizon)
            per_horizon[str(horizon)] = evaluate(frame, cost_bps)
        results[variant["name"]] = {
            "definition": variant,
            "definition_hash": definition_hash(variant),
            "settings": {"horizons": list(horizons), "cost_bps": cost_bps,
                         "eval_dates": "monthly"},
            "data": data_fingerprint(con),
            "metrics": per_horizon,
        }
    return results


def print_comparison(results, horizons):
    for horizon in horizons:
        print(f"\n=== horizon {horizon} sessions ===")
        print(f"{'variant':<22} {'dates':>6} {'IC':>8} {'t':>7} {'monot':>6} "
              f"{'hit':>6} {'turn':>6} {'gross':>8} {'net':>8}")
        for name, r in results.items():
            m = r["metrics"][str(horizon)]
            print(f"{name:<22} {m['ic']['n_dates']:>6} "
                  f"{m['ic']['mean']:>8.4f} {m['ic']['t_stat']:>7.2f} "
                  f"{m['monotonicity']:>6.2f} {m['hit_rate']:>6.2f} "
                  f"{m['turnover']:>6.2f} "
                  f"{m['excess_vs_equal_weight_gross']:>8.4f} "
                  f"{m['excess_vs_equal_weight_net']:>8.4f}")


def write_results(results, directory=RESULTS_DIR):
    os.makedirs(directory, exist_ok=True)
    for name, payload in results.items():
        path = os.path.join(directory, f"{name}.json")
        with open(path, "w") as f:
            json.dump(payload, f, indent=2, sort_keys=True, default=float)
            f.write("\n")
    return directory


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--variants", default=VARIANTS_PATH)
    parser.add_argument("--horizons", type=int, nargs="+",
                        default=list(HORIZONS))
    parser.add_argument("--cost-bps", type=float, default=DEFAULT_COST_BPS)
    args = parser.parse_args()

    variants = load_variants(args.variants)
    con = duckdb.connect(WAREHOUSE, read_only=True)
    results = run(con, variants, args.horizons, args.cost_bps)
    con.close()

    print_comparison(results, args.horizons)
    directory = write_results(results)
    print(f"\nresults recorded in {directory}/ "
          "(re-running an unchanged variant reproduces its file exactly)")
    print(f"\n{CAVEATS}")


if __name__ == "__main__":
    main()
