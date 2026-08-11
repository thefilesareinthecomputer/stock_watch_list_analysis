"""IC decay: how far forward does the ranking stay right?

    uv run python scripts/ic_decay.py

Evaluates the registry's scored composite, each scored component, and the
computed candidates at every monthly horizon from 1 to 12 months out. The
forecast window should come from this curve, not from an assumption
(SPEC-SIGNAL-TIERS §2 "Decay"). Results land in results/decay/ with the
same reproducibility contract as variant comparisons; every variant logs
one trial covering all rungs.
"""
import argparse
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

import duckdb  # noqa: E402

from backtest.costs import DEFAULT_COST_BPS  # noqa: E402
from backtest.harness import CAVEATS  # noqa: E402
from backtest.returns import DECAY_HORIZONS  # noqa: E402
from compare_variants import run, write_results  # noqa: E402
from scoring.tiers import candidate_variants, load_registry, scored_variant  # noqa: E402

WAREHOUSE = os.path.join(ROOT, "warehouse", "market.duckdb")
RESULTS_DIR = os.path.join(ROOT, "results", "decay")


def decay_targets(registry):
    """The composite, its components alone, and every computed candidate."""
    composite = scored_variant(registry)
    singles = [
        {"name": f"scored_{c['name']}", "components": [dict(c, weight=1.0)]}
        for c in composite["components"]
    ]
    return [composite] + singles + candidate_variants(registry)


def print_decay(results):
    names = list(results)
    print(f"\n{'months':>7}", *(f"{n[:18]:>20}" for n in names))
    print(f"{'':>7}", *(f"{'IC      t':>20}" for _ in names))
    for horizon in DECAY_HORIZONS:
        cells = []
        for name in names:
            m = results[name]["metrics"][str(horizon)]
            cells.append(f"{m['ic']['mean']:>10.4f} {m['ic']['t_stat']:>8.2f}")
        print(f"{horizon // 21:>7}", *(f"{c:>20}" for c in cells))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cost-bps", type=float, default=DEFAULT_COST_BPS)
    args = parser.parse_args()

    registry = load_registry()
    con = duckdb.connect(WAREHOUSE, read_only=True)
    results = run(con, decay_targets(registry), DECAY_HORIZONS, args.cost_bps)
    con.close()

    print_decay(results)
    write_results(results, RESULTS_DIR)
    print(f"\nresults recorded in {RESULTS_DIR}/")
    print(f"\n{CAVEATS}")


if __name__ == "__main__":
    main()
