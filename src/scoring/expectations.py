"""Frozen expectations: what a call round will be graded against.

Attached to every round at emit time and never restated
(SPEC-BUY-SELL-CALLS "Frozen expectation"): the walk-forward distribution
of top-decile net excess per horizon rung, HAIRCUT by the out-of-sample
decay factor (registry `calls.haircut`, default 0.5 - the McLean-Pontiff
midpoint), plus fold-level IC. The sha256 of the decay results file makes
the claim traceable to the exact recorded evidence; settlement grades a
vintage against its own frozen copy, even if the registry or the decay
results have moved since.
"""
import hashlib
import json
import os

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DECAY_DIR = os.path.join(ROOT, "results", "decay")

# The grading rungs (21/63/126 sessions). The full decay ladder informs the
# curve; a vintage is settled at these.
EXPECTATION_HORIZONS = (21, 63, 126)


def composite_decay_path(registry):
    """Where ic_decay.py records the scored composite's results."""
    name = f"methodology_{registry['methodology_version'].replace('-', '_')}"
    return os.path.join(DECAY_DIR, f"{name}.json")


def freeze_expectation(registry, decay_path=None):
    """The expectation dict for one round, from recorded decay results.

    Refuses to freeze from a results file that lacks the excess
    distribution: an expectation without p10/p90 cannot separate a bad
    window from drift, and a silent fallback here would poison every
    later settlement of the round.
    """
    path = decay_path or composite_decay_path(registry)
    with open(path, "rb") as f:
        blob = f.read()
    results = json.loads(blob)
    haircut = registry["calls"]["haircut"]

    horizons = {}
    for horizon in EXPECTATION_HORIZONS:
        metrics = results["metrics"].get(str(horizon))
        if metrics is None or "excess_net_distribution" not in metrics:
            raise ValueError(
                f"{os.path.basename(path)}: horizon {horizon} has no "
                "excess_net_distribution - regenerate with scripts/ic_decay.py")
        dist = metrics["excess_net_distribution"]
        horizons[str(horizon)] = {
            "excess_net": {k: dist[k] for k in ("mean", "p10", "p90", "n_dates")},
            "excess_net_haircut": {k: dist[k] * haircut
                                   for k in ("mean", "p10", "p90")},
            "ic_mean": metrics["ic"]["mean"],
            "ic_by_year": metrics["ic"]["by_year"],
        }

    return {
        "methodology_version": registry["methodology_version"],
        "definition_hash": results["definition_hash"],
        "haircut": haircut,
        "horizons": horizons,
        "source_file": os.path.basename(path),
        "source_sha256": hashlib.sha256(blob).hexdigest(),
    }
