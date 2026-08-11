"""Frozen expectations: haircut math, source hash, and loud refusals."""
import hashlib
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import pytest

from scoring.expectations import composite_decay_path, freeze_expectation
from scoring.tiers import load_registry


def _decay_file(tmp_path, drop_distribution=False):
    metrics = {}
    for h in (21, 63, 126, 252):
        m = {"ic": {"mean": 0.05, "t_stat": 4.0, "n_dates": 190,
                    "by_year": {"2024": 0.06, "2025": 0.04}},
             "excess_net_distribution": {"mean": 0.02, "p10": -0.04,
                                         "p90": 0.09, "n_dates": 190}}
        if drop_distribution:
            del m["excess_net_distribution"]
        metrics[str(h)] = m
    path = tmp_path / "methodology_v2_local.json"
    path.write_text(json.dumps({"definition_hash": "abc123",
                                "metrics": metrics}))
    return str(path)


def test_freeze_haircuts_and_hashes_the_source(tmp_path):
    path = _decay_file(tmp_path)
    registry = load_registry()
    exp = freeze_expectation(registry, decay_path=path)

    assert set(exp["horizons"]) == {"21", "63", "126"}  # grading rungs only
    rung = exp["horizons"]["126"]
    assert rung["excess_net"]["mean"] == 0.02
    assert rung["excess_net_haircut"]["mean"] == pytest.approx(0.01)
    assert rung["excess_net_haircut"]["p10"] == pytest.approx(-0.02)
    assert rung["ic_by_year"] == {"2024": 0.06, "2025": 0.04}
    assert exp["haircut"] == 0.5
    assert exp["definition_hash"] == "abc123"

    with open(path, "rb") as f:
        assert exp["source_sha256"] == hashlib.sha256(f.read()).hexdigest()


def test_freeze_refuses_results_without_distribution(tmp_path):
    path = _decay_file(tmp_path, drop_distribution=True)
    with pytest.raises(ValueError, match="excess_net_distribution"):
        freeze_expectation(load_registry(), decay_path=path)


def test_default_path_names_the_registry_methodology():
    path = composite_decay_path(load_registry())
    assert path.endswith(os.path.join("results", "decay",
                                      "methodology_v2_local.json"))
