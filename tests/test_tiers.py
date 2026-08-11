"""Tier registry: valid, evidence-bearing, and candidates provably weightless."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import duckdb
import numpy as np
import pandas as pd
import pytest

from scoring.tiers import (
    candidate_variants, load_registry, rank_latest, scored_variant,
)


def test_shipped_registry_is_valid_and_resorted():
    registry = load_registry()
    tiers = {s["name"]: s["tier"] for s in registry["signals"]}
    # The user-ruled scored set, nothing else.
    assert [n for n, t in tiers.items() if t == "scored"] \
        == ["mom_12_1", "earnings_yield"]
    # All four incumbents are out of the scored tier.
    for incumbent in ("change_30d_pct", "pe_ratio", "rsi", "mfi"):
        assert tiers[incumbent] == "monitored"


def test_every_tier_change_is_a_recorded_event():
    registry = load_registry()
    events = registry["events"]
    promoted = {e["signal"] for e in events if e["action"] == "promote"}
    demoted = {e["signal"] for e in events if e["action"] == "demote"}
    assert promoted == {"mom_12_1", "earnings_yield"}
    assert {"change_30d_pct", "pe_ratio", "rsi", "mfi"} <= demoted
    for event in events:
        if event["action"] in ("promote", "demote"):
            assert event["reason"]
            assert event["trial_count_at_decision"] > 0


def test_calls_config_matches_ruled_defaults():
    calls = load_registry()["calls"]
    assert calls["enter_percentile"] == 0.90
    assert calls["exit_percentile"] == 0.50
    assert calls["haircut"] == 0.5
    assert calls["first_round_month"] == "2026-08"  # prospective, no backfill
    assert calls["drift"] == {"below_mean_rounds": 5, "below_p10_rounds": 3,
                              "fold_t_bar": 2.0}


def test_malformed_calls_block_fails_loudly(tmp_path):
    import json
    base = load_registry()
    for calls in (
        None,                                              # block missing
        {"enter_percentile": 0.5, "exit_percentile": 0.9,  # inverted hysteresis
         "haircut": 0.5,
         "drift": {"below_mean_rounds": 5, "below_p10_rounds": 3,
                   "fold_t_bar": 2.0}},
        {"enter_percentile": 0.9, "exit_percentile": 0.5,  # drift incomplete
         "haircut": 0.5, "drift": {"below_mean_rounds": 5}},
    ):
        broken = dict(base, calls=calls)
        if calls is None:
            del broken["calls"]
        path = tmp_path / "registry.json"
        path.write_text(json.dumps(broken))
        with pytest.raises(ValueError):
            load_registry(str(path))


def test_scored_variant_contains_only_scored_signals():
    variant = scored_variant(load_registry())
    names = [c["name"] for c in variant["components"]]
    assert names == ["mom_12_1", "earnings_yield"]


def test_candidate_variants_skip_uncomputed():
    names = [v["name"] for v in candidate_variants(load_registry())]
    assert "cand_beta" not in names  # computed: false
    assert "cand_gross_profitability" in names


def _warehouse():
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE silver_signals (symbol VARCHAR, "
                "as_of_date TIMESTAMP, change_30d_pct DOUBLE, "
                "change_365d_pct DOUBLE)")
    con.execute("CREATE TABLE gold_candidate_signals (symbol VARCHAR, "
                "as_of_date TIMESTAMP, earnings_yield DOUBLE, "
                "gross_profitability DOUBLE, roe DOUBLE)")
    rows = [("AAA", 40.0, 2.0, 0.06, 0.5, 0.3),   # strong mom, strong E/P
            ("BBB", 10.0, 1.0, 0.03, 0.2, 0.1),
            ("CCC", -5.0, 0.5, 0.01, 0.9, 0.9)]   # weak scored, best candidates
    for symbol, y365, m30, ep, gp, roe in rows:
        con.execute("INSERT INTO silver_signals VALUES (?, '2026-08-11', ?, ?)",
                    [symbol, m30, y365])
        con.execute("INSERT INTO gold_candidate_signals VALUES "
                    "(?, '2026-08-11', ?, ?, ?)", [symbol, ep, gp, roe])
    return con


def test_rank_latest_orders_by_scored_signals():
    con = _warehouse()
    rank_latest(con, load_registry())
    df = con.execute("SELECT symbol, composite_rank FROM "
                     "gold_watchlist_ranked_v2 ORDER BY composite_rank").df()
    assert list(df["symbol"]) == ["AAA", "BBB", "CCC"]


def test_candidates_provably_contribute_zero_weight():
    # The 10b acceptance test: pervert every candidate value and the v2
    # scores must not move by a single bit.
    con = _warehouse()
    rank_latest(con, load_registry())
    before = con.execute("SELECT symbol, composite_score FROM "
                         "gold_watchlist_ranked_v2 ORDER BY symbol").df()

    con.execute("UPDATE gold_candidate_signals SET "
                "gross_profitability = -999, roe = 999")
    rank_latest(con, load_registry())
    after = con.execute("SELECT symbol, composite_score FROM "
                        "gold_watchlist_ranked_v2 ORDER BY symbol").df()
    pd.testing.assert_frame_equal(before, after)


def test_missing_scored_input_scores_neutral_not_worst():
    con = _warehouse()
    con.execute("UPDATE gold_candidate_signals SET earnings_yield = NULL "
                "WHERE symbol = 'BBB'")
    rank_latest(con, load_registry())
    row = con.execute("SELECT earnings_yield_pct FROM "
                      "gold_watchlist_ranked_v2 WHERE symbol='BBB'").fetchone()
    assert row[0] == 0.5
