"""Variants: data-defined, direction-correct, reproducible, always counted."""
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

import duckdb
import numpy as np
import pandas as pd
import pytest

from backtest.returns import build_forward_returns
from backtest.trials import trial_count
from compare_variants import run, write_results
from scoring.variants import (
    definition_hash, load_variants, score_table, validate_variant,
)

UP = {"name": "up", "components": [
    {"name": "m", "expression": "change_30d_pct", "ascending": True,
     "weight": 1.0}]}
DOWN = {"name": "down", "components": [
    {"name": "m", "expression": "change_30d_pct", "ascending": False,
     "weight": 1.0}]}


def _warehouse():
    """Three symbols, five months of sessions, a signal that tracks drift."""
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE silver_adjusted_prices "
                "(symbol VARCHAR, date DATE, adj_open DOUBLE, adj_close DOUBLE)")
    con.execute("CREATE TABLE silver_signals "
                "(symbol VARCHAR, as_of_date TIMESTAMP, change_30d_pct DOUBLE)")
    con.execute("CREATE TABLE gold_candidate_signals "
                "(symbol VARCHAR, as_of_date TIMESTAMP, earnings_yield DOUBLE,"
                " gross_profitability DOUBLE, roe DOUBLE,"
                " earnings_yield_pct DOUBLE, gross_profitability_pct DOUBLE,"
                " roe_pct DOUBLE)")
    dates = pd.bdate_range("2024-01-01", periods=110)
    drift = {"AAA": 1.002, "BBB": 1.0, "SPY": 1.001}
    rng = np.random.default_rng(3)
    for symbol, d in drift.items():
        price = 100.0
        for date in dates:
            price *= d * (1 + rng.normal(0, 0.002))
            con.execute("INSERT INTO silver_adjusted_prices VALUES (?,?,?,?)",
                        [symbol, str(date.date()), price, price])
            con.execute("INSERT INTO silver_signals VALUES (?,?,?)",
                        [symbol, str(date.date()), (d - 1) * 1000])
            con.execute("INSERT INTO gold_candidate_signals VALUES "
                        "(?,?,?,?,?,?,?,?)",
                        [symbol, str(date.date()), 0.05, 0.3, 0.2,
                         0.5, 0.5, 0.5])
    build_forward_returns(con, horizons=(5,), benchmark="SPY")
    return con


def test_shipped_variants_file_is_valid():
    variants = load_variants()
    assert len(variants) >= 2
    names = [v["name"] for v in variants]
    assert len(names) == len(set(names))
    for v in variants:
        assert len(definition_hash(v)) == 64


def test_validation_rejects_sql_smuggling():
    bad = {"name": "x", "components": [
        {"name": "c", "expression": "1; DROP TABLE silver_signals",
         "ascending": True, "weight": 1.0}]}
    with pytest.raises(ValueError):
        validate_variant(bad)
    bad["components"][0]["expression"] = "rsi -- comment"
    with pytest.raises(ValueError):
        validate_variant(bad)


def test_direction_flag_inverts_the_ranking():
    con = _warehouse()
    up = con.execute(f"SELECT symbol, score FROM "
                     f"{score_table(con, UP, 5, 'up_t')} "
                     "ORDER BY as_of_date, symbol").df()
    down = con.execute(f"SELECT symbol, score FROM "
                       f"{score_table(con, DOWN, 5, 'down_t')} "
                       "ORDER BY as_of_date, symbol").df()
    # AAA has the strongest drift signal: best under ascending, worst under
    # descending. Scores must invert, not merely differ.
    assert up.loc[up["symbol"] == "AAA", "score"].iloc[0] == 1.0
    assert down.loc[down["symbol"] == "AAA", "score"].iloc[0] == 0.0
    assert (up["score"] + down["score"] == 1.0).all()


def test_missing_component_scores_neutral():
    con = _warehouse()
    con.execute("UPDATE gold_candidate_signals SET roe = NULL "
                "WHERE symbol = 'BBB'")
    variant = {"name": "roe_only", "components": [
        {"name": "r", "expression": "roe", "ascending": True, "weight": 2.0}]}
    scores = con.execute(f"SELECT symbol, score FROM "
                         f"{score_table(con, variant, 5, 'roe_t')}").df()
    assert (scores.loc[scores["symbol"] == "BBB", "score"] == 0.5).all()


def test_two_variants_one_command_differ_and_are_counted(tmp_path):
    con = _warehouse()
    log = str(tmp_path / "log.jsonl")
    results = run(con, [UP, DOWN], horizons=(5,), cost_bps=10.0,
                  log_path=log)
    assert set(results) == {"up", "down"}
    assert (results["up"]["metrics"]["5"]["ic"]["mean"]
            != results["down"]["metrics"]["5"]["ic"]["mean"])
    assert trial_count(log) == 2  # every variant counted, before results


def test_rerun_reproduces_recorded_results_exactly(tmp_path):
    con = _warehouse()
    log = str(tmp_path / "log.jsonl")
    kwargs = dict(horizons=(5,), cost_bps=10.0, log_path=log)

    first = run(con, [UP], **kwargs)
    write_results(first, str(tmp_path / "r1"))
    second = run(con, [UP], **kwargs)
    write_results(second, str(tmp_path / "r2"))

    with open(tmp_path / "r1" / "up.json", "rb") as f1, \
         open(tmp_path / "r2" / "up.json", "rb") as f2:
        assert f1.read() == f2.read()  # byte-identical, task 9's criterion


def test_recorded_result_names_its_methodology(tmp_path):
    con = _warehouse()
    results = run(con, [UP], horizons=(5,), cost_bps=10.0,
                  log_path=str(tmp_path / "log.jsonl"))
    write_results(results, str(tmp_path / "r"))
    with open(tmp_path / "r" / "up.json") as f:
        payload = json.load(f)
    assert payload["definition"] == UP
    assert payload["definition_hash"] == definition_hash(UP)
    assert payload["settings"]["cost_bps"] == 10.0
    assert "max_as_of_date" in payload["data"]
