"""End-to-end: the full local chain on synthetic data, bronze to verdict.

One test walks the exact path build_local.py takes - raw prices with a
dividend and a split, EDGAR-shaped facts, indicator signals, candidate
signals, forward returns - then evaluates a signal and logs the trial. Every
stage's guarantee is asserted where a downstream consumer depends on it.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

import duckdb
import numpy as np
import pandas as pd

from build_local import build_gold, build_signals
from backtest.harness import evaluation_frame
from backtest.metrics import evaluate
from backtest.returns import build_forward_returns
from backtest.trials import log_trial, trial_count
from common.fundamentals import build_fundamental_tables
from scoring.candidates import build_candidate_signals

SYMBOLS = ["FLAT", "GROW", "SPY"]


def _seed_warehouse(con):
    con.execute("""
        CREATE TABLE bronze_prices (
            symbol VARCHAR, date DATE, open DOUBLE, high DOUBLE, low DOUBLE,
            close DOUBLE, volume BIGINT, dividend DOUBLE, split_ratio DOUBLE)
    """)
    dates = pd.bdate_range("2023-01-02", periods=320)
    rng = np.random.default_rng(42)
    drift = {"GROW": 1.0015, "FLAT": 1.0, "SPY": 1.0005}
    for symbol in SYMBOLS:
        price = 100.0
        for i, date in enumerate(dates):
            price *= drift[symbol] * (1 + rng.normal(0, 0.004))
            dividend = 0.5 if (symbol == "GROW" and i == 150) else 0.0
            split = 2.0 if (symbol == "GROW" and i == 200) else None
            if split:
                price /= split
            con.execute(
                "INSERT INTO bronze_prices VALUES (?,?,?,?,?,?,?,?,?)",
                [symbol, str(date.date()), price, price * 1.01, price * 0.99,
                 price, 1_000_000, dividend, split])

    con.execute("""
        CREATE TABLE bronze_fundamentals (
            symbol VARCHAR, cik VARCHAR, taxonomy VARCHAR, tag VARCHAR,
            unit VARCHAR, start_date DATE, end_date DATE, value DOUBLE,
            fiscal_year INTEGER, fiscal_period VARCHAR, form VARCHAR,
            filed DATE, frame VARCHAR,
            _run_id VARCHAR, _ingest_ts VARCHAR, _source_system VARCHAR,
            _source_event_ts VARCHAR, _load_type VARCHAR)
    """)
    facts = [("NetIncomeLoss", "2022-01-01", "2022-12-31", 200.0),
             ("Revenues", "2022-01-01", "2022-12-31", 1000.0),
             ("CostOfRevenue", "2022-01-01", "2022-12-31", 600.0),
             ("Assets", None, "2022-12-31", 2000.0),
             ("StockholdersEquity", None, "2022-12-31", 800.0)]
    for symbol, scale in [("GROW", 1.0), ("FLAT", 0.25)]:
        for tag, start, end, value in facts:
            # FLAT's income scales harder than its balance sheet, so the
            # two symbols' ratios differ and per-date ranks are computable.
            factor = 0.1 if (symbol == "FLAT" and tag == "NetIncomeLoss") \
                else scale
            con.execute(
                "INSERT INTO bronze_fundamentals VALUES "
                "(?,?, 'us-gaap', ?, 'USD', ?, ?, ?, 2022, 'FY', '10-K', "
                "'2023-08-01', NULL, 'r','t','sec_edgar','t','full')",
                [symbol, "1", tag, start, end, value * factor])
        con.execute(
            "INSERT INTO bronze_fundamentals VALUES "
            "(?,?, 'dei', 'EntityCommonStockSharesOutstanding', 'shares', "
            "NULL, '2023-07-31', 100.0, 2022, 'FY', '10-K', '2023-08-01', "
            "NULL, 'r','t','sec_edgar','t','full')", [symbol, "1"])


def test_full_local_chain(tmp_path):
    con = duckdb.connect(":memory:")
    _seed_warehouse(con)

    # Bronze -> silver: identical indicator function to the Spark job.
    signals, adjusted = build_signals(con, SYMBOLS)
    assert set(signals["symbol"]) == set(SYMBOLS)
    con.register("s", signals)
    con.execute("CREATE TABLE silver_signals AS SELECT * FROM s")
    con.register("a", adjusted)
    con.execute("CREATE TABLE silver_adjusted_prices AS SELECT * FROM a")

    # The split must be invisible in the adjusted series: no 50% day.
    grow = con.execute(
        "SELECT adj_close FROM silver_adjusted_prices "
        "WHERE symbol='GROW' ORDER BY date").df()["adj_close"]
    assert grow.pct_change().abs().max() < 0.20

    # Gold composite: every symbol ranked, scores inside [0, 1].
    build_gold(con)
    ranked = con.execute("SELECT composite_score, composite_rank "
                         "FROM gold_watchlist_ranked").df()
    assert len(ranked) == len(SYMBOLS)
    assert ranked["composite_score"].between(0, 1).all()
    assert set(ranked["composite_rank"]) == {1, 2, 3}

    # Fundamentals -> candidates: PIT boundary at the filing date.
    build_fundamental_tables(con)
    build_candidate_signals(con)
    n_sig, n_cand = con.execute(
        "SELECT (SELECT COUNT(*) FROM silver_signals), "
        "(SELECT COUNT(*) FROM gold_candidate_signals)").fetchone()
    assert n_cand == n_sig  # ASOF LEFT JOIN: universe never shrinks
    before, after = con.execute("""
        SELECT (SELECT roe FROM gold_candidate_signals
                WHERE symbol='GROW' AND as_of_date < '2023-08-01'
                ORDER BY as_of_date DESC LIMIT 1),
               (SELECT roe FROM gold_candidate_signals
                WHERE symbol='GROW' AND as_of_date >= '2023-08-01'
                ORDER BY as_of_date LIMIT 1)""").fetchone()
    assert before is None and after == 0.25

    # Forward returns: next-open fills, benchmark excess zero on itself.
    build_forward_returns(con, horizons=(5,), benchmark="SPY")
    bad = con.execute("SELECT COUNT(*) FROM backtest_forward_returns "
                      "WHERE entry_date <= as_of_date").fetchone()[0]
    assert bad == 0
    spy = con.execute("SELECT MAX(ABS(excess_return)) FROM "
                      "backtest_forward_returns WHERE symbol='SPY'").fetchone()[0]
    assert spy == 0.0

    # Trial logged before the verdict, then the verdict computes.
    log_path = str(tmp_path / "log.jsonl")
    log_trial("gold_candidate_signals", "roe_pct", (5,), 10.0,
              "e2e chain check", path=log_path)
    frame = evaluation_frame(con, "gold_candidate_signals", "roe_pct", 5)
    assert not frame.empty
    assert "SPY" not in set(frame["symbol"])
    result = evaluate(frame, cost_bps=10.0)
    assert result["ic"]["n_dates"] >= 3
    assert np.isfinite(result["ic"]["mean"])
    assert np.isfinite(result["excess_vs_equal_weight_net"])
    assert trial_count(log_path) == 1
