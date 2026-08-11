"""Candidate signal construction: direction, nulls, splits, point-in-time.

Direction is settled by executing the SQL, never by argument (plan.md
gotcha 0). Each test builds the tables in an in-memory DuckDB and asserts on
what actually comes out.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import duckdb
import pytest

from scoring.candidates import build_candidate_signals

ASOF = "2024-06-03"


def _con():
    con = duckdb.connect(":memory:")
    con.execute("""
        CREATE TABLE bronze_prices (
            symbol VARCHAR, date DATE, close DOUBLE, split_ratio DOUBLE)
    """)
    con.execute("""
        CREATE TABLE silver_signals (symbol VARCHAR, as_of_date DATE)
    """)
    con.execute("""
        CREATE TABLE silver_fundamental_metrics (
            symbol VARCHAR, filed DATE, net_income DOUBLE,
            shares_outstanding DOUBLE, shares_date DATE,
            roe DOUBLE, gross_profitability DOUBLE, revenues DOUBLE)
    """)
    con.execute("CREATE TABLE bronze_entity (symbol VARCHAR, cik VARCHAR, "
                "sic VARCHAR, sic_description VARCHAR, entity_name VARCHAR, "
                "_ingest_ts VARCHAR)")
    return con


def _price(con, symbol, date, close, split=None):
    con.execute("INSERT INTO bronze_prices VALUES (?, ?, ?, ?)",
                [symbol, date, close, split])


def _signal(con, symbol, date=ASOF):
    con.execute("INSERT INTO silver_signals VALUES (?, ?)", [symbol, date])


def _metrics(con, symbol, filed, net_income=None, shares=None,
             shares_date=None, roe=None, gp=None, revenues=1.0):
    con.execute("INSERT INTO silver_fundamental_metrics VALUES "
                "(?, ?, ?, ?, ?, ?, ?, ?)",
                [symbol, filed, net_income, shares, shares_date, roe, gp,
                 revenues])


def _row(con, symbol, date=ASOF):
    df = con.execute(
        "SELECT * FROM gold_candidate_signals "
        "WHERE symbol = ? AND as_of_date = ?", [symbol, date]).df()
    assert len(df) == 1
    return df.iloc[0]


def test_higher_metric_earns_higher_percentile():
    con = _con()
    for sym, roe, gp in [("GOOD", 0.30, 0.5), ("MID", 0.10, 0.3),
                         ("BAD", -0.05, 0.1)]:
        _price(con, sym, ASOF, 100.0)
        _signal(con, sym)
        _metrics(con, sym, "2024-02-01", net_income=roe * 1000, shares=10,
                 shares_date="2024-02-01", roe=roe, gp=gp)
        _price(con, sym, "2024-02-01", 100.0)
    build_candidate_signals(con)

    assert _row(con, "GOOD")["roe_pct"] == 1.0
    assert _row(con, "BAD")["roe_pct"] == 0.0
    assert _row(con, "GOOD")["gross_profitability_pct"] == 1.0
    assert _row(con, "GOOD")["earnings_yield_pct"] == 1.0
    assert _row(con, "BAD")["earnings_yield_pct"] == 0.0  # negative earnings rank worst


def test_missing_fundamentals_stay_null_and_do_not_distort_ranks():
    con = _con()
    for sym in ("A", "B", "NOFUND"):
        _price(con, sym, ASOF, 100.0)
        _signal(con, sym)
    _price(con, "A", "2024-02-01", 100.0)
    _price(con, "B", "2024-02-01", 100.0)
    _metrics(con, "A", "2024-02-01", roe=0.2)
    _metrics(con, "B", "2024-02-01", roe=0.1)
    build_candidate_signals(con)

    nofund = _row(con, "NOFUND")
    assert nofund["roe_pct"] != nofund["roe_pct"]  # NULL -> NaN, not worst
    # The two real values still span the full range: nulls were excluded
    # from the window rather than squatting on the top ranks.
    assert _row(con, "A")["roe_pct"] == 1.0
    assert _row(con, "B")["roe_pct"] == 0.0


def test_earnings_yield_uses_split_adjusted_shares():
    con = _con()
    _signal(con, "S")
    _price(con, "S", "2024-02-01", 200.0)
    _price(con, "S", "2024-04-01", 100.0, split=2.0)  # 2-for-1 after filing
    _price(con, "S", ASOF, 100.0)
    _metrics(con, "S", "2024-02-01", net_income=1000.0, shares=100.0,
             shares_date="2024-02-01")
    build_candidate_signals(con)

    # Market cap must use 200 post-split shares: 100 * (close 100) * 2.
    assert _row(con, "S")["earnings_yield"] == pytest.approx(1000.0 / 20000.0)


def test_stale_share_count_nulls_earnings_yield():
    # A share count older than ~400 days (BRK's undimensioned dei count is
    # 15 years stale) must yield NULL, never a fictional market cap.
    con = _con()
    _signal(con, "S")
    _price(con, "S", "2022-01-03", 100.0)
    _price(con, "S", ASOF, 100.0)
    _metrics(con, "S", "2022-01-03", net_income=1000.0, shares=100.0,
             shares_date="2022-01-03", roe=0.2)
    build_candidate_signals(con)

    row = _row(con, "S")
    assert row["earnings_yield"] != row["earnings_yield"]  # NULL
    assert row["roe"] == 0.2  # ratios without a price basis survive


def test_fundamentals_filed_after_as_of_are_not_visible():
    con = _con()
    _signal(con, "S", "2024-01-15")
    _price(con, "S", "2024-01-15", 100.0)
    _price(con, "S", ASOF, 100.0)
    _signal(con, "S", ASOF)
    _metrics(con, "S", "2024-02-01", net_income=1000.0, shares=100.0,
             shares_date="2024-02-01", roe=0.2)
    build_candidate_signals(con)

    before = _row(con, "S", "2024-01-15")
    after = _row(con, "S", ASOF)
    assert before["roe"] != before["roe"]  # filing not yet knowable
    assert after["roe"] == 0.2


def test_non_operating_sic_scores_neutral_even_with_revenues():
    # SLV reports a Revenues tag (silver sold to pay expenses), so the
    # revenue marker alone missed it. SIC 6221 is the discriminator.
    con = _con()
    con.execute("INSERT INTO bronze_entity VALUES "
                "('SLV', '1', '6221', 'Commodity Contracts', 'Trust', 't')")
    _signal(con, "SLV")
    _price(con, "SLV", "2024-02-01", 30.0)
    _price(con, "SLV", ASOF, 30.0)
    _metrics(con, "SLV", "2024-02-01", net_income=21e9, shares=5.8e8,
             shares_date="2024-02-01", roe=4.0, revenues=3.1e9)
    build_candidate_signals(con)

    row = _row(con, "SLV")
    assert row["earnings_yield"] != row["earnings_yield"]  # NULL
    assert row["roe"] != row["roe"]  # NULL


def test_trust_without_revenues_scores_neutral_on_earnings_ratios():
    # A commodity trust files a 10-K whose net income is unrealized metal
    # appreciation. No reported revenue -> not an operating company -> its
    # E/P and ROE are NULL, never a rank-topping fiction (SLV ranked #3
    # before this guard).
    con = _con()
    _signal(con, "TRUST")
    _price(con, "TRUST", "2024-02-01", 30.0)
    _price(con, "TRUST", ASOF, 30.0)
    _metrics(con, "TRUST", "2024-02-01", net_income=5000.0, shares=100.0,
             shares_date="2024-02-01", roe=0.9, revenues=None)
    build_candidate_signals(con)

    row = _row(con, "TRUST")
    assert row["earnings_yield"] != row["earnings_yield"]  # NULL
    assert row["roe"] != row["roe"]  # NULL


def test_symbol_with_no_filings_ever_still_appears():
    # The ASOF LEFT JOIN guarantee: the universe never silently shrinks
    # (plan.md gotcha 0a).
    con = _con()
    _signal(con, "ETF")
    _price(con, "ETF", ASOF, 500.0)
    build_candidate_signals(con)

    row = _row(con, "ETF")
    assert row["earnings_yield"] != row["earnings_yield"]
    assert row["roe"] != row["roe"]
