"""Point-in-time semantics of the silver fundamentals build.

These tests pin the three rules that make EDGAR-derived metrics honest:
facts exist from their filed date, restatements win for the same period, and
an amendment to an old period can never roll the knowledge series backwards.
The last one is the quiet failure mode: a 10-K/A restating fiscal 2022 lands
AFTER the fiscal 2024 10-K, and a naive "latest filing wins" join would
regress every metric to 2022 values.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import duckdb
import pytest

from common.fundamentals import build_fundamental_tables

BRONZE_DDL = """
    CREATE TABLE bronze_fundamentals (
        symbol VARCHAR, cik VARCHAR, taxonomy VARCHAR, tag VARCHAR,
        unit VARCHAR, start_date DATE, end_date DATE, value DOUBLE,
        fiscal_year INTEGER, fiscal_period VARCHAR, form VARCHAR,
        filed DATE, frame VARCHAR,
        _run_id VARCHAR, _ingest_ts VARCHAR, _source_system VARCHAR,
        _source_event_ts VARCHAR, _load_type VARCHAR
    )
"""

CONTRACT = ("run", "ts", "sec_edgar", "ts", "full")


def _con():
    con = duckdb.connect(":memory:")
    con.execute(BRONZE_DDL)
    return con


def _fact(con, symbol, taxonomy, tag, start, end, value, fp, form, filed):
    con.execute(
        "INSERT INTO bronze_fundamentals VALUES "
        "(?, '0000000001', ?, ?, 'USD', ?, ?, ?, 2024, ?, ?, ?, NULL, "
        " ?, ?, ?, ?, ?)",
        [symbol, taxonomy, tag, start, end, value, fp, form, filed, *CONTRACT],
    )


def _metrics(con, symbol):
    return con.execute(
        "SELECT * FROM silver_fundamental_metrics WHERE symbol = ? "
        "ORDER BY filed", [symbol]
    ).df()


def test_fact_not_knowable_before_filed_date():
    con = _con()
    _fact(con, "A", "us-gaap", "NetIncomeLoss",
          "2023-01-01", "2023-12-31", 100.0, "FY", "10-K", "2024-02-15")
    build_fundamental_tables(con)

    rows = _metrics(con, "A")
    # The only knowledge event is the filing date itself - nothing exists
    # at period end.
    assert list(rows["filed"].astype(str)) == ["2024-02-15"]
    assert rows.iloc[0]["net_income"] == 100.0


def test_quarterly_and_ytd_facts_are_excluded():
    con = _con()
    _fact(con, "A", "us-gaap", "NetIncomeLoss",
          "2023-01-01", "2023-12-31", 100.0, "FY", "10-K", "2024-02-15")
    # Q3 flow and a nine-month YTD entry, both carrying the FY label styles
    # that sneak through naive filters.
    _fact(con, "A", "us-gaap", "NetIncomeLoss",
          "2023-07-01", "2023-09-30", 30.0, "Q3", "10-Q", "2023-10-30")
    _fact(con, "A", "us-gaap", "NetIncomeLoss",
          "2023-01-01", "2023-09-30", 80.0, "FY", "10-K", "2024-02-15")
    build_fundamental_tables(con)

    rows = _metrics(con, "A")
    assert len(rows) == 1
    assert rows.iloc[0]["net_income"] == 100.0


def test_restatement_wins_for_the_same_period():
    con = _con()
    _fact(con, "A", "us-gaap", "NetIncomeLoss",
          "2023-01-01", "2023-12-31", 100.0, "FY", "10-K", "2024-02-15")
    _fact(con, "A", "us-gaap", "NetIncomeLoss",
          "2023-01-01", "2023-12-31", 90.0, "FY", "10-K/A", "2024-06-01")
    build_fundamental_tables(con)

    rows = _metrics(con, "A")
    assert rows.iloc[0]["net_income"] == 100.0   # before the amendment
    assert rows.iloc[-1]["net_income"] == 90.0   # after it


def test_amendment_to_old_period_does_not_roll_back():
    con = _con()
    _fact(con, "A", "us-gaap", "NetIncomeLoss",
          "2022-01-01", "2022-12-31", 50.0, "FY", "10-K", "2023-02-15")
    _fact(con, "A", "us-gaap", "NetIncomeLoss",
          "2023-01-01", "2023-12-31", 100.0, "FY", "10-K", "2024-02-15")
    # Amendment restating fiscal 2022, filed after the 2023 10-K.
    _fact(con, "A", "us-gaap", "NetIncomeLoss",
          "2022-01-01", "2022-12-31", 45.0, "FY", "10-K/A", "2024-06-01")
    build_fundamental_tables(con)

    rows = _metrics(con, "A")
    # The knowledge event created by the amendment still reports fiscal 2023.
    assert rows.iloc[-1]["net_income"] == 100.0
    assert str(rows.iloc[-1]["period_end"])[:10] == "2023-12-31"


def test_foreign_annual_reports_are_included():
    con = _con()
    _fact(con, "A", "us-gaap", "NetIncomeLoss",
          "2023-01-01", "2023-12-31", 100.0, "FY", "20-F", "2024-04-20")
    build_fundamental_tables(con)

    assert _metrics(con, "A").iloc[0]["net_income"] == 100.0


def test_tag_priority_picks_the_preferred_revenue_tag():
    con = _con()
    _fact(con, "A", "us-gaap", "SalesRevenueNet",
          "2023-01-01", "2023-12-31", 500.0, "FY", "10-K", "2024-02-15")
    _fact(con, "A", "us-gaap", "Revenues",
          "2023-01-01", "2023-12-31", 510.0, "FY", "10-K", "2024-02-15")
    build_fundamental_tables(con)

    rows = _metrics(con, "A")
    assert rows.iloc[0]["revenues"] == 510.0


def test_tag_fallback_when_preferred_tag_absent():
    con = _con()
    _fact(con, "A", "us-gaap", "SalesRevenueNet",
          "2023-01-01", "2023-12-31", 500.0, "FY", "10-K", "2024-02-15")
    build_fundamental_tables(con)

    assert _metrics(con, "A").iloc[0]["revenues"] == 500.0


def test_gross_profit_falls_back_to_revenue_minus_cost():
    con = _con()
    _fact(con, "A", "us-gaap", "Revenues",
          "2023-01-01", "2023-12-31", 500.0, "FY", "10-K", "2024-02-15")
    _fact(con, "A", "us-gaap", "CostOfRevenue",
          "2023-01-01", "2023-12-31", 300.0, "FY", "10-K", "2024-02-15")
    _fact(con, "A", "us-gaap", "Assets",
          None, "2023-12-31", 1000.0, "FY", "10-K", "2024-02-15")
    build_fundamental_tables(con)

    row = _metrics(con, "A").iloc[0]
    assert row["gross_profit"] == 200.0
    assert row["gross_profitability"] == pytest.approx(0.2)


def test_ratios_are_null_not_infinite_on_zero_denominator():
    con = _con()
    _fact(con, "A", "us-gaap", "NetIncomeLoss",
          "2023-01-01", "2023-12-31", 100.0, "FY", "10-K", "2024-02-15")
    _fact(con, "A", "us-gaap", "StockholdersEquity",
          None, "2023-12-31", 0.0, "FY", "10-K", "2024-02-15")
    build_fundamental_tables(con)

    row = _metrics(con, "A").iloc[0]
    assert row["roe"] != row["roe"]  # NaN: NULL in DuckDB, never inf


def test_concepts_advance_independently():
    con = _con()
    # Shares outstanding is dated at the 10-K cover page, not fiscal year
    # end; both must appear on the same knowledge event.
    _fact(con, "A", "us-gaap", "NetIncomeLoss",
          "2023-01-01", "2023-12-31", 100.0, "FY", "10-K", "2024-02-15")
    _fact(con, "A", "dei", "EntityCommonStockSharesOutstanding",
          None, "2024-02-01", 1000.0, "FY", "10-K", "2024-02-15")
    build_fundamental_tables(con)

    row = _metrics(con, "A").iloc[0]
    assert row["net_income"] == 100.0
    assert row["shares_outstanding"] == 1000.0
    assert str(row["shares_date"])[:10] == "2024-02-01"
