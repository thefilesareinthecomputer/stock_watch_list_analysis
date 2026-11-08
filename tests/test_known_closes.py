"""Proof that stored history equals what traded: published closes vs bronze.

Two price bugs passed self-referential tests (plan.md gotcha 0j). This
test compares bronze_prices with closes published outside Yahoo, kept in
tests/fixtures/known_closes.json with the citation per row. Before the
warehouse is migrated to traded terms the comparison runs through the same
conversion the migration uses, so the proof is available before the
migration as well as after it.
"""
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import pytest

HERE = os.path.dirname(__file__)
FIXTURE = os.path.join(HERE, "fixtures", "known_closes.json")
WAREHOUSE = os.path.join(HERE, "..", "warehouse", "market.duckdb")
POSITIONS = os.path.join(HERE, "..", "knowledge", "POSITIONS.md")
REQUIRED = {("AAPL", "2020-08-28"), ("WMT", "2024-02-23"),
            ("CMG", "2024-06-25"), ("APH", "2026-09-02")}

with open(FIXTURE) as f:
    FIX = json.load(f)
ROWS = FIX["rows"]


def test_fixture_has_at_least_ten_rows_each_cited_outside_yahoo():
    assert len(ROWS) >= 10
    for row in ROWS:
        assert "yahoo" not in row["source_url"].lower(), row["symbol"]
        assert row["source_title"] and row["quote"], row["symbol"]
        # The quoted sentence states the figure itself.
        assert f"{row['close']:.2f}" in row["quote"].replace(",", ""), row["symbol"]


def test_fixture_covers_the_required_split_cases():
    assert REQUIRED <= {(r["symbol"], r["date"]) for r in ROWS}


def test_fixture_rows_are_unique_per_symbol_and_date():
    keys = [(r["symbol"], r["date"]) for r in ROWS]
    assert len(keys) == len(set(keys))


@pytest.mark.skipif(not os.path.exists(POSITIONS),
                    reason="no positions file on this machine")
def test_fixture_names_no_held_symbol():
    from common.positions import held_symbols, load_positions

    held = set(held_symbols(load_positions(POSITIONS)))
    assert not {r["symbol"] for r in ROWS} & held


def test_checker_reports_ok_fail_and_absent():
    import duckdb

    from common.known_closes import check_known_closes
    from common.warehouse_meta import BRONZE_TERMS, TRADED, set_meta

    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE bronze_prices (symbol VARCHAR, date DATE, close DOUBLE)")
    con.execute("INSERT INTO bronze_prices VALUES ('GOOD', '2020-01-02', 10.0), "
                "('BAD', '2020-01-02', 10.5)")
    set_meta(con, BRONZE_TERMS, TRADED)
    rows = [{"symbol": "GOOD", "date": "2020-01-02", "close": 10.0},
            {"symbol": "BAD", "date": "2020-01-02", "close": 10.0},
            {"symbol": "NONE", "date": "2020-01-02", "close": 10.0}]
    assert [r[4] for r in check_known_closes(con, rows)] == ["ok", "FAIL", "absent"]


@pytest.mark.skipif(not os.path.exists(WAREHOUSE),
                    reason="local warehouse not built; run scripts/backfill.py")
@pytest.mark.parametrize("row", ROWS, ids=[f"{r['symbol']}-{r['date']}" for r in ROWS])
def test_bronze_reproduces_the_published_close(row):
    """Before the migration the checker converts the served close through
    the split log (what the migration will store); after it, the stored
    close itself."""
    import duckdb

    from common.known_closes import check_known_closes

    con = duckdb.connect(WAREHOUSE, read_only=True)
    try:
        (result,) = check_known_closes(con, [row])
    finally:
        con.close()
    symbol, day, published, stored, status = result
    if status == "absent":
        pytest.skip(f"{symbol} {day} not in the warehouse")
    assert status == "ok", (f"{symbol} {day}: bronze {stored:.4f}, published "
                            f"{published} ({row['source_title']})")
