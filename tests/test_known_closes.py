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


@pytest.mark.skipif(not os.path.exists(WAREHOUSE),
                    reason="local warehouse not built; run scripts/backfill.py")
@pytest.mark.parametrize("row", ROWS, ids=[f"{r['symbol']}-{r['date']}" for r in ROWS])
def test_bronze_reproduces_the_published_close(row):
    import duckdb

    from common.corporate_actions import splits_for
    from common.traded_prices import to_traded
    from common.warehouse_meta import bronze_is_traded

    con = duckdb.connect(WAREHOUSE, read_only=True)
    try:
        frame = con.execute(
            "SELECT date, close FROM bronze_prices WHERE symbol = ? AND date = ?",
            [row["symbol"], row["date"]]).df()
        if frame.empty:
            pytest.skip(f"{row['symbol']} {row['date']} not in the warehouse")
        if bronze_is_traded(con):
            stored = float(frame["close"].iloc[0])
        else:
            # Not yet migrated: the stored close is Yahoo's restated value;
            # check what the migration will store.
            stored = float(to_traded(frame, splits_for(con, row["symbol"]))
                           ["close"].iloc[0])
    finally:
        con.close()
    assert abs(stored - row["close"]) < 0.01, (
        f"{row['symbol']} {row['date']}: bronze {stored:.4f}, "
        f"published {row['close']} ({row['source_title']})")
