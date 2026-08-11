"""Positions parser: account sections, fractional quantities, loud failures.

Fixtures only - the real POSITIONS.md is private and never read by tests.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import duckdb
import pandas as pd
import pytest

from common.positions import (
    build_held_table, check_held_freshness, held_symbols, load_positions,
    parse_positions,
)

FIXTURE = """\
# ROTH ETF
AAA 4
BBB 160

# BROKERAGE STOCK
aaa 2.5
CCC 1.006
"""


def test_parse_sections_fractions_and_case():
    positions = parse_positions(FIXTURE)
    assert positions == [
        {"account": "ROTH ETF", "symbol": "AAA", "quantity": 4.0},
        {"account": "ROTH ETF", "symbol": "BBB", "quantity": 160.0},
        {"account": "BROKERAGE STOCK", "symbol": "AAA", "quantity": 2.5},
        {"account": "BROKERAGE STOCK", "symbol": "CCC", "quantity": 1.006},
    ]
    # Cross-account holdings collapse to one symbol.
    assert held_symbols(positions) == ["AAA", "BBB", "CCC"]


@pytest.mark.parametrize("text", [
    "AAA 4",                    # position before any account header
    "# ACCT\nAAA",              # missing quantity
    "# ACCT\nAAA 4 extra",      # trailing junk
    "# ACCT\nAAA four",         # non-numeric quantity
])
def test_malformed_lines_fail_loudly(text):
    with pytest.raises(ValueError):
        parse_positions(text)


def test_missing_file_means_no_positions(tmp_path):
    assert load_positions(path=str(tmp_path / "POSITIONS.md")) == []


def _warehouse():
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE silver_signals (symbol VARCHAR, "
                "as_of_date TIMESTAMP)")
    for symbol in ("AAA", "BBB"):
        for date in ("2026-08-10", "2026-08-11"):
            con.execute("INSERT INTO silver_signals VALUES (?, ?)",
                        [symbol, date])
    return con


def test_freshness_gate_splits_tracked_from_untracked():
    tracked, untracked = check_held_freshness(
        _warehouse(), ["AAA", "BBB", "ZZZ"])
    assert tracked == ["AAA", "BBB"]
    assert untracked == ["ZZZ"]  # promotion candidate, not an error


def test_stale_tracked_held_symbol_fails_the_gate():
    con = _warehouse()
    con.execute("DELETE FROM silver_signals WHERE symbol = 'BBB' "
                "AND as_of_date = '2026-08-11'")
    with pytest.raises(ValueError, match="BBB"):
        check_held_freshness(con, ["AAA", "BBB"])


def test_held_table_joins_rank_and_call_untracked_gets_nulls():
    con = _warehouse()
    con.execute("CREATE TABLE gold_watchlist_ranked_v2 (symbol VARCHAR, "
                "composite_rank INTEGER, composite_score DOUBLE)")
    con.execute("INSERT INTO gold_watchlist_ranked_v2 VALUES "
                "('AAA', 3, 0.81), ('BBB', 40, 0.44)")
    con.execute("CREATE TABLE gold_calls (symbol VARCHAR, "
                "as_of_date DATE, call VARCHAR)")
    con.execute("INSERT INTO gold_calls VALUES "
                "('AAA', '2026-07-31', 'buy'), ('AAA', '2026-08-31', 'hold'),"
                "('BBB', '2026-08-31', 'none')")
    build_held_table(con, parse_positions(FIXTURE) + [
        {"account": "ROTH ETF", "symbol": "ZZZ", "quantity": 1.0}])

    df = con.execute("SELECT * FROM gold_held_positions").df()
    aaa = df.loc[df["symbol"] == "AAA"].iloc[0]
    assert aaa["accounts"] == "BROKERAGE STOCK, ROTH ETF"  # merged accounts
    assert aaa["total_quantity"] == 6.5
    assert bool(aaa["tracked"]) is True
    assert aaa["call"] == "hold"          # latest round only
    zzz = df.loc[df["symbol"] == "ZZZ"].iloc[0]
    assert bool(zzz["tracked"]) is False
    assert pd.isna(zzz["composite_rank"]) and pd.isna(zzz["call"])
    # Tracked and ranked rows sort first: the priority ordering.
    assert list(df["symbol"])[:2] == ["AAA", "BBB"]


def test_held_table_works_without_rank_or_call_tables():
    con = _warehouse()
    build_held_table(con, parse_positions(FIXTURE))
    df = con.execute("SELECT * FROM gold_held_positions").df()
    assert len(df) == 3
    assert pd.isna(df["composite_rank"]).all()
