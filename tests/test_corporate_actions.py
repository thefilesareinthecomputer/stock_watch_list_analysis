"""corporate_actions: splits kept as permanent events, like trades.

Yahoo restates every past close after a split, which once produced a
double-applied split (plan.md gotcha 0j). The split log is the fixed point
that every traded-price conversion and adjustment is derived from, so it
must be append-only: recorded once on first sight, never rewritten.
"""
import inspect
import os
import re
import sys
from datetime import date

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import duckdb
import pytest

from common import corporate_actions as ca
from common.corporate_actions import (
    ensure_table, record_actions, splits_for,
)

SPLIT = {"symbol": "xyz", "date": "2024-06-10", "ratio": 10.0}


@pytest.fixture
def con():
    c = duckdb.connect()
    ensure_table(c)
    return c


def _rows(con):
    return con.execute(
        "SELECT symbol, date, ratio, type, source, first_seen "
        "FROM corporate_actions ORDER BY symbol, date").fetchall()


def test_table_has_the_six_columns(con):
    cols = [r[0] for r in con.execute("DESCRIBE corporate_actions").fetchall()]
    assert cols == ["symbol", "date", "ratio", "type", "source", "first_seen"]


def test_ensure_table_is_idempotent(con):
    record_actions(con, [SPLIT], source="yfinance")
    ensure_table(con)
    assert len(_rows(con)) == 1


def test_new_event_inserts_one_row_with_first_seen(con):
    inserted, conflicts = record_actions(con, [SPLIT], source="yfinance")
    assert inserted == 1 and conflicts == []
    (row,) = _rows(con)
    symbol, day, ratio, kind, source, first_seen = row
    assert (symbol, day, ratio, kind, source) == (
        "XYZ", date(2024, 6, 10), 10.0, "split", "yfinance")
    assert re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z", first_seen)


def test_same_event_again_changes_no_row(con):
    record_actions(con, [SPLIT], source="yfinance")
    before = _rows(con)
    inserted, conflicts = record_actions(
        con, [SPLIT], source="another-source", first_seen="2030-01-01T00:00:00Z")
    assert inserted == 0 and conflicts == []
    assert _rows(con) == before          # ratio, source and first_seen all kept


def test_differing_ratio_for_a_known_event_is_a_conflict_not_a_write(con):
    record_actions(con, [SPLIT], source="yfinance")
    before = _rows(con)
    inserted, conflicts = record_actions(
        con, [{**SPLIT, "ratio": 4.0}], source="yfinance")
    assert inserted == 0
    assert conflicts == [("XYZ", date(2024, 6, 10), "split", 10.0, 4.0)]
    assert _rows(con) == before


def test_batch_mixes_new_known_and_conflicting(con):
    record_actions(con, [SPLIT], source="yfinance")
    batch = [
        SPLIT,                                              # known
        {**SPLIT, "ratio": 2.0},                            # conflict
        {"symbol": "abc", "date": "2020-08-31", "ratio": 4.0},  # new
    ]
    inserted, conflicts = record_actions(con, batch, source="yfinance")
    assert inserted == 1 and len(conflicts) == 1
    assert [r[0] for r in _rows(con)] == ["ABC", "XYZ"]


def test_timestamp_dates_land_as_dates(con):
    import pandas as pd
    record_actions(con, [{**SPLIT, "date": pd.Timestamp("2024-06-10")}],
                   source="yfinance")
    assert splits_for(con, "xyz") == [(date(2024, 6, 10), 10.0)]


def test_reverse_split_is_a_split_with_a_ratio_below_one(con):
    inserted, _ = record_actions(
        con, [{**SPLIT, "ratio": 0.1}], source="yfinance")   # 1-for-10
    assert inserted == 1
    assert splits_for(con, "xyz") == [(date(2024, 6, 10), 0.1)]


@pytest.mark.parametrize("bad, match", [
    ({"ratio": 0}, "ratio"),
    ({"ratio": -2}, "ratio"),
    ({"ratio": 1.0}, "ratio"),          # a 1:1 split is not an event
    ({"ratio": None}, "ratio"),
    ({"date": "06/10/2024"}, "date"),
    ({"symbol": ""}, "symbol"),
    ({"type": "dividend"}, "type"),     # dividends stay in bronze_prices
])
def test_defective_event_is_rejected_before_any_write(con, bad, match):
    with pytest.raises(ValueError, match=match):
        record_actions(con, [{**SPLIT, **bad}], source="yfinance")
    assert _rows(con) == []


def test_a_bad_event_in_a_batch_writes_nothing(con):
    with pytest.raises(ValueError):
        record_actions(con, [SPLIT, {**SPLIT, "symbol": "abc", "ratio": 0}],
                       source="yfinance")
    assert _rows(con) == []


def test_splits_for_is_oldest_first_and_per_symbol(con):
    record_actions(con, [
        {"symbol": "xyz", "date": "2024-06-10", "ratio": 10.0},
        {"symbol": "xyz", "date": "2021-07-20", "ratio": 4.0},
        {"symbol": "abc", "date": "2020-08-31", "ratio": 4.0},
    ], source="yfinance")
    assert splits_for(con, "xyz") == [
        (date(2021, 7, 20), 4.0), (date(2024, 6, 10), 10.0)]
    assert splits_for(con, "ABC") == [(date(2020, 8, 31), 4.0)]
    assert splits_for(con, "none") == []


def test_writer_exposes_no_update_or_delete_path():
    public = [n for n in dir(ca) if not n.startswith("_")]
    assert not [n for n in public
                if re.search(r"update|delete|remove|replace|drop", n, re.I)]
    src = inspect.getsource(ca)
    assert not re.search(r"\b(UPDATE|DELETE|DROP|TRUNCATE|REPLACE)\b", src)
