"""Immutability tests for the recommendation snapshot.

The guarantee under test: once a (as_of_date, methodology_version) is recorded,
no later run of the same methodology can change it. Everything downstream -
backtesting, judging past calls, the paper track - rests on this holding, and
nothing else in the warehouse provides it, since every other gold table is
rebuilt with CREATE OR REPLACE on each run.
"""
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import pytest

from scoring.snapshot import SNAPSHOT_COLUMNS, create_sql, insert_sql

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession  # noqa: E402

TABLE = "recs"
SOURCE = "ranked"


@pytest.fixture(scope="module")
def spark():
    s = (SparkSession.builder
         .master("local[1]")
         .appName("test_snapshot")
         .getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


@pytest.fixture(autouse=True)
def clean(spark):
    spark.sql(f"DROP TABLE IF EXISTS {TABLE}")
    yield
    spark.sql(f"DROP TABLE IF EXISTS {TABLE}")


def _source(spark, rows):
    """Stand in for gold.watchlist_ranked."""
    cols = ["symbol", "as_of_date", "composite_score", "composite_rank",
            "momentum_pct", "value_pct", "risk_pct", "quality_pct",
            "last_closing_price", "trade_signal"]
    spark.createDataFrame(rows, cols).createOrReplaceTempView(SOURCE)


def _row(sym, date, score, rank, price=100.0, signal="Buy"):
    return (sym, date, score, rank, 0.5, 0.5, 0.5, 0.5, price, signal)


def _snapshot(spark, run_id, version="v1"):
    spark.sql(create_sql(TABLE, SOURCE, run_id, version))
    spark.sql(insert_sql(TABLE, SOURCE, run_id, version))


def _rows(spark):
    return {(r["symbol"], r["as_of_date"]): r.asDict()
            for r in spark.sql(f"SELECT * FROM {TABLE}").collect()}


def test_first_run_records_every_symbol(spark):
    _source(spark, [_row("AAPL", "2026-03-01", 0.9, 1),
                    _row("MSFT", "2026-03-01", 0.8, 2)])
    _snapshot(spark, "run-1")
    assert len(_rows(spark)) == 2


def test_rerunning_the_same_day_changes_nothing(spark):
    """The core guarantee. A second run with different scores must not restate
    what was already recorded - that is what makes the record evidence."""
    _source(spark, [_row("AAPL", "2026-03-01", 0.9, 1)])
    _snapshot(spark, "run-1")
    before = _rows(spark)

    _source(spark, [_row("AAPL", "2026-03-01", 0.1, 99)])
    _snapshot(spark, "run-2")
    after = _rows(spark)

    assert len(after) == 1, "re-run duplicated rows"
    assert after[("AAPL", "2026-03-01")]["composite_score"] == 0.9
    assert after[("AAPL", "2026-03-01")]["_run_id"] == "run-1"
    assert before == after


def test_a_new_day_appends_alongside_history(spark):
    _source(spark, [_row("AAPL", "2026-03-01", 0.9, 1)])
    _snapshot(spark, "run-1")
    _source(spark, [_row("AAPL", "2026-03-02", 0.4, 7)])
    _snapshot(spark, "run-2")

    rows = _rows(spark)
    assert len(rows) == 2
    assert rows[("AAPL", "2026-03-01")]["composite_score"] == 0.9
    assert rows[("AAPL", "2026-03-02")]["composite_score"] == 0.4


def test_methodology_bump_writes_alongside_not_over(spark):
    """Changing the score must not silently restate past recommendations - the
    old version stays queryable next to the new one."""
    _source(spark, [_row("AAPL", "2026-03-01", 0.9, 1)])
    _snapshot(spark, "run-1", version="v1")
    _source(spark, [_row("AAPL", "2026-03-01", 0.2, 40)])
    _snapshot(spark, "run-2", version="v2")

    out = {r["methodology_version"]: r["composite_score"]
           for r in spark.sql(f"SELECT * FROM {TABLE}").collect()}
    assert out == {"v1": 0.9, "v2": 0.2}


def test_snapshot_carries_provenance(spark):
    _source(spark, [_row("AAPL", "2026-03-01", 0.9, 1)])
    _snapshot(spark, "run-abc")
    row = _rows(spark)[("AAPL", "2026-03-01")]
    for field in SNAPSHOT_COLUMNS:
        assert field in row
    assert row["_run_id"] == "run-abc"
    assert row["methodology_version"] == "v1"
    assert row["_snapshot_ts"] is not None


def test_partial_day_is_still_one_shot(spark):
    """If a run recorded only some symbols, a later run must not top it up -
    a half-recorded day is a data-quality incident, not something to silently
    repair, because the added rows would carry the wrong day's evidence."""
    _source(spark, [_row("AAPL", "2026-03-01", 0.9, 1)])
    _snapshot(spark, "run-1")
    _source(spark, [_row("AAPL", "2026-03-01", 0.9, 1),
                    _row("MSFT", "2026-03-01", 0.8, 2)])
    _snapshot(spark, "run-2")
    assert len(_rows(spark)) == 1
