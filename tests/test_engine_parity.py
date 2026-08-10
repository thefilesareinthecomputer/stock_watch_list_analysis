"""Spark and DuckDB must produce identical results from identical SQL.

The point of a local DuckDB warehouse is to iterate on recommendation quality in
seconds instead of the ~18 minutes a Databricks run costs. That is only worth
anything if what you measure locally is what production will produce.

So the transforms are written once, as SQL, and executed by both engines. This
test is the mechanism that keeps that honest: same string, same fixture, same
answer. If it goes red, local results have stopped predicting Databricks and the
iteration loop is lying to you.

Where the dialects genuinely differ, the fix is to move to the common subset
rather than to branch - a branch is two implementations again, which is the
drift this whole arrangement exists to prevent.
"""
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import pytest

from scoring.components import COMPONENTS, percentile_sql
from scoring.snapshot import create_sql, insert_sql

pyspark = pytest.importorskip("pyspark")
duckdb = pytest.importorskip("duckdb")
from pyspark.sql import SparkSession  # noqa: E402

COLUMNS = ["symbol", "change_30d_pct", "pe_ratio", "rsi", "mfi"]

# Deliberately awkward: nulls in every scored column, a negative P/E, ties, and
# a symbol with nothing but nulls. Engines agree on the easy cases.
FIXTURE = [
    ("AAA", 30.0, 5.0, 20.0, 80.0),
    ("BBB", 10.0, 20.0, 50.0, 50.0),
    ("CCC", -5.0, 50.0, 80.0, 20.0),
    ("DDD", 10.0, -8.0, 50.0, 50.0),      # tie on momentum, negative P/E
    ("EEE", None, None, None, None),      # nothing at all
    ("FFF", 0.0, 999.0, 50.0, 50.0),      # collides with the P/E sentinel
]


@pytest.fixture(scope="module")
def spark():
    s = (SparkSession.builder
         .master("local[1]")
         .appName("test_parity")
         .getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


def _spark_result(spark, sql):
    spark.createDataFrame(FIXTURE, COLUMNS).createOrReplaceTempView("signals")
    rows = spark.sql(f"SELECT symbol, {sql} FROM signals").collect()
    return {r["symbol"]: r.asDict() for r in rows}


def _duckdb_result(sql):
    con = duckdb.connect()
    con.execute(
        "CREATE TABLE signals (symbol VARCHAR, change_30d_pct DOUBLE, "
        "pe_ratio DOUBLE, rsi DOUBLE, mfi DOUBLE)"
    )
    con.executemany("INSERT INTO signals VALUES (?, ?, ?, ?, ?)", FIXTURE)
    cur = con.execute(f"SELECT symbol, {sql} FROM signals")
    names = [d[0] for d in cur.description]
    return {r[0]: dict(zip(names, r)) for r in cur.fetchall()}


def test_component_percentiles_agree_across_engines(spark):
    sql = percentile_sql()
    spark_out = _spark_result(spark, sql)
    duck_out = _duckdb_result(sql)

    assert set(spark_out) == set(duck_out)
    mismatches = []
    for symbol in sorted(spark_out):
        for component in COMPONENTS:
            a = spark_out[symbol][component]
            b = duck_out[symbol][component]
            if abs(a - b) > 1e-9:
                mismatches.append(f"{symbol}.{component}: spark={a} duckdb={b}")
    assert not mismatches, "engines disagree:\n" + "\n".join(mismatches)


def test_ranking_order_agrees_across_engines(spark):
    """Absolute percentiles matching matters less than the induced ordering,
    which is what actually selects recommendations."""
    sql = percentile_sql()
    spark_out = _spark_result(spark, sql)
    duck_out = _duckdb_result(sql)

    for component in COMPONENTS:
        spark_order = sorted(spark_out, key=lambda s: (spark_out[s][component], s))
        duck_order = sorted(duck_out, key=lambda s: (duck_out[s][component], s))
        assert spark_order == duck_order, f"{component} ordering differs"


def test_snapshot_sql_runs_on_duckdb_with_the_same_semantics():
    """The snapshot is what a local backtest reads, so it has to build locally.

    This caught a real one: CURRENT_TIMESTAMP() is a function in Spark but a
    bare keyword in DuckDB, so the parenthesised form failed outright. The bare
    form works in both, which is why the SQL uses it.
    """
    con = duckdb.connect()
    con.execute(
        "CREATE TABLE ranked (symbol VARCHAR, as_of_date VARCHAR, "
        "composite_score DOUBLE, composite_rank INTEGER, momentum_pct DOUBLE, "
        "value_pct DOUBLE, risk_pct DOUBLE, quality_pct DOUBLE, "
        "last_closing_price DOUBLE, trade_signal VARCHAR)"
    )
    con.execute("INSERT INTO ranked VALUES "
                "('AAPL','2026-03-01',0.9,1,0.5,0.5,0.5,0.5,100.0,'Buy')")

    con.execute(create_sql("recs", "ranked", "run-1"))
    con.execute(insert_sql("recs", "ranked", "run-1"))

    # First write wins here exactly as it does on Spark.
    con.execute("UPDATE ranked SET composite_score = 0.1")
    con.execute(insert_sql("recs", "ranked", "run-2"))

    rows = con.execute("SELECT composite_score, _run_id FROM recs").fetchall()
    assert rows == [(0.9, "run-1")]


def test_partitioned_percentiles_agree_across_engines(spark):
    """daily_analytics ranks within as_of_date; the windowed form must port too."""
    rows = [("AAA", "2026-01-01", 30.0), ("BBB", "2026-01-01", 10.0),
            ("AAA", "2026-01-02", 10.0), ("BBB", "2026-01-02", 30.0)]
    sql = ("PERCENT_RANK() OVER (PARTITION BY as_of_date "
           "ORDER BY COALESCE(change_30d_pct, 0) ASC) AS momentum_pct")
    assert sql in percentile_sql(partition_by="as_of_date")

    spark.createDataFrame(rows, ["symbol", "as_of_date", "change_30d_pct"]) \
         .createOrReplaceTempView("ts")
    spark_out = {(r["symbol"], r["as_of_date"]): r["momentum_pct"]
                 for r in spark.sql(f"SELECT symbol, as_of_date, {sql} FROM ts").collect()}

    con = duckdb.connect()
    con.execute("CREATE TABLE ts (symbol VARCHAR, as_of_date VARCHAR, change_30d_pct DOUBLE)")
    con.executemany("INSERT INTO ts VALUES (?, ?, ?)", rows)
    duck_out = {(r[0], r[1]): r[2]
                for r in con.execute(f"SELECT symbol, as_of_date, {sql} FROM ts").fetchall()}

    assert spark_out == duck_out
