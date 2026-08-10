"""Direction and null tests for score components.

These exist because all four components shipped inverted. A component whose
direction is untested silently reverses the product: the pipeline reported its
worst-ranked names as its best, and every test passed.

Each component gets two tests - the intended-best input must earn the highest
percentile, and a missing input must not earn the best one.
"""
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import pytest

from scoring.components import COMPONENTS, percentile_sql

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    s = (SparkSession.builder
         .master("local[1]")
         .appName("test_scoring")
         .getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


def _score(spark, rows):
    """Rank `rows` through the real component SQL and return {symbol: {...}}."""
    cols = ["symbol", "change_30d_pct", "pe_ratio", "rsi", "mfi"]
    spark.createDataFrame(rows, cols).createOrReplaceTempView("signals")
    out = spark.sql(f"SELECT symbol, {percentile_sql()} FROM signals").collect()
    return {r["symbol"]: r.asDict() for r in out}


# ── Direction: the intended-best input must score highest ──────────

def test_momentum_direction_higher_return_is_better(spark):
    r = _score(spark, [("best", 30.0, 20.0, 50.0, 50.0),
                       ("mid", 10.0, 20.0, 50.0, 50.0),
                       ("worst", -5.0, 20.0, 50.0, 50.0)])
    assert r["best"]["momentum_pct"] == 1.0
    assert r["worst"]["momentum_pct"] == 0.0


def test_value_direction_cheaper_is_better(spark):
    r = _score(spark, [("cheap", 0.0, 5.0, 50.0, 50.0),
                       ("mid", 0.0, 20.0, 50.0, 50.0),
                       ("rich", 0.0, 50.0, 50.0, 50.0)])
    assert r["cheap"]["value_pct"] == 1.0
    assert r["rich"]["value_pct"] == 0.0


def test_risk_direction_less_overbought_is_better(spark):
    r = _score(spark, [("oversold", 0.0, 20.0, 20.0, 50.0),
                       ("neutral", 0.0, 20.0, 50.0, 50.0),
                       ("overbought", 0.0, 20.0, 80.0, 50.0)])
    assert r["oversold"]["risk_pct"] == 1.0
    assert r["overbought"]["risk_pct"] == 0.0


def test_quality_direction_stronger_flow_is_better(spark):
    r = _score(spark, [("strong", 0.0, 20.0, 50.0, 80.0),
                       ("mid", 0.0, 20.0, 50.0, 50.0),
                       ("weak", 0.0, 20.0, 50.0, 20.0)])
    assert r["strong"]["quality_pct"] == 1.0
    assert r["weak"]["quality_pct"] == 0.0


# ── Nulls and degenerate inputs must never score best ──────────────

def test_missing_pe_does_not_rank_as_best_value(spark):
    """The original defect: COALESCE(pe, 999) ASC gave a missing P/E the top
    value score, so any symbol lacking fundamentals ranked as the cheapest."""
    r = _score(spark, [("cheap", 0.0, 5.0, 50.0, 50.0),
                       ("rich", 0.0, 50.0, 50.0, 50.0),
                       ("nope", 0.0, None, 50.0, 50.0)])
    assert r["cheap"]["value_pct"] == 1.0
    assert r["nope"]["value_pct"] < r["rich"]["value_pct"]


def test_negative_pe_does_not_rank_as_best_value(spark):
    """A loss-making company has a negative P/E, which is numerically the
    smallest value and would otherwise rank as the cheapest name."""
    r = _score(spark, [("cheap", 0.0, 5.0, 50.0, 50.0),
                       ("rich", 0.0, 50.0, 50.0, 50.0),
                       ("lossmaker", 0.0, -8.0, 50.0, 50.0)])
    assert r["cheap"]["value_pct"] == 1.0
    assert r["lossmaker"]["value_pct"] < r["rich"]["value_pct"]


@pytest.mark.parametrize("column,component", [
    ("rsi", "risk_pct"),
    ("mfi", "quality_pct"),
])
def test_missing_oscillator_lands_between_extremes(spark, column, component):
    """Missing RSI/MFI fall back to 50, which must rank mid-pack rather than
    winning or losing outright."""
    idx = {"rsi": 3, "mfi": 4}[column]
    def row(sym, val):
        base = [sym, 0.0, 20.0, 50.0, 50.0]
        base[idx] = val
        return tuple(base)
    r = _score(spark, [row("low", 10.0), row("high", 90.0), row("missing", None)])
    assert 0.0 < r["missing"][component] < 1.0


# ── Structural ─────────────────────────────────────────────────────

def test_percentiles_stay_in_unit_range(spark):
    r = _score(spark, [("a", 30.0, 5.0, 20.0, 80.0),
                       ("b", 10.0, 20.0, 50.0, 50.0),
                       ("c", -5.0, None, None, None)])
    for sym in r.values():
        for name in COMPONENTS:
            assert 0.0 <= sym[name] <= 1.0


def test_partition_ranks_within_date(spark):
    """daily_analytics ranks per as_of_date; ranking must not leak across dates."""
    spark.createDataFrame(
        [("A", "2026-01-01", 30.0), ("B", "2026-01-01", 10.0),
         ("A", "2026-01-02", 10.0), ("B", "2026-01-02", 30.0)],
        ["symbol", "as_of_date", "change_30d_pct"],
    ).createOrReplaceTempView("ts")
    sql = ("PERCENT_RANK() OVER (PARTITION BY as_of_date "
           "ORDER BY COALESCE(change_30d_pct, 0) ASC) AS momentum_pct")
    assert sql in percentile_sql(partition_by="as_of_date")
    out = {(r["symbol"], r["as_of_date"]): r["momentum_pct"]
           for r in spark.sql(f"SELECT symbol, as_of_date, {sql} FROM ts").collect()}
    assert out[("A", "2026-01-01")] == 1.0
    assert out[("B", "2026-01-02")] == 1.0
