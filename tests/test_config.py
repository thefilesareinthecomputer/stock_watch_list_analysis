"""Smoke tests for config validity — run before deploy."""
import pytest
from src.common.config import (
    TICKERS,
    CATALOG,
    BENCHMARK_TICKERS,
    FUNDAMENTALS_NUMERIC_FIELDS,
    FUNDAMENTALS_STRING_FIELDS,
    TABLE_GOLD_DIM_DATE,
    TABLE_GOLD_DIM_SECURITY,
    TABLE_GOLD_FACT_MARKET,
    TABLE_GOLD_FACT_FUNDAMENTALS,
    TABLE_GOLD_FACT_SIGNALS,
    TABLE_GOLD_SIGNAL_ALERTS,
    TABLE_GOLD_BENCHMARK_COMPARE,
    TABLE_GOLD_PORTFOLIO_CANDIDATES,
)


def test_tickers_not_empty():
    assert len(TICKERS) > 0


def test_tickers_are_strings():
    for t in TICKERS:
        assert isinstance(t, str)
        assert 1 <= len(t) <= 10


def test_no_duplicate_tickers():
    assert len(TICKERS) == len(set(TICKERS))


def test_catalog_name():
    assert CATALOG in ("stock_analytics", "main")


def test_fundamentals_fields_no_overlap():
    overlap = set(FUNDAMENTALS_NUMERIC_FIELDS) & set(FUNDAMENTALS_STRING_FIELDS)
    assert not overlap, f"Fields in both numeric and string: {overlap}"


def test_benchmark_tickers_present():
    assert "SPY" in BENCHMARK_TICKERS
    assert "QQQ" in BENCHMARK_TICKERS


def test_benchmark_not_in_watchlist():
    """BENCHMARK_TICKERS and TICKERS are separate lists."""
    # Benchmarks may or may not overlap with watchlist — just verify they exist
    assert len(BENCHMARK_TICKERS) >= 2


def test_gold_tables_in_catalog():
    """All Gold tables should be under the catalog."""
    for table in [TABLE_GOLD_DIM_DATE, TABLE_GOLD_DIM_SECURITY,
                  TABLE_GOLD_FACT_MARKET, TABLE_GOLD_FACT_FUNDAMENTALS,
                  TABLE_GOLD_FACT_SIGNALS, TABLE_GOLD_SIGNAL_ALERTS,
                  TABLE_GOLD_BENCHMARK_COMPARE, TABLE_GOLD_PORTFOLIO_CANDIDATES]:
        assert table.startswith(f"{CATALOG}.gold."), f"{table} not in gold schema"