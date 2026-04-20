"""Tests for quality module — data validation functions."""
import numpy as np
import pandas as pd
import pytest
from src.common.quality import (
    check_not_empty,
    check_expected_columns,
    check_null_percentage,
    check_duplicate_keys,
    check_price_consistency,
    check_date_monotonic,
    check_source_event_ts_before_ingest_ts,
    check_bronze_prices,
    check_silver_prices,
)


@pytest.fixture
def valid_bronze_df():
    """Valid Bronze prices DataFrame."""
    return pd.DataFrame({
        "symbol": ["AAPL"] * 5,
        "date": pd.date_range("2024-01-01", periods=5),
        "open": [100.0, 101.0, 102.0, 103.0, 104.0],
        "high": [101.0, 102.0, 103.0, 104.0, 105.0],
        "low": [99.0, 100.0, 101.0, 102.0, 103.0],
        "close": [100.5, 101.5, 102.5, 103.5, 104.5],
        "volume": [1000, 1100, 1200, 1300, 1400],
        "_source_event_ts": ["2024-01-01T00:00:00Z"] * 5,
        "_ingest_ts": ["2024-01-02T00:00:00Z"] * 5,
    })


class TestCheckNotEmpty:
    def test_empty(self):
        df = pd.DataFrame()
        assert len(check_not_empty(df, "test")) == 1

    def test_not_empty(self):
        df = pd.DataFrame({"a": [1]})
        assert len(check_not_empty(df, "test")) == 0


class TestCheckExpectedColumns:
    def test_missing_cols(self):
        df = pd.DataFrame({"a": [1]})
        assert len(check_expected_columns(df, {"a", "b"}, "test")) == 1

    def test_all_present(self):
        df = pd.DataFrame({"a": [1], "b": [2]})
        assert len(check_expected_columns(df, {"a", "b"}, "test")) == 0


class TestCheckNullPercentage:
    def test_high_nulls(self):
        df = pd.DataFrame({"a": [None, None, None, 1.0]})
        assert len(check_null_percentage(df, ["a"], "test", 0.5)) == 1

    def test_low_nulls(self):
        df = pd.DataFrame({"a": [1.0, 2.0, 3.0, None]})
        assert len(check_null_percentage(df, ["a"], "test", 0.5)) == 0


class TestCheckDuplicateKeys:
    def test_duplicates(self):
        df = pd.DataFrame({"a": [1, 1], "b": [2, 2]})
        assert len(check_duplicate_keys(df, ["a", "b"], "test")) == 1

    def test_no_duplicates(self):
        df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
        assert len(check_duplicate_keys(df, ["a", "b"], "test")) == 0


class TestCheckPriceConsistency:
    def test_high_below_low(self):
        df = pd.DataFrame({"high": [99], "low": [100], "close": [100]})
        assert len(check_price_consistency(df, "test")) >= 1

    def test_negative_volume(self):
        df = pd.DataFrame({"high": [100], "low": [99], "close": [99.5], "volume": [-1]})
        assert len(check_price_consistency(df, "test")) >= 1

    def test_valid_prices(self):
        df = pd.DataFrame({"high": [100], "low": [99], "close": [99.5], "volume": [1000]})
        assert len(check_price_consistency(df, "test")) == 0


class TestCheckSourceEventTs:
    def test_future_event(self):
        df = pd.DataFrame({
            "_source_event_ts": ["2024-01-03T00:00:00Z"],
            "_ingest_ts": ["2024-01-02T00:00:00Z"],
        })
        assert len(check_source_event_ts_before_ingest_ts(df, "test")) == 1

    def test_valid_order(self, valid_bronze_df):
        assert len(check_source_event_ts_before_ingest_ts(valid_bronze_df, "test")) == 0


class TestCheckBronzePrices:
    def test_valid(self, valid_bronze_df):
        issues = check_bronze_prices(valid_bronze_df)
        assert len(issues) == 0

    def test_empty(self):
        issues = check_bronze_prices(pd.DataFrame())
        assert len(issues) > 0


class TestCheckSilverPrices:
    def test_duplicates_flagged(self):
        df = pd.DataFrame({
            "symbol": ["AAPL", "AAPL"],
            "date": ["2024-01-01", "2024-01-01"],
            "close": [100.0, 100.0],
            "high": [101.0, 101.0],
            "low": [99.0, 99.0],
            "volume": [1000, 1000],
        })
        issues = check_silver_prices(df)
        assert any("duplicate" in i.lower() for i in issues)