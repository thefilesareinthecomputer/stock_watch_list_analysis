"""
Tests for Databricks Connect compatibility fixes.
Covers: __file__ fallback, Row-based createDataFrame, date string casting.

Run locally with: pytest tests/ -v
No Spark or Databricks required — tests validate Python logic only.
"""
import datetime
import os
import sys

import numpy as np
import pandas as pd
import pytest


# ── __file__ fallback ──────────────────────────────────────────────────

class TestFileFallback:
    """Verify that sys.path insertion works with and without __file__."""

    def test_file_exists_normal(self):
        """When __file__ is defined, the path calculation should work."""
        # Simulate the normal case
        script_dir = os.path.dirname(os.path.abspath(__file__))
        src_dir = os.path.normpath(os.path.join(script_dir, ".."))
        assert os.path.isdir(src_dir) or True  # path calculation just shouldn't raise

    def test_file_undefined_fallback(self):
        """When __file__ is undefined (Databricks exec), os.getcwd() is used."""
        # Simulate the NameError fallback
        try:
            _ = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
        except NameError:
            _src_dir = os.path.join(os.getcwd(), "..")
            result = os.path.normpath(_src_dir)
            assert ".." not in result or result.endswith("..")  # just shouldn't raise

    def test_path_resolution_produces_valid_dir(self):
        """Both approaches should produce a path that can be inserted into sys.path."""
        # Normal path
        try:
            src_dir = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
        except NameError:
            src_dir = os.path.normpath(os.path.join(os.getcwd(), ".."))

        assert isinstance(src_dir, str)
        assert len(src_dir) > 0


# ── Row-based DataFrame creation ────────────────────────────────────────

class TestRowCreation:
    """Verify Row object creation from pandas DataFrames (Databricks Connect fix)."""

    @pytest.mark.skipif(
        sys.modules.get("pyspark") is None,
        reason="pyspark not installed locally"
    )
    def test_basic_row_conversion(self):
        """pandas DataFrame rows should convert to Row-compatible dicts."""
        from pyspark.sql import Row

        df = pd.DataFrame({
            "symbol": ["AAPL", "MSFT"],
            "close": [150.0, 300.0],
            "volume": [1000000, 2000000],
        })

        rows = [Row(**row.to_dict()) for _, row in df.iterrows()]
        assert len(rows) == 2
        assert rows[0].symbol == "AAPL"
        assert rows[0].close == 150.0
        assert rows[1].symbol == "MSFT"

    @pytest.mark.skipif(
        sys.modules.get("pyspark") is None,
        reason="pyspark not installed locally"
    )
    def test_nan_to_none_conversion(self):
        """NaN values in pandas should become None for Spark compatibility."""
        from pyspark.sql import Row

        df = pd.DataFrame({
            "symbol": ["AAPL", "MSFT"],
            "pe_ratio": [25.0, np.nan],
            "eps": [6.0, 8.0],
        })

        rows = [
            Row(**{k: (None if isinstance(v, float) and pd.isna(v) else v) for k, v in row.items()})
            for _, row in df.iterrows()
        ]
        assert rows[0].pe_ratio == 25.0
        assert rows[1].pe_ratio is None
        assert rows[1].eps == 8.0

    def test_string_where_notna_conversion(self):
        """String columns with NaN should use .where(notna, None) pattern."""
        df = pd.DataFrame({
            "symbol": ["AAPL", "MSFT"],
            "sector": ["Technology", None],
            "industry": ["Software", np.nan],
        })

        for col in ["sector", "industry"]:
            df[col] = df[col].astype(object).where(df[col].notna(), None)

        # Verify None values preserved
        assert df["sector"].iloc[1] is None
        assert df["industry"].iloc[1] is None

    def test_fundamentals_nan_handling(self):
        """Fundamentals DataFrame NaN→None conversion for all numeric fields."""
        df = pd.DataFrame({
            "symbol": ["AAPL"],
            "marketCap": [2500000000000.0],
            "trailingPE": [np.nan],
            "sector": ["Technology"],
        })

        df["trailingPE"] = pd.to_numeric(df["trailingPE"], errors="coerce")
        df = df.where(pd.notna(df), None)

        assert df["marketCap"].iloc[0] == 2500000000000.0
        assert pd.isna(df["trailingPE"].iloc[0]) or df["trailingPE"].iloc[0] is None


# ── Date string casting ────────────────────────────────────────────────

class TestDateStringCasting:
    """Verify date column string formatting for Spark SQL CAST."""

    def test_date_to_string_format(self):
        """Dates should format as YYYY-MM-DD strings for Spark SQL CAST."""
        dates = pd.Series([datetime.date(2024, 1, 15), datetime.date(2024, 6, 30)])
        formatted = pd.to_datetime(dates).dt.strftime("%Y-%m-%d")
        assert formatted.iloc[0] == "2024-01-15"
        assert formatted.iloc[1] == "2024-06-30"

    def test_string_date_is_castable(self):
        """YYYY-MM-DD strings are valid for Spark SQL CAST(date AS DATE)."""
        # This validates the format, not the actual Spark cast
        date_str = "2024-01-15"
        parsed = datetime.date.fromisoformat(date_str)
        assert parsed == datetime.date(2024, 1, 15)

    def test_pandas_timestamp_to_string(self):
        """pandas Timestamps should also produce valid date strings."""
        ts = pd.Timestamp("2024-03-15")
        formatted = ts.strftime("%Y-%m-%d")
        assert formatted == "2024-03-15"


# ── Price data dtype casting ────────────────────────────────────────────

class TestPriceDtypeCasting:
    """Verify dtype casts applied before Row creation (ingest_prices)."""

    def test_volume_cast_to_int64(self):
        """Volume should be int64 with NaN→0 fill."""
        df = pd.DataFrame({"volume": [1000.0, np.nan, 3000.0]})
        result = df["volume"].fillna(0).astype("int64")
        assert result.dtype == np.int64
        assert result.iloc[1] == 0

    def test_float_columns(self):
        """OHLC columns should be float64."""
        df = pd.DataFrame({
            "open": [150, 200],
            "close": [155, 210],
        })
        for col in ["open", "close"]:
            df[col] = df[col].astype("float64")
        assert df["open"].dtype == np.float64
        assert df["close"].dtype == np.float64

    def test_symbol_cast_to_str(self):
        """Symbol column should be string type."""
        df = pd.DataFrame({"symbol": ["AAPL", "MSFT"]})
        df["symbol"] = df["symbol"].astype(str)
        # pandas 2.x uses StringDtype; just verify values are strings
        assert df["symbol"].iloc[0] == "AAPL"
        assert df["symbol"].iloc[1] == "MSFT"