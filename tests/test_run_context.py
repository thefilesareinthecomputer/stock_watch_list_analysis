"""Tests for run_context module — PIT metadata generation."""
import pytest
from src.common.run_context import (
    new_run_id, now_ts, source_event_ts,
    SOURCE_SYSTEM, LOAD_TYPE, META_COLUMNS,
)


class TestRunId:
    def test_uuid_format(self):
        rid = new_run_id()
        assert len(rid) == 36  # UUID with dashes
        assert rid.count("-") == 4

    def test_unique(self):
        ids = {new_run_id() for _ in range(10)}
        assert len(ids) == 10


class TestNowTs:
    def test_iso_format(self):
        ts = now_ts()
        assert ts.endswith("Z")
        assert "T" in ts

    def test_returns_string(self):
        assert isinstance(now_ts(), str)


class TestSourceEventTs:
    def test_from_string(self):
        result = source_event_ts("2024-01-15")
        assert result == "2024-01-15T00:00:00Z"

    def test_from_date(self):
        from datetime import date
        result = source_event_ts(date(2024, 1, 15))
        assert result == "2024-01-15T00:00:00Z"


class TestConstants:
    def test_source_system(self):
        assert SOURCE_SYSTEM == "yfinance"

    def test_load_type(self):
        assert LOAD_TYPE == "full"

    def test_meta_columns(self):
        assert "_run_id" in META_COLUMNS
        assert "_ingest_ts" in META_COLUMNS
        assert "_source_event_ts" in META_COLUMNS
        assert len(META_COLUMNS) == 5