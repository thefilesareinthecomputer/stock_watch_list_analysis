"""Warehouse existence checks shared by every script that reads optional
tables (fixtures build partial warehouses; real builds may skip a tier)."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import duckdb

from common.tables import has_column, has_table


def test_has_table_and_has_column():
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE bronze_prices (symbol VARCHAR, close DOUBLE)")
    assert has_table(con, "bronze_prices") is True
    assert has_table(con, "gold_calls") is False
    assert has_column(con, "bronze_prices", "close") is True
    assert has_column(con, "bronze_prices", "split_ratio") is False
    assert has_column(con, "missing_table", "close") is False
