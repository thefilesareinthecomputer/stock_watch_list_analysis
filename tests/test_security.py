"""Quote types: fetched once, stored, never re-fetched."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import duckdb

from common.security import ensure_quote_types


def test_fetches_only_missing_symbols_and_stores_types():
    con = duckdb.connect(":memory:")
    calls = []

    def fetch(symbol):
        calls.append(symbol)
        return {"AZN": "EQUITY", "VOO": "ETF"}.get(symbol, "UNKNOWN")

    assert ensure_quote_types(con, ["AZN", "VOO"], fetch=fetch) == 2
    assert calls == ["AZN", "VOO"]

    # Idempotent: nothing re-fetched, a new symbol is picked up alone.
    assert ensure_quote_types(con, ["AZN", "VOO", "XXX"], fetch=fetch) == 1
    assert calls == ["AZN", "VOO", "XXX"]

    rows = dict(con.execute(
        "SELECT symbol, quote_type FROM bronze_security").fetchall())
    assert rows == {"AZN": "EQUITY", "VOO": "ETF", "XXX": "UNKNOWN"}
