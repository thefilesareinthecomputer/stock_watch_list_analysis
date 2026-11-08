"""Facts about the warehouse itself that cannot be inferred from its rows.

One key, one value. The first use is `bronze_terms`: 'traded' once
bronze_prices holds the closes that actually traded (EPIC-000000). Readers
that must not run against a restated history check this instead of
guessing from the data, which is the inference this project is retiring.
"""
from common.run_context import now_ts
from common.tables import has_table

TABLE = "warehouse_meta"
BRONZE_TERMS = "bronze_terms"
TRADED = "traded"

SCHEMA = f"""
    CREATE TABLE IF NOT EXISTS {TABLE} (
        key VARCHAR PRIMARY KEY,
        value VARCHAR NOT NULL,
        set_at VARCHAR NOT NULL
    )
"""


def get_meta(con, key):
    """The stored value, or None when the key (or the table) is absent.
    Reads only, so it works on a read-only connection."""
    if not has_table(con, TABLE):
        return None
    row = con.execute(f"SELECT value FROM {TABLE} WHERE key = ?", [key]).fetchone()
    return row[0] if row else None


def set_meta(con, key, value):
    con.execute(SCHEMA)
    con.execute(f"INSERT OR REPLACE INTO {TABLE} VALUES (?, ?, ?)",
                [key, value, now_ts()])


def bronze_is_traded(con):
    return get_meta(con, BRONZE_TERMS) == TRADED
