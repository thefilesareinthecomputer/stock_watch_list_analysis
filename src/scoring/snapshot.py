"""Append-only recommendation snapshot.

A conclusion that can silently change is not evidence of anything. Every other
gold table is rebuilt with CREATE OR REPLACE on each run, and `auto_adjust=True`
rescales the whole close history on every dividend - so without this table there
is no record of what was recommended on any past date, and backtesting or
judging past decisions is impossible in principle, not just unimplemented.

IMMUTABILITY RULE: first write wins, per (as_of_date, methodology_version).

Re-running a day inserts nothing; the original snapshot stands. Changing how the
score is computed means bumping METHODOLOGY_VERSION, which writes a new row
*alongside* the old one rather than restating it. That is what makes "what did
this system say on 2026-03-01, and on what basis" answerable later.

Pinned by tests/test_snapshot.py.
"""
from scoring.components import METHODOLOGY_VERSION

# Only the fields needed to reconstruct and judge a recommendation. Indicator
# detail stays in signal_history; duplicating it here would make the immutable
# table the widest one in the warehouse for no gain.
SNAPSHOT_COLUMNS = [
    "symbol",
    "as_of_date",
    "composite_score",
    "composite_rank",
    "momentum_pct",
    "value_pct",
    "risk_pct",
    "quality_pct",
    "last_closing_price",
    "trade_signal",
]


def select_sql(source, run_id, version=METHODOLOGY_VERSION):
    """Rows to snapshot, stamped with methodology and run provenance."""
    cols = ",\n            ".join(SNAPSHOT_COLUMNS)
    return f"""
        SELECT
            {cols},
            '{version}' AS methodology_version,
            '{run_id}' AS _run_id,
            CURRENT_TIMESTAMP() AS _snapshot_ts
        FROM {source}
    """


def create_sql(table, source, run_id, version=METHODOLOGY_VERSION):
    """Create the table on first run, taking its schema from the source.

    WHERE 1 = 0 creates the shape without rows, so the schema cannot drift from
    what select_sql actually produces.
    """
    return (f"CREATE TABLE IF NOT EXISTS {table} AS "
            f"SELECT * FROM ({select_sql(source, run_id, version)}) WHERE 1 = 0")


def insert_sql(table, source, run_id, version=METHODOLOGY_VERSION):
    """Append this run's recommendations, unless the day is already recorded.

    The NOT EXISTS guard is the immutability mechanism: if any row exists for
    this (as_of_date, methodology_version), the whole insert is a no-op. Re-runs
    and retries are therefore safe, and a past snapshot can never be rewritten
    by a later run of the same methodology.
    """
    return f"""
        INSERT INTO {table}
        SELECT * FROM ({select_sql(source, run_id, version)}) s
        WHERE NOT EXISTS (
            SELECT 1 FROM {table} r
            WHERE r.as_of_date = s.as_of_date
              AND r.methodology_version = s.methodology_version
        )
    """
