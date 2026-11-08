"""Source revisions: the source changed a price we already stored.

With bronze_prices append-only, a restatement Yahoo makes to a session we
hold is no longer absorbed by a rewrite; it is seen in the overlap window
of the next fetch. Each difference is recorded here once, with the stored
and the newly served close, and bronze keeps the stored value. The daily
data-proof report lists these so a revision is a known fact, not a silent
drift.
"""
from common.run_context import now_ts
from common.tables import has_table

TABLE = "source_revisions"
TOLERANCE = 0.005          # half a cent: anything smaller is float noise

SCHEMA = f"""
    CREATE TABLE IF NOT EXISTS {TABLE} (
        symbol VARCHAR NOT NULL,
        date DATE NOT NULL,
        stored_close DOUBLE NOT NULL,
        source_close DOUBLE NOT NULL,
        first_seen VARCHAR NOT NULL,
        PRIMARY KEY (symbol, date, source_close)
    )
"""


def record_revisions(con, rows):
    """Append (symbol, date, stored_close, source_close) rows not yet logged
    for that served value. Returns the rows that were new."""
    con.execute(SCHEMA)
    stamp, new = now_ts(), []
    for symbol, day, stored, source in rows:
        seen = con.execute(
            f"SELECT 1 FROM {TABLE} WHERE symbol = ? AND date = ? "
            "AND source_close = ?", [symbol, day, source]).fetchone()
        if seen:
            continue
        con.execute(f"INSERT INTO {TABLE} VALUES (?, ?, ?, ?, ?)",
                    [symbol, day, stored, source, stamp])
        new.append((symbol, day, stored, source))
    return new


def revisions_since(con, first_seen):
    """[(symbol, date, stored_close, source_close)] first seen at or after
    the ISO timestamp, in symbol order; empty without the table."""
    if not has_table(con, TABLE):
        return []
    return con.execute(
        f"SELECT symbol, date, stored_close, source_close FROM {TABLE} "
        "WHERE first_seen >= ? ORDER BY symbol, date", [first_seen]).fetchall()
