"""Corporate actions as an append-only event log, the way trades are.

Yahoo restates every past close after a split, so a split can only be
trusted as evidence if it is recorded once, on first sight, and never
rewritten. Every traded-price conversion and every adjusted series is
derived from this log plus bronze_prices; nothing downstream depends on a
history the source may restate.

Grain: one row per (symbol, date, type). A reverse split is a split with a
ratio below 1 (Yahoo reports a 1-for-10 as 0.1). Dividends stay in
bronze_prices. There is no path here that alters or removes a row: a
source that later disagrees with the log is reported back to the caller
as a conflict and the log keeps what it first saw.
"""
import math
from datetime import date

from common.run_context import now_ts
from common.tables import has_table

TABLE = "corporate_actions"
TYPES = ("split",)

SCHEMA = f"""
    CREATE TABLE IF NOT EXISTS {TABLE} (
        symbol VARCHAR NOT NULL,
        date DATE NOT NULL,
        ratio DOUBLE NOT NULL,         -- shares after / shares before
        type VARCHAR NOT NULL,
        source VARCHAR NOT NULL,       -- who reported it, e.g. yfinance
        first_seen VARCHAR NOT NULL,   -- when this log first recorded it (UTC)
        PRIMARY KEY (symbol, date, type)
    )
"""


def ensure_table(con):
    con.execute(SCHEMA)


def _validate(event):
    """One clean event dict, or ValueError naming the defective field."""
    symbol = str(event.get("symbol") or "").strip().upper()
    if not symbol:
        raise ValueError("symbol is required")
    day = event.get("date")
    if not isinstance(day, date):
        try:
            day = date.fromisoformat(str(day))
        except ValueError:
            raise ValueError(f"date must be YYYY-MM-DD, got {day!r}") from None
    ratio = event.get("ratio")
    try:
        ratio = float(ratio)
    except (TypeError, ValueError):
        raise ValueError(f"ratio must be a number, got {ratio!r}") from None
    if not ratio > 0 or ratio == 1.0 or not math.isfinite(ratio):
        raise ValueError(f"ratio must be positive and not 1, got {ratio}")
    kind = event.get("type", "split")
    if kind not in TYPES:
        raise ValueError(f"type must be one of {TYPES}, got {kind!r}")
    return {"symbol": symbol, "date": day, "ratio": ratio, "type": kind}


def record_actions(con, events, source, first_seen=None):
    """Append the events this log has not seen. Returns (inserted, conflicts).

    An event whose (symbol, date, type) is already logged writes nothing,
    whatever its source or stamp. If its ratio differs from the logged one it
    comes back in `conflicts` as (symbol, date, type, logged, reported) so the
    caller can print it; the log is left as it was. A defective event raises
    before anything in the batch is written.
    """
    ensure_table(con)
    clean = [_validate(e) for e in events]
    stamp = first_seen or now_ts()
    inserted, conflicts = 0, []
    for e in clean:
        key = [e["symbol"], e["date"], e["type"]]
        row = con.execute(
            f"SELECT ratio FROM {TABLE} WHERE symbol = ? AND date = ? AND type = ?",
            key).fetchone()
        if row is None:
            con.execute(f"INSERT INTO {TABLE} VALUES (?, ?, ?, ?, ?, ?)",
                        [*key[:2], e["ratio"], e["type"], source, stamp])
            inserted += 1
        elif not math.isclose(row[0], e["ratio"], rel_tol=1e-9):
            conflicts.append((*key, row[0], e["ratio"]))
    return inserted, conflicts


def splits_for(con, symbol):
    """[(date, ratio)] of logged splits for one symbol, oldest first."""
    if not has_table(con, TABLE):
        return []
    return con.execute(
        f"SELECT date, ratio FROM {TABLE} WHERE symbol = ? AND type = 'split' "
        "ORDER BY date", [symbol.strip().upper()]).fetchall()
