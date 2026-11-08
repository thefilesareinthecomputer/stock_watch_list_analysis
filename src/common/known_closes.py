"""Published closes vs bronze: the fixture-backed proof (STORY-000010).

tests/fixtures/known_closes.json holds pre-split closes published outside
Yahoo, each with its citation. Checking bronze against them is the one
test that cannot pass by comparing our data with itself, so the migration
gate, the test suite and the daily data-proof report all run it.
"""
import json
import os

from common.corporate_actions import split_factor, splits_for
from common.tables import has_column
from common.warehouse_meta import bronze_is_traded

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
FIXTURE = os.path.join(ROOT, "tests", "fixtures", "known_closes.json")
TOLERANCE = 0.01


def load_known_closes(path=FIXTURE):
    with open(path) as f:
        return json.load(f)["rows"]


def check_known_closes(con, rows=None, tolerance=TOLERANCE):
    """[(symbol, date, published, stored, status)] with status 'ok', 'FAIL'
    or 'absent'. Before the migration `stored` is what the migration will
    store (the served close converted through the split log)."""
    rows = load_known_closes() if rows is None else rows
    traded = bronze_is_traded(con)
    out = []
    for row in rows:
        hit = con.execute(
            "SELECT close FROM bronze_prices WHERE symbol = ? AND date = ?",
            [row["symbol"], row["date"]]).fetchone()
        if hit is None:
            out.append((row["symbol"], row["date"], row["close"], None, "absent"))
            continue
        stored = float(hit[0])
        if not traded:
            # Served terms: restated for the splits the row's capture run
            # knew, i.e. those on or before that run's newest session
            # (every split when the table carries no runs).
            vintage = None
            if has_column(con, "bronze_prices", "_run_id"):
                vintage = con.execute(
                    "SELECT MAX(date) FROM bronze_prices WHERE symbol = ? AND _run_id = "
                    "(SELECT _run_id FROM bronze_prices WHERE symbol = ? AND date = ?)",
                    [row["symbol"], row["symbol"], row["date"]]).fetchone()[0]
            stored *= split_factor(splits_for(con, row["symbol"]), row["date"], vintage)
        ok = abs(stored - row["close"]) < tolerance
        out.append((row["symbol"], row["date"], row["close"], stored,
                    "ok" if ok else "FAIL"))
    return out
