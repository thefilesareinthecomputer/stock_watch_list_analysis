"""scripts/fresh.py: the warehouse staleness probe behind /recs."""
import importlib.util
import os
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

import duckdb

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

_SPEC = importlib.util.spec_from_file_location(
    "fresh", os.path.join(os.path.dirname(__file__), "..", "scripts", "fresh.py"))
fresh = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(fresh)

# Friday 2026-08-21 was a session; Monday 2026-08-24 pre-open expects it.
MONDAY_PREOPEN = datetime(2026, 8, 24, 8, 0, tzinfo=ZoneInfo("America/New_York"))


def _warehouse(tmp_path, latest):
    path = str(tmp_path / "w.duckdb")
    con = duckdb.connect(path)
    con.execute("CREATE TABLE bronze_prices (symbol VARCHAR, date DATE)")
    con.execute("INSERT INTO bronze_prices VALUES ('XYZ', ?)", [latest])
    con.close()
    return path


def test_fresh_when_latest_bar_is_last_completed_session(tmp_path):
    latest, expected = fresh.check(_warehouse(tmp_path, "2026-08-21"),
                                   now=MONDAY_PREOPEN)
    assert str(latest) == str(expected) == "2026-08-21"


def test_stale_when_warehouse_lags(tmp_path):
    latest, expected = fresh.check(_warehouse(tmp_path, "2026-08-14"),
                                   now=MONDAY_PREOPEN)
    assert str(latest) < str(expected)


def test_held_names_are_checked_one_by_one_not_by_the_global_max(tmp_path):
    # The global max can be current while a held name is not. One session
    # behind is a fetch glitch (fail and refresh); further behind is a dead
    # listing (report it, but do not block every other name).
    path = str(tmp_path / "w.duckdb")
    con = duckdb.connect(path)
    con.execute("CREATE TABLE bronze_prices (symbol VARCHAR, date DATE)")
    con.execute("INSERT INTO bronze_prices VALUES ('LIVE', '2026-08-21'), "
                "('LAG1', '2026-08-20'), ('DEAD', '2026-08-10')")
    con.close()
    status = fresh.held_status(path, ["LIVE", "LAG1", "DEAD", "NONE"],
                               now=MONDAY_PREOPEN)
    assert status == {"behind": ["LAG1"], "dead": ["DEAD", "NONE"]}


def test_weekend_expects_friday_not_saturday(tmp_path):
    saturday_night = datetime(2026, 8, 22, 23, 0,
                              tzinfo=ZoneInfo("America/New_York"))
    _, expected = fresh.check(_warehouse(tmp_path, "2026-08-21"),
                              now=saturday_night)
    assert str(expected) == "2026-08-21"
