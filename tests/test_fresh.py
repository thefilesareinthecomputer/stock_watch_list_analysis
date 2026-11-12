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


# --- the gate: data the guard must not run on, beyond the bronze dates


def _full_warehouse(tmp_path, live, stale=(), unadjusted=(), latest="2026-08-21"):
    """A traded warehouse: `live` symbols current in bronze and in adjusted
    prices, `stale` symbols stopped on 2026-08-10, `unadjusted` symbols
    current in bronze with no adjusted rows."""
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))
    import backfill
    from common.corporate_actions import record_actions
    from common.warehouse_meta import BRONZE_TERMS, TRADED, set_meta

    path = str(tmp_path / "full.duckdb")
    con = duckdb.connect(path)
    con.execute(backfill.SCHEMA)
    set_meta(con, BRONZE_TERMS, TRADED)
    record_actions(con, [{"symbol": "ZZZ", "date": "2020-01-02", "ratio": 2.0}],
                   source="test")
    con.execute("CREATE TABLE silver_adjusted_prices (symbol VARCHAR, "
                "date TIMESTAMP, adj_open DOUBLE, adj_close DOUBLE)")
    for symbol in list(live) + list(unadjusted):
        con.execute("INSERT INTO bronze_prices (symbol, date, close, adj_close, "
                    "dividend, _run_id) VALUES (?, ?, 10.0, 10.0, 0.0, 'r')",
                    [symbol, latest])
    for symbol in live:
        con.execute("INSERT INTO silver_adjusted_prices VALUES (?, ?, 10.0, 10.0)",
                    [symbol, latest])
    for symbol in stale:
        con.execute("INSERT INTO bronze_prices (symbol, date, close, adj_close, "
                    "dividend, _run_id) VALUES (?, '2026-08-10', 10.0, 10.0, "
                    "0.0, 'r')", [symbol])
    con.close()
    return path


LIVE = [f"S{i:02d}" for i in range(60)]


def _checks(problems):
    return [name for name, _ in problems]


def test_gate_passes_on_a_complete_warehouse(tmp_path):
    path = _full_warehouse(tmp_path, LIVE)
    problems, _ = fresh.gate(path, ["S00", "S01"], LIVE, now=MONDAY_PREOPEN,
                             known_rows=[])
    assert problems == []


def test_a_live_held_name_without_adjusted_prices_fails(tmp_path):
    # The guard values holdings from silver_adjusted_prices. Bronze can be
    # current while the build left a holding out.
    path = _full_warehouse(tmp_path, LIVE, unadjusted=["NEWCO"])
    problems, _ = fresh.gate(path, ["S00", "NEWCO"], LIVE + ["NEWCO"],
                             now=MONDAY_PREOPEN, known_rows=[])
    assert _checks(problems) == ["adjusted prices"]
    assert "NEWCO" in problems[0][1]


def test_a_dead_held_name_is_noted_and_does_not_fail(tmp_path):
    # Two or more sessions behind is a dead listing: it must never block
    # every other holding, and it must not hide either.
    path = _full_warehouse(tmp_path, LIVE, stale=["DEAD"])
    problems, notes = fresh.gate(path, ["S00", "DEAD"], LIVE + ["DEAD"],
                                 now=MONDAY_PREOPEN, known_rows=[])
    assert problems == []
    assert any("DEAD" in note for note in notes)


def test_a_stale_watchlist_above_the_tolerance_fails(tmp_path):
    stale = ["OLD1", "OLD2", "OLD3"]            # 3 of 62 non-held: 4.8%
    path = _full_warehouse(tmp_path, LIVE, stale=stale)
    problems, _ = fresh.gate(path, ["S00"], LIVE + stale, now=MONDAY_PREOPEN,
                             known_rows=[])
    assert _checks(problems) == ["stale rate"]


def test_a_symbol_with_no_rows_at_all_counts_as_stale(tmp_path):
    missing = ["GONE1", "GONE2", "GONE3"]
    path = _full_warehouse(tmp_path, LIVE)
    problems, _ = fresh.gate(path, ["S00"], LIVE + missing, now=MONDAY_PREOPEN,
                             known_rows=[])
    assert _checks(problems) == ["stale rate"]


def test_a_failed_known_close_fails(tmp_path):
    path = _full_warehouse(tmp_path, LIVE)
    rows = [{"symbol": "S00", "date": "2026-08-21", "close": 99.0}]
    problems, _ = fresh.gate(path, ["S00"], LIVE, now=MONDAY_PREOPEN,
                             known_rows=rows)
    assert _checks(problems) == ["known closes"]


def test_an_absent_known_close_is_said_out_loud_and_does_not_block(tmp_path):
    path = _full_warehouse(tmp_path, LIVE)
    rows = [{"symbol": "NOTHERE", "date": "2026-08-21", "close": 99.0}]
    problems, notes = fresh.gate(path, ["S00"], LIVE, now=MONDAY_PREOPEN,
                                 known_rows=rows)
    assert problems == []
    assert "known closes: 1 of 1 not in this warehouse, not checked" in notes


def test_a_new_mismatch_on_a_held_name_fails_and_a_ruled_one_does_not(
        tmp_path, monkeypatch):
    path = _full_warehouse(tmp_path, LIVE)
    monkeypatch.setattr(fresh, "divergent",
                        lambda con, symbols: [("S00", 0.30), ("S01", 0.15)])
    monkeypatch.setattr(fresh, "known_defects",
                        lambda check: {"S01": {"ruled": "2026-11-08"}})
    problems, _ = fresh.gate(path, ["S00", "S01"], LIVE, now=MONDAY_PREOPEN,
                             known_rows=[])
    assert _checks(problems) == ["adjustment mismatch"]
    assert "S00" in problems[0][1] and "S01" not in problems[0][1]


def test_a_bar_dated_after_the_expected_session_fails(tmp_path):
    path = _full_warehouse(tmp_path, LIVE, latest="2026-08-25")
    problems, _ = fresh.gate(path, ["S00"], LIVE, now=MONDAY_PREOPEN,
                             known_rows=[])
    assert "future bar" in _checks(problems)


def test_main_exits_nonzero_and_names_the_failed_check(
        tmp_path, monkeypatch, capsys):
    import datetime as dt

    path = _full_warehouse(tmp_path, LIVE)
    session = dt.date(2026, 8, 21)
    monkeypatch.setattr(fresh, "WAREHOUSE", path)
    monkeypatch.setattr(fresh, "check", lambda: (session, session))
    monkeypatch.setattr(fresh, "load_positions", lambda: [])
    monkeypatch.setattr(fresh, "held_status",
                        lambda w, held: {"behind": [], "dead": []})
    monkeypatch.setattr(fresh, "gate", lambda w, held, symbols: (
        [("known closes", "FAIL 1")], []))
    assert fresh.main() == 1
    assert "NOT TRUSTED: known closes - FAIL 1" in capsys.readouterr().out


def test_weekend_expects_friday_not_saturday(tmp_path):
    saturday_night = datetime(2026, 8, 22, 23, 0,
                              tzinfo=ZoneInfo("America/New_York"))
    _, expected = fresh.check(_warehouse(tmp_path, "2026-08-21"),
                              now=saturday_night)
    assert str(expected) == "2026-08-21"
