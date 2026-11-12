"""Backfill symbol selection: multi-level columns even for one ticker."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

import pandas as pd
import pytest

from backfill import _symbol_frame

DATES = pd.bdate_range(end="2026-08-10", periods=3)


def _multi(symbols):
    return pd.concat(
        {s: pd.DataFrame({"Close": [1.0] * 3, "Volume": [10] * 3},
                         index=DATES) for s in symbols}, axis=1)


def test_a_symbol_whose_newest_bar_is_old_is_stale_not_fetched():
    # The source can return rows that stop weeks ago (a renamed listing):
    # "rows came back" must not count as current.
    import duckdb
    from backfill import stale_from_source

    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE bronze_prices (symbol VARCHAR, date DATE)")
    con.execute("INSERT INTO bronze_prices VALUES ('LIVE', '2026-11-06'), "
                "('OLD', '2026-09-10')")
    assert stale_from_source(con, ["LIVE", "OLD"], "2026-11-05") == [
        ("OLD", "2026-09-10")]


def test_multi_level_single_symbol_selects_by_ticker():
    # The 726 % 25 == 1 crash: a one-symbol batch still has ticker-level
    # columns and must be selected, not passed through.
    frame = _symbol_frame(_multi(["AAA"]), "AAA")
    assert list(frame.columns) == ["Close", "Volume"]


def test_multi_level_batch_selects_each_symbol():
    raw = _multi(["AAA", "BBB"])
    assert list(_symbol_frame(raw, "BBB").columns) == ["Close", "Volume"]
    with pytest.raises(KeyError):
        _symbol_frame(raw, "MISSING")


def test_single_level_frame_passes_through():
    flat = pd.DataFrame({"Close": [1.0] * 3, "Volume": [10] * 3}, index=DATES)
    assert _symbol_frame(flat, "AAA") is flat


def test_backfill_drops_partial_bar_for_unfinished_session(monkeypatch):
    # The 2026-08-12 incident: a pre-open run stored 313 partial same-day
    # bars. Rows dated past the completed-session cutoff never land.
    import datetime

    import duckdb

    import backfill

    def fake_fetch(batch, start):
        return pd.DataFrame({
            "Date": [pd.Timestamp("2026-08-10"), pd.Timestamp("2099-01-01")],
            "symbol": "AAA", "open": 1.0, "high": 1.0, "low": 1.0,
            "close": 1.0, "adj_close": 1.0, "volume": 10,
            "dividend": 0.0, "split_ratio": 0.0})

    monkeypatch.setattr(backfill, "_fetch", fake_fetch)
    con = duckdb.connect(":memory:")
    total, missing = backfill.backfill_prices(con, ["AAA"], "2026-08-01")
    dates = [r[0] for r in con.execute(
        "SELECT date FROM bronze_prices ORDER BY date").fetchall()]
    assert dates == [datetime.date(2026, 8, 10)]
    assert total == 1 and missing == []


# --- split capture: every split Yahoo reports lands in corporate_actions once


def _frame_with_split(ratio, symbol="AAA"):
    return pd.DataFrame({
        "Date": [pd.Timestamp("2026-08-07"), pd.Timestamp("2026-08-10")],
        "symbol": symbol, "open": 1.0, "high": 1.0, "low": 1.0,
        "close": 1.0, "adj_close": 1.0, "volume": 10,
        "dividend": 0.0, "split_ratio": [0.0, ratio]})


def _events(con):
    return con.execute(
        "SELECT symbol, date, ratio FROM corporate_actions ORDER BY symbol, date"
    ).fetchall()


def test_backfill_logs_each_fetched_split_exactly_once(monkeypatch):
    import datetime

    import duckdb

    import backfill

    monkeypatch.setattr(backfill, "_fetch", lambda b, s: _frame_with_split(4.0))
    con = duckdb.connect(":memory:")
    backfill.backfill_prices(con, ["AAA"], "2026-08-01")
    backfill.backfill_prices(con, ["AAA"], "2026-08-01")     # second run
    assert _events(con) == [("AAA", datetime.date(2026, 8, 10), 4.0)]
    # No split row in bronze is without its event (the STORY-000003 query).
    assert con.execute(
        "SELECT COUNT(*) FROM bronze_prices p "
        "LEFT JOIN corporate_actions c USING (symbol, date) "
        "WHERE p.split_ratio > 0 AND p.split_ratio <> 1 AND c.symbol IS NULL"
    ).fetchone()[0] == 0


def test_conflicting_split_ratio_is_printed_and_the_log_is_kept(
        monkeypatch, capsys):
    import datetime

    import duckdb

    import backfill

    con = duckdb.connect(":memory:")
    monkeypatch.setattr(backfill, "_fetch", lambda b, s: _frame_with_split(4.0))
    backfill.backfill_prices(con, ["AAA"], "2026-08-01")
    monkeypatch.setattr(backfill, "_fetch", lambda b, s: _frame_with_split(2.0))
    backfill.backfill_prices(con, ["AAA"], "2026-08-01")
    out = capsys.readouterr().out
    assert "SPLIT RATIO CONFLICT AAA 2026-08-10" in out
    assert "logged 4" in out and "reports 2" in out
    assert _events(con) == [("AAA", datetime.date(2026, 8, 10), 4.0)]


def test_splits_already_in_bronze_are_seeded_into_the_log(monkeypatch):
    # The log starts empty on an existing warehouse: rows stored before the
    # log existed are recorded from bronze, for symbols not in this fetch.
    import datetime

    import duckdb

    import backfill

    con = _traded_warehouse()
    con.execute("INSERT INTO bronze_prices (symbol, date, close, split_ratio) "
                "VALUES ('OLD', '2021-07-20', 1.0, 10.0), "
                "('OLD', '2021-07-21', 1.0, 0.0)")
    monkeypatch.setattr(backfill, "_fetch", lambda b, s: pd.DataFrame())
    backfill.backfill_prices(con, ["AAA"], "2026-08-01")
    assert _events(con) == [("OLD", datetime.date(2021, 7, 20), 10.0)]


# --- append-only: past sessions are never rewritten (STORY-000006)


def _traded_warehouse():
    import duckdb

    from common.warehouse_meta import BRONZE_TERMS, TRADED, set_meta

    import backfill

    con = duckdb.connect(":memory:")
    con.execute(backfill.SCHEMA)
    set_meta(con, BRONZE_TERMS, TRADED)
    return con


def _bars(symbol, closes, splits=None):
    """Fetched frame: one row per (date, close); splits = {date: ratio}."""
    dates = sorted(closes)
    return pd.DataFrame({
        "Date": [pd.Timestamp(d) for d in dates], "symbol": symbol,
        "open": [closes[d] for d in dates], "high": [closes[d] for d in dates],
        "low": [closes[d] for d in dates], "close": [closes[d] for d in dates],
        "adj_close": [closes[d] for d in dates], "volume": 1000,
        "dividend": 0.0,
        "split_ratio": [(splits or {}).get(d, 0.0) for d in dates]})


def _stored(con, symbol):
    return con.execute(
        "SELECT date, close, volume, _load_type FROM bronze_prices "
        "WHERE symbol = ? ORDER BY date", [symbol]).fetchall()


def _fingerprint(con):
    return con.execute("SELECT COUNT(*), SUM(HASH(symbol, date, close)) "
                       "FROM bronze_prices").fetchone()


def test_backfill_source_has_no_delete_against_bronze():
    import inspect

    import backfill

    assert "DELETE" not in inspect.getsource(backfill)


def test_backfill_refuses_a_warehouse_still_in_restated_terms(monkeypatch):
    import duckdb

    import backfill

    con = duckdb.connect(":memory:")
    con.execute(backfill.SCHEMA)
    con.execute("INSERT INTO bronze_prices (symbol, date, close) "
                "VALUES ('AAA', '2026-08-07', 100.0)")
    monkeypatch.setattr(backfill, "_fetch", lambda b, s: pd.DataFrame())
    with pytest.raises(SystemExit, match="migrate_traded_prices"):
        backfill.backfill_prices(con, ["AAA"], "2026-08-01")


def test_an_empty_warehouse_is_marked_traded_and_loads(monkeypatch):
    import duckdb

    from common.warehouse_meta import bronze_is_traded

    import backfill

    con = duckdb.connect(":memory:")
    monkeypatch.setattr(backfill, "_fetch",
                        lambda b, s: _bars("AAA", {"2026-08-07": 100.0}))
    total, _ = backfill.backfill_prices(con, ["AAA"], "2026-08-01")
    assert total == 1 and bronze_is_traded(con)


def test_second_run_rewrites_nothing_and_appends_the_new_session(monkeypatch):
    import datetime

    import backfill

    con = _traded_warehouse()
    monkeypatch.setattr(backfill, "_fetch", lambda b, s: _bars(
        "AAA", {"2026-08-06": 99.0, "2026-08-07": 100.0}))
    backfill.backfill_prices(con, ["AAA"], "2026-08-01")
    before = _fingerprint(con)

    # The source now restates 08-07 and adds 08-10. Stored rows stay put;
    # the new session lands once.
    monkeypatch.setattr(backfill, "_fetch", lambda b, s: _bars(
        "AAA", {"2026-08-06": 99.0, "2026-08-07": 50.0, "2026-08-10": 101.0}))
    total, _ = backfill.backfill_prices(con, ["AAA"], "2026-08-01")
    assert total == 1
    rows = _stored(con, "AAA")
    assert [(r[0], r[1]) for r in rows] == [
        (datetime.date(2026, 8, 6), 99.0), (datetime.date(2026, 8, 7), 100.0),
        (datetime.date(2026, 8, 10), 101.0)]
    assert [r[3] for r in rows] == ["full", "full", "incremental"]
    assert _fingerprint(con)[0] == before[0] + 1
    assert _fingerprint(_subset(con, "date <= '2026-08-07'")) == before


def _subset(con, where):
    sub = con.cursor()
    sub.execute("CREATE OR REPLACE TEMP TABLE _sub AS "
                f"SELECT * FROM bronze_prices WHERE {where}")

    class _View:
        def execute(self, sql):
            return sub.execute(sql.replace("bronze_prices", "_sub"))
    return _View()


def test_new_rows_are_stored_in_traded_terms(monkeypatch):
    # A 2-for-1 on 08-10. Yahoo serves 08-07 halved; the split-date row is
    # already post-split. For a known symbol only 08-10 is new and lands
    # as served; the stored 08-07 keeps its traded close. For a symbol
    # with no history, 08-07 is converted back to traded terms on the way in.
    import datetime

    import backfill

    con = _traded_warehouse()
    monkeypatch.setattr(backfill, "_fetch",
                        lambda b, s: _bars("OLD", {"2026-08-07": 100.0}))
    backfill.backfill_prices(con, ["OLD"], "2026-08-01")

    served = {"2026-08-07": 50.0, "2026-08-10": 52.0}
    # Like the source, a fetch returns only the batch it was asked for.
    monkeypatch.setattr(backfill, "_fetch", lambda batch, s: pd.concat(
        [_bars(sym, served, {"2026-08-10": 2.0}) for sym in batch],
        ignore_index=True))
    backfill.backfill_prices(con, ["OLD", "NEW"], "2026-08-01")

    assert [(r[0], r[1], r[2]) for r in _stored(con, "OLD")] == [
        (datetime.date(2026, 8, 7), 100.0, 1000),
        (datetime.date(2026, 8, 10), 52.0, 1000)]
    assert [(r[0], r[1], r[2]) for r in _stored(con, "NEW")] == [
        (datetime.date(2026, 8, 7), 100.0, 500),
        (datetime.date(2026, 8, 10), 52.0, 1000)]


# --- source revisions: a restated stored session is logged, never applied


def _revisions(con):
    return con.execute("SELECT symbol, date, stored_close, source_close "
                       "FROM source_revisions ORDER BY date").fetchall()


def test_a_changed_close_in_the_overlap_is_logged_and_bronze_keeps_its_value(
        monkeypatch, capsys):
    import datetime

    import backfill

    con = _traded_warehouse()
    monkeypatch.setattr(backfill, "_fetch", lambda b, s: _bars(
        "AAA", {"2026-08-06": 99.0, "2026-08-07": 100.0}))
    backfill.backfill_prices(con, ["AAA"], "2026-08-01")
    monkeypatch.setattr(backfill, "_fetch", lambda b, s: _bars(
        "AAA", {"2026-08-06": 99.0, "2026-08-07": 50.0, "2026-08-10": 101.0}))
    backfill.backfill_prices(con, ["AAA"], "2026-08-01")

    assert _revisions(con) == [("AAA", datetime.date(2026, 8, 7), 100.0, 50.0)]
    assert _stored(con, "AAA")[1][1] == 100.0
    out = capsys.readouterr().out
    assert "SOURCE REVISION AAA 2026-08-07: stored 100.0000, source now 50.0000" in out


def test_an_unchanged_overlap_logs_no_revision(monkeypatch):
    import backfill

    con = _traded_warehouse()
    bars = _bars("AAA", {"2026-08-06": 99.0, "2026-08-07": 100.0})
    monkeypatch.setattr(backfill, "_fetch", lambda b, s: bars)
    backfill.backfill_prices(con, ["AAA"], "2026-08-01")
    backfill.backfill_prices(con, ["AAA"], "2026-08-01")
    assert _revisions(con) == []


def test_the_same_revision_is_logged_once_across_days(monkeypatch, capsys):
    import backfill

    con = _traded_warehouse()
    monkeypatch.setattr(backfill, "_fetch", lambda b, s: _bars(
        "AAA", {"2026-08-07": 100.0}))
    backfill.backfill_prices(con, ["AAA"], "2026-08-01")
    monkeypatch.setattr(backfill, "_fetch", lambda b, s: _bars(
        "AAA", {"2026-08-07": 50.0}))
    backfill.backfill_prices(con, ["AAA"], "2026-08-01")
    backfill.backfill_prices(con, ["AAA"], "2026-08-01")   # next day, same restatement
    assert len(_revisions(con)) == 1
    assert capsys.readouterr().out.count("SOURCE REVISION") == 1


def test_a_gap_inside_the_fetched_window_is_filled_never_rewritten(monkeypatch):
    import datetime

    import backfill

    con = _traded_warehouse()
    monkeypatch.setattr(backfill, "_fetch", lambda b, s: _bars(
        "AAA", {"2026-08-05": 98.0, "2026-08-07": 100.0}))      # 08-06 missing
    backfill.backfill_prices(con, ["AAA"], "2026-08-01")
    monkeypatch.setattr(backfill, "_fetch", lambda b, s: _bars(
        "AAA", {"2026-08-05": 98.0, "2026-08-06": 99.0, "2026-08-07": 55.0}))
    total, _ = backfill.backfill_prices(con, ["AAA"], "2026-08-01")
    assert total == 1
    assert [(r[0], r[1]) for r in _stored(con, "AAA")] == [
        (datetime.date(2026, 8, 5), 98.0), (datetime.date(2026, 8, 6), 99.0),
        (datetime.date(2026, 8, 7), 100.0)]                    # 08-07 kept


def test_a_fresh_symbol_whose_split_the_source_left_unrestated_is_not_multiplied(
        monkeypatch, capsys):
    import backfill

    con = _traded_warehouse()
    # Served closes halve on the split day: the source did not restate.
    monkeypatch.setattr(backfill, "_fetch", lambda b, s: _bars(
        "NEW", {"2026-08-06": 94.0, "2026-08-07": 90.0, "2026-08-11": 45.5},
        {"2026-08-11": 2.0}))
    backfill.backfill_prices(con, ["NEW"], "2026-08-01")
    assert [r[1] for r in _stored(con, "NEW")] == [94.0, 90.0, 45.5]
    assert "SPLIT NOT RESTATED BY SOURCE NEW 2026-08-11" in capsys.readouterr().out


def test_known_symbols_fetch_from_the_overlap_and_new_ones_from_start(monkeypatch):
    import backfill

    con = _traded_warehouse()
    con.execute("INSERT INTO bronze_prices (symbol, date, close) "
                "VALUES ('OLD', '2026-08-07', 100.0)")
    calls = []

    def fake_fetch(batch, start):
        calls.append((tuple(batch), str(start)))
        return pd.DataFrame()

    monkeypatch.setattr(backfill, "_fetch", fake_fetch)
    backfill.backfill_prices(con, ["OLD", "NEW"], "2010-01-01")
    assert sorted(calls) == [(("NEW",), "2010-01-01"),
                             (("OLD",), "2026-07-31")]


# --- a fetch that brings nothing back is a failed step, not a quiet success


def _d(text):
    import datetime
    return datetime.date.fromisoformat(text)


def test_an_empty_fetch_leaves_every_live_symbol_short():
    from backfill import live_shortfall

    before = {f"S{i}": _d("2026-11-09") for i in range(50)}
    short, live = live_shortfall(before, dict(before), _d("2026-11-10"),
                                 list(before))
    assert len(live) == 50 and short == sorted(before)


def test_a_symbol_that_stopped_weeks_ago_is_not_live():
    # A delisted name returns no rows on every run; it is stale from
    # source and must not fail the step forever.
    from backfill import live_shortfall

    before = {"LIVE": _d("2026-11-09"), "GONE": _d("2026-08-14")}
    after = {"LIVE": _d("2026-11-10"), "GONE": _d("2026-08-14")}
    short, live = live_shortfall(before, after, _d("2026-11-10"),
                                 ["LIVE", "GONE"])
    assert live == ["LIVE"] and short == []


def test_a_session_already_stored_is_not_short():
    from backfill import live_shortfall

    stored = {"AAA": _d("2026-11-10"), "BBB": _d("2026-11-10")}
    assert live_shortfall(stored, dict(stored), _d("2026-11-10"),
                          ["AAA", "BBB"])[0] == []


def test_a_symbol_with_nothing_stored_yet_is_live():
    # A swapped watchlist: the only stored name is long dead. The new names
    # must still fail an outage. The dead name stays in the live set for
    # this one run, until the new names give the warehouse a newer bar.
    from backfill import live_shortfall

    before = {"GONE": _d("2026-08-14")}
    outage = live_shortfall(before, dict(before), _d("2026-11-10"),
                            ["GONE", "NEW1", "NEW2"])
    assert outage == (["GONE", "NEW1", "NEW2"], ["GONE", "NEW1", "NEW2"])
    loaded = {**before, "NEW1": _d("2026-11-10"), "NEW2": _d("2026-11-10")}
    assert live_shortfall(before, loaded, _d("2026-11-10"),
                          ["GONE", "NEW1", "NEW2"])[0] == ["GONE"]


def test_an_empty_warehouse_counts_every_requested_symbol_as_live():
    from backfill import live_shortfall

    short, live = live_shortfall({}, {}, _d("2026-11-10"), ["AAA", "BBB"])
    assert live == ["AAA", "BBB"] and short == ["AAA", "BBB"]


def _run_main(monkeypatch, tmp_path, fetch, seed):
    import duckdb

    import backfill

    warehouse = str(tmp_path / "market.duckdb")
    con = duckdb.connect(warehouse)
    con.execute(backfill.SCHEMA)
    from common.warehouse_meta import BRONZE_TERMS, TRADED, set_meta
    set_meta(con, BRONZE_TERMS, TRADED)
    for symbol, day in seed:
        con.execute("INSERT INTO bronze_prices (symbol, date, close) "
                    "VALUES (?, ?, 100.0)", [symbol, day])
    con.close()
    monkeypatch.setattr(backfill, "WAREHOUSE", warehouse)
    monkeypatch.setattr(backfill, "_fetch", fetch)
    monkeypatch.setattr(backfill, "daily_symbols",
                        lambda *a: sorted({s for s, _ in seed}))
    monkeypatch.setattr(backfill, "ensure_quote_types", lambda con, s: 0)
    monkeypatch.setattr(backfill, "completed_session_cutoff",
                        lambda now=None: _d("2026-11-10"))
    monkeypatch.setattr(sys, "argv", ["backfill.py"])
    return backfill.main()


def test_main_exits_nonzero_when_the_source_serves_nothing(
        monkeypatch, tmp_path, capsys):
    # The outage case: every batch comes back empty. The chain retries only
    # on a non-zero exit, so this must not read as success.
    seed = [(f"S{i}", "2026-11-09") for i in range(10)]
    code = _run_main(monkeypatch, tmp_path, lambda b, s: pd.DataFrame(), seed)
    assert code == 1
    assert "FETCH SHORT" in capsys.readouterr().out


def test_main_exits_zero_when_only_a_dead_symbol_returns_nothing(
        monkeypatch, tmp_path):
    seed = [(f"S{i}", "2026-11-09") for i in range(10)] + [
        ("GONE", "2026-08-14")]

    def fetch(batch, start):
        live = [s for s in batch if s != "GONE"]
        return pd.concat([_bars(s, {"2026-11-10": 101.0}) for s in live])

    assert _run_main(monkeypatch, tmp_path, fetch, seed) == 0
