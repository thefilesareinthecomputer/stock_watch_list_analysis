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

    con = duckdb.connect(":memory:")
    con.execute(backfill.SCHEMA)
    con.execute("INSERT INTO bronze_prices (symbol, date, close, split_ratio) "
                "VALUES ('OLD', '2021-07-20', 1.0, 10.0), "
                "('OLD', '2021-07-21', 1.0, 0.0)")
    monkeypatch.setattr(backfill, "_fetch", lambda b, s: pd.DataFrame())
    backfill.backfill_prices(con, ["AAA"], "2026-08-01")
    assert _events(con) == [("OLD", datetime.date(2021, 7, 20), 10.0)]
