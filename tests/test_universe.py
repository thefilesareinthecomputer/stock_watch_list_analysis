"""Universe: dollar-volume tiering, PIT membership, emerging visibility."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import duckdb
import pandas as pd
import pytest

from common.universe import (
    build_line_of_sight, current_members, fetch_broad_window,
    rank_dollar_volume, update_membership,
)


def _broad(con, specs):
    """bronze_prices_broad from {symbol: (close_start, close_end, volume)} -
    260 sessions of linearly drifting close, constant volume."""
    rows = []
    dates = pd.bdate_range(end="2026-08-10", periods=260)
    for symbol, (start, end, volume) in specs.items():
        for i, date in enumerate(dates):
            close = start + (end - start) * i / 259
            rows.append((symbol, date.date(), close, volume))
    con.execute("CREATE OR REPLACE TABLE bronze_prices_broad "
                "(symbol VARCHAR, date DATE, close DOUBLE, volume BIGINT)")
    con.executemany("INSERT INTO bronze_prices_broad VALUES (?, ?, ?, ?)",
                    rows)
    return con


def _con(specs):
    return _broad(duckdb.connect(":memory:"), specs)


# Eleven symbols so PERCENT_RANK has granularity: second-best rank lands
# exactly at the 0.90 emerging bar. PNY out-surges ZIP but stays sub-dollar.
BASE = {"BIG": (100.0, 100.0, 10_000_000),   # huge, flat, in the tier
        "MID": (50.0, 50.0, 100_000),        # modest, flat, in the tier
        "ZIP": (2.0, 6.0, 40_000),           # small, tripling: emerging
        "PNY": (0.1, 0.9, 900_000),          # top surge, sub-dollar: floored
        **{f"F{i}": (10.0, 10.0, 10_000) for i in range(7)}}  # flat filler


def test_fetch_is_incremental_and_resumes_after_a_kill(monkeypatch):
    import datetime

    import common.universe as universe

    con = duckdb.connect(":memory:")
    today = datetime.date(2026, 8, 11)
    dates = pd.bdate_range(end="2026-08-10", periods=5)
    frame = pd.DataFrame({"Close": [10.0] * 5, "Volume": [1000] * 5},
                         index=dates)
    # Single-symbol batches: batch of one returns the raw frame directly.
    monkeypatch.setattr(universe, "FETCH_BATCH", 1)
    fetched = []

    def fetch(batch, start):
        if "DIE" in batch:
            raise KeyboardInterrupt  # the kill, mid-run
        fetched.append(list(batch))
        return frame

    with pytest.raises(KeyboardInterrupt):
        fetch_broad_window(con, ["AAA", "BBB", "DIE"], fetch=fetch,
                           today=today)
    # AAA and BBB survived the kill: batches are written as they land.
    survivors = {r[0] for r in con.execute(
        "SELECT DISTINCT symbol FROM bronze_prices_broad").fetchall()}
    assert survivors == {"AAA", "BBB"}

    # The rerun skips what landed and fetches only the remainder.
    fetched.clear()

    def fetch_retry(batch, start):
        fetched.append(list(batch))
        return frame

    fetch_broad_window(con, ["AAA", "BBB", "DIE"], fetch=fetch_retry,
                       today=today)
    assert fetched == [["DIE"]]

    # The kill can span midnight (it did, twice): a next-day rerun still
    # skips recent symbols, while a rerun past the resume window refetches.
    fetched.clear()
    fetch_broad_window(con, ["AAA", "BBB", "DIE"], fetch=fetch_retry,
                       today=today + datetime.timedelta(days=1))
    assert fetched == []
    fetch_broad_window(con, ["AAA"], fetch=fetch_retry,
                       today=today + datetime.timedelta(days=30))
    assert fetched == [["AAA"]]


def test_broad_fetch_drops_partial_bar_for_unfinished_session(monkeypatch):
    import datetime

    import common.universe as universe

    con = duckdb.connect(":memory:")
    monkeypatch.setattr(universe, "FETCH_BATCH", 1)
    frame = pd.DataFrame(
        {"Close": [10.0, 11.0], "Volume": [1000, 10]},
        index=pd.DatetimeIndex([pd.Timestamp("2026-08-10"),
                                pd.Timestamp("2099-01-01")]))
    fetch_broad_window(con, ["AAA"], fetch=lambda b, s: frame,
                       today=datetime.date(2026, 8, 11))
    dates = [r[0] for r in con.execute(
        "SELECT date FROM bronze_prices_broad").fetchall()]
    assert dates == [datetime.date(2026, 8, 10)]


def test_rank_dollar_volume_orders_by_median_traded_value():
    top = rank_dollar_volume(_con(BASE), top_n=2)
    assert list(top["symbol"]) == ["BIG", "MID"]


def test_membership_events_are_append_only_and_reconstructable():
    con = _con(BASE)
    top = rank_dollar_volume(con, top_n=2)
    assert update_membership(con, top) == (2, 0)
    assert update_membership(con, top) == (0, 0)          # idempotent
    assert current_members(con) == {"BIG", "MID"}

    # MID's volume dies; ZIP takes its seat. Exit is an event, not a delete.
    smaller = top[top["symbol"] == "BIG"].assign(rank=1)
    zip_row = pd.DataFrame([{"symbol": "ZIP", "dollar_volume": 1.0,
                             "rank": 2}])
    assert update_membership(con, pd.concat([smaller, zip_row])) == (1, 1)
    assert current_members(con) == {"BIG", "ZIP"}
    events = con.execute("SELECT COUNT(*) FROM universe_membership"
                         ).fetchone()[0]
    assert events == 4  # 3 enters + 1 exit, nothing rewritten


def test_line_of_sight_keeps_small_caps_and_tags_emerging():
    con = _con(BASE)
    update_membership(con, rank_dollar_volume(con, top_n=2))
    total, emerging = build_line_of_sight(con, watchlist=["MID"],
                                          held=["BIG"])
    assert total == 11                      # nothing filtered out
    df = con.execute("SELECT * FROM gold_line_of_sight").df()
    by = {r["symbol"]: r for _, r in df.iterrows()}

    assert bool(by["ZIP"]["emerging"]) is True    # small, momentum-surging
    assert bool(by["BIG"]["emerging"]) is False   # in the tier already
    # PNY has the TOP surge percentile yet stays out: the sub-dollar floor
    # is what excludes it, nothing else.
    assert by["PNY"]["mom_pct"] == df["mom_pct"].max()
    assert bool(by["PNY"]["emerging"]) is False
    assert emerging == 1
    assert bool(by["MID"]["is_watchlist"]) and bool(by["BIG"]["is_held"])
    assert bool(by["BIG"]["is_top_n"]) and not bool(by["ZIP"]["is_top_n"])


def test_emerging_is_empty_in_a_bear_market_not_a_permanent_decile():
    # Every stock declining: relative top-deciles still exist, but the
    # absolute confirmations (rising, volume building) empty the tag.
    bear = {sym: (start, start * 0.6, vol)
            for sym, (start, _, vol) in BASE.items()}
    con = _con(bear)
    update_membership(con, rank_dollar_volume(con, top_n=2))
    total, emerging = build_line_of_sight(con, watchlist=[], held=[])
    assert total == 11
    assert emerging == 0
    # ...and the sell-side mirror fires: everything is down 3m, 6m and 12m.
    falling = con.execute("SELECT COUNT(*) FROM gold_line_of_sight "
                          "WHERE deteriorating").fetchone()[0]
    assert falling == 11


def test_deteriorating_needs_all_three_horizons_down():
    # Flat names never deteriorate; a recent-recovery name (down on the
    # year, up over 3 months) does not either.
    con = duckdb.connect(":memory:")
    rows = []
    dates = pd.bdate_range(end="2026-08-10", periods=260)
    for i, date in enumerate(dates):
        rows.append(("VEE", date.date(),                 # V-shape: crashed,
                     10.0 - 6.0 * min(i, 200) / 200      # then recovering
                     + 3.0 * max(i - 200, 0) / 59, 50_000))
    con.execute("CREATE TABLE bronze_prices_broad (symbol VARCHAR, "
                "date DATE, close DOUBLE, volume BIGINT)")
    con.executemany("INSERT INTO bronze_prices_broad VALUES (?, ?, ?, ?)",
                    rows)
    build_line_of_sight(con, watchlist=[], held=[])
    row = con.execute("SELECT ret_3m > 0, ret_12m < 0, deteriorating "
                      "FROM gold_line_of_sight").fetchone()
    assert row == (True, True, False)


def test_line_of_sight_survives_symbols_with_short_history():
    specs = dict(BASE)
    con = _con(specs)
    # An IPO with 30 sessions: 12-1 momentum undefined, row still present.
    con.executemany(
        "INSERT INTO bronze_prices_broad VALUES (?, ?, ?, ?)",
        [("IPO", d.date(), 10.0, 50_000)
         for d in pd.bdate_range(end="2026-08-10", periods=30)])
    update_membership(con, rank_dollar_volume(con, top_n=2))
    total, _ = build_line_of_sight(con, watchlist=[], held=[])
    assert total == 12
    ipo = con.execute("SELECT mom_12_1, emerging FROM gold_line_of_sight "
                      "WHERE symbol = 'IPO'").fetchone()
    assert ipo[0] is None       # honest gap, not a fabricated number
