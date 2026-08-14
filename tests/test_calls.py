"""Call state machine, and the round scoring frame it runs on."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import duckdb
import pytest

from scoring.calls import (
    build_round, emit_round, latest_calls, load_gold_calls, next_call,
    read_rounds, round_scores, simulate_calls,
)
from scoring.tiers import load_registry

CFG = {"enter_percentile": 0.90, "exit_percentile": 0.50}


@pytest.mark.parametrize("prior,pct,expected", [
    # Out of position: enter only at the top decile.
    ("none", 0.90, "buy"),
    ("none", 0.8999, "none"),
    ("none", 0.0, "none"),
    ("sell", 0.90, "buy"),      # re-entry needs the full enter bar
    ("sell", 0.89, "none"),
    ("sell", 0.51, "none"),     # above exit is NOT enough to re-enter
    # In position: exit only below the exit line - the hysteresis band.
    ("buy", 1.0, "hold"),
    ("buy", 0.50, "hold"),      # criterion 5: no exit at or above 0.50
    ("buy", 0.4999, "sell"),
    ("hold", 0.89, "hold"),     # below enter but held: stay
    ("hold", 0.50, "hold"),
    ("hold", 0.4999, "sell"),
])
def test_every_transition(prior, pct, expected):
    assert next_call(prior, pct, CFG) == expected


def test_unknown_prior_and_bad_percentile_raise():
    with pytest.raises(ValueError):
        next_call("held", 0.9, CFG)
    with pytest.raises(ValueError):
        next_call("none", None, CFG)
    with pytest.raises(ValueError):
        next_call("none", 1.5, CFG)


def test_shipped_registry_config_drives_the_machine():
    cfg = load_registry()["calls"]
    assert next_call("none", 0.90, cfg) == "buy"
    assert next_call("hold", 0.50, cfg) == "hold"
    assert next_call("hold", 0.49, cfg) == "sell"


DATES = ("2026-06-30", "2026-07-31")


def _warehouse():
    """Twelve eligible symbols with deterministic ordering, plus one of each
    excluded kind: a benchmark (SPY), a commodity trust (SLV, SIC 6221) and
    a plain ETF (VOO, quoteType)."""
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE silver_signals (symbol VARCHAR, "
                "as_of_date TIMESTAMP, change_30d_pct DOUBLE, "
                "change_365d_pct DOUBLE)")
    con.execute("CREATE TABLE gold_candidate_signals (symbol VARCHAR, "
                "as_of_date TIMESTAMP, earnings_yield DOUBLE, "
                "gross_profitability DOUBLE, roe DOUBLE)")
    con.execute("CREATE TABLE bronze_entity (symbol VARCHAR, sic VARCHAR)")
    con.execute("CREATE TABLE bronze_security (symbol VARCHAR, "
                "quote_type VARCHAR)")
    symbols = [f"S{i:02d}" for i in range(12)] + ["SPY", "SLV", "VOO"]
    for date in DATES:
        for i, symbol in enumerate(symbols):
            con.execute(
                "INSERT INTO silver_signals VALUES (?, ?, 0.0, ?)",
                [symbol, date, i * 10.0])
            con.execute(
                "INSERT INTO gold_candidate_signals VALUES (?, ?, ?, 0.1, 0.1)",
                [symbol, date, i * 0.01])
    con.execute("INSERT INTO bronze_entity VALUES ('SLV', '6221'), "
                "('S00', '3674')")   # an operating SIC must NOT exclude
    con.execute("INSERT INTO bronze_security VALUES ('VOO', 'ETF'), "
                "('S01', 'EQUITY'), ('S02', 'UNKNOWN')")
    return con


def test_round_scores_excludes_yardsticks_not_equities():
    df = round_scores(_warehouse(), load_registry(), [DATES[0]])
    symbols = set(df["symbol"])
    assert symbols == {f"S{i:02d}" for i in range(12)}  # SPY, SLV, VOO gone


def test_score_percentile_direction_best_reads_one():
    # Gotcha 0: PERCENT_RANK direction is settled by executing, never argued.
    df = round_scores(_warehouse(), load_registry(), [DATES[0]])
    best = df.loc[df["symbol"] == "S11"].iloc[0]
    worst = df.loc[df["symbol"] == "S00"].iloc[0]
    assert best["composite_rank"] == 1
    assert best["score_percentile"] == 1.0
    assert worst["score_percentile"] == 0.0
    assert df["score_percentile"].between(0.0, 1.0).all()


def test_round_scores_ranks_each_date_independently():
    df = round_scores(_warehouse(), load_registry(), list(DATES))
    assert len(df) == 24  # 12 eligible symbols x 2 dates
    per_date = df.groupby("as_of_date")["composite_rank"].min()
    assert (per_date == 1).all()


def test_missing_exclusion_table_fails_loudly():
    con = _warehouse()
    con.execute("DROP TABLE bronze_security")
    with pytest.raises(Exception):
        round_scores(con, load_registry(), [DATES[0]])


def test_simulate_carries_state_and_skips_absent_symbols():
    con = _warehouse()
    # S11 disappears on the second date (delisted): keeps no row, and its
    # state is simply never advanced - a gap, not a fabricated call.
    con.execute("DELETE FROM silver_signals WHERE symbol = 'S11' "
                "AND as_of_date = ?", [DATES[1]])
    scores = round_scores(con, load_registry(), list(DATES))
    calls = simulate_calls(scores, load_registry()["calls"])

    first = calls[calls["as_of_date"] == calls["as_of_date"].min()]
    second = calls[calls["as_of_date"] == calls["as_of_date"].max()]
    assert set(first.loc[first["call"] == "buy", "symbol"]) == {"S10", "S11"}
    assert "S11" not in set(second["symbol"])
    # With S11 gone, S10 tops the 11 remaining (pct 1.0) and stays held;
    # S09 rises to pct 0.9 and enters from none.
    by_symbol = dict(zip(second["symbol"], second["call"]))
    assert by_symbol["S10"] == "hold"
    assert by_symbol["S09"] == "buy"
    assert second.loc[second["symbol"] == "S10", "prior_call"].iloc[0] == "buy"


EXPECTATION = {"source_sha256": "deadbeef", "haircut": 0.5, "horizons": {}}


def _round(date, prior_calls=None):
    scores = round_scores(_warehouse(), load_registry(), [date])
    return build_round(scores, prior_calls or {}, load_registry(),
                       EXPECTATION, "run-1", "2026-08-11T12:00:00Z")


def test_build_round_applies_the_machine_from_none():
    entry = _round(DATES[0])
    calls = {c["symbol"]: c["call"] for c in entry["calls"]}
    # 12 symbols from `none`: only the top decile (percentile >= 0.90)
    # enters, which with PERCENT_RANK over 12 is exactly S11 (1.0) and
    # S10 (10/11 = 0.909).
    assert calls["S11"] == "buy" and calls["S10"] == "buy"
    assert all(c == "none" for s, c in calls.items()
               if s not in ("S10", "S11"))
    assert all(c["prior_call"] == "none" for c in entry["calls"])
    assert entry["expectation"]["source_sha256"] == "deadbeef"


def test_build_round_carries_prior_state_through_hysteresis():
    # Percentile is (rank-1)/11 here: S06 = 0.545 (above exit, below enter),
    # S05 = 0.455 (below exit).
    prior = {"S06": "buy", "S05": "buy", "S00": "hold", "S11": "sell"}
    calls = {c["symbol"]: c["call"] for c in _round(DATES[0], prior)["calls"]}
    assert calls["S06"] == "hold"   # in the hysteresis band and held: stay
    assert calls["S05"] == "sell"   # below the exit line: out
    assert calls["S00"] == "sell"   # worst percentile: out
    assert calls["S11"] == "buy"    # sold before, back at the top: re-enter


def test_emit_round_is_first_write_wins(tmp_path):
    path = str(tmp_path / "calls_log.jsonl")
    entry = _round(DATES[0])
    assert emit_round(entry, path=path) is True
    # Re-run of the same round, even with different content: no-op.
    changed = dict(entry, run_id="run-2")
    assert emit_round(changed, path=path) is False
    rounds = read_rounds(path)
    assert len(rounds) == 1 and rounds[0]["run_id"] == "run-1"
    # A later round appends; latest_calls reads the newest state.
    assert emit_round(_round(DATES[1]), path=path) is True
    assert latest_calls(read_rounds(path))["S11"] == "buy"


def test_latest_calls_carries_state_across_a_round_gap():
    # A symbol absent from one round (Yahoo hole, tier churn) keeps its
    # state instead of resetting to none - the live path must match
    # simulate_calls, or a held name re-enters as a fresh buy with no
    # sell ever recorded.
    rounds = [
        {"calls": [{"symbol": "A", "call": "buy"},
                   {"symbol": "B", "call": "none"}]},
        {"calls": [{"symbol": "B", "call": "none"}]},  # A missing
    ]
    state = latest_calls(rounds)
    assert state["A"] == "buy" and state["B"] == "none"
    assert latest_calls([]) == {}


def test_gold_calls_rebuilds_identically_from_the_log(tmp_path):
    path = str(tmp_path / "calls_log.jsonl")
    emit_round(_round(DATES[0]), path=path)
    emit_round(_round(DATES[1], {"S11": "buy"}), path=path)

    con = duckdb.connect(":memory:")
    assert load_gold_calls(con, path=path) == 24
    first = con.execute("SELECT * FROM gold_calls ORDER BY as_of_date, "
                        "symbol").df()
    # The warehouse dies; the record does not.
    con.execute("DROP TABLE gold_calls")
    load_gold_calls(con, path=path)
    rebuilt = con.execute("SELECT * FROM gold_calls ORDER BY as_of_date, "
                          "symbol").df()
    import pandas as pd
    pd.testing.assert_frame_equal(first, rebuilt)
    row = con.execute("SELECT call, prior_call, expectation_source FROM "
                      "gold_calls WHERE symbol = 'S11' AND as_of_date = ?",
                      [DATES[1]]).fetchone()
    assert row == ("hold", "buy", "deadbeef")
