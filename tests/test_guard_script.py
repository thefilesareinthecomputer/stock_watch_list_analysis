"""scripts/guard.py end to end over an in-memory warehouse fixture.

Fixtures only; the real positions and warehouse are never read here.
"""
import os
import sys

ROOT = os.path.join(os.path.dirname(__file__), "..")
sys.path.insert(0, os.path.join(ROOT, "src"))
sys.path.insert(0, os.path.join(ROOT, "scripts"))

import duckdb
import pandas as pd

from guard import build_guard, replay

DATES = pd.bdate_range("2025-09-01", periods=300)


def _con():
    """SINK falls 30% over its last 21 sessions (300 shares of ~$80);
    STEADY and the SPY benchmark stay flat; FUNDX is a big fund."""
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE silver_adjusted_prices (symbol VARCHAR, "
                "date DATE, adj_open DOUBLE, adj_close DOUBLE)")
    paths = {
        # Small daily swings (high stays 80.0) so speed has a yardstick.
        "SINK": [80.0 - 0.4 * (i % 2) for i in range(279)]
                + [80.0 * (0.7 ** ((i + 1) / 21)) for i in range(21)],
        "STEADY": [50.0] * 300,
        "FUNDX": [100.0] * 300,
        "SPY": [500.0] * 300,
    }
    for sym, closes in paths.items():
        for d, c in zip(DATES, closes):
            con.execute("INSERT INTO silver_adjusted_prices VALUES "
                        "(?, ?, ?, ?)", [sym, d.date(), c, c])
    con.execute("CREATE TABLE bronze_security (symbol VARCHAR, "
                "quote_type VARCHAR)")
    con.execute("INSERT INTO bronze_security VALUES ('SINK', 'EQUITY'), "
                "('STEADY', 'EQUITY'), ('FUNDX', 'ETF')")
    con.execute("CREATE TABLE bronze_entity (symbol VARCHAR, "
                "sic_description VARCHAR)")
    con.execute("INSERT INTO bronze_entity VALUES "
                "('SINK', 'Semiconductors'), ('STEADY', 'Utilities')")
    return con


POSITIONS = [
    {"account": "ROTH", "symbol": "SINK", "quantity": 300.0},
    {"account": "ROTH", "symbol": "STEADY", "quantity": 20.0},
    {"account": "ROTH", "symbol": "FUNDX", "quantity": 2000.0},
]
TODAY = str(DATES[-1].date())


def test_report_names_the_alert_with_its_why_and_sector_exposure():
    report, events, firing = build_guard(_con(), POSITIONS, [], TODAY)
    assert firing == [("SINK", "drawdown")]
    assert "SINK" in report.split("## Needs attention")[1].split("##")[0]
    assert "vs market" in report and "sigma" in report
    exposure = report.split("## Exposure by sector")[1]
    assert "Funds" in exposure and "Semiconductors" in exposure
    assert "sell" not in report.lower()       # informs, never instructs


def test_a_stale_holding_shows_only_the_broker_check_not_old_numbers():
    con = _con()
    con.execute("INSERT INTO silver_adjusted_prices "
                "SELECT 'DEADX', date, adj_open, adj_close "
                "FROM silver_adjusted_prices WHERE symbol = 'SINK' "
                "AND date < ?", [str(DATES[-5].date())])
    report, _, _ = build_guard(con, POSITIONS + [
        {"account": "ROTH", "symbol": "DEADX", "quantity": 10.0}], [], TODAY)
    line = [ln for ln in report.splitlines() if ln.startswith("- DEADX")][0]
    assert "check the broker" in line
    assert "21-day move" not in line and "past the line" not in line


def test_second_run_in_the_same_episode_is_quiet():
    _, events, _ = build_guard(_con(), POSITIONS, [], TODAY)
    report, events2, firing2 = build_guard(_con(), POSITIONS, events, TODAY)
    assert firing2 == [] and events2 == []
    assert "no new alerts" in report


def test_replay_names_the_first_day_the_line_was_crossed():
    fires = replay(_con(), POSITIONS, str(DATES[250].date()), TODAY)
    (sym, alert, day), = [f for f in fires if f[1] == "drawdown"]
    assert sym == "SINK"
    # 0.7 ** (k/21) <= 0.8 first at k = 14 of the 21 falling sessions.
    assert day == str(DATES[279 + 13].date())
