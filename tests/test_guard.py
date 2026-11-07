"""Portfolio guard evaluation (tasks/SPEC-GUARD.md): which holdings need the
user's attention. Fixtures only - real positions are private.

The guard informs; it never says sell. These tests pin the gate that keeps
a deep drop on a few cheap shares from ever reaching the user.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import pandas as pd
import pytest

from common.guard import (
    DEFAULT_LIMITS, alert_factors, append_events, evaluate_holdings,
    fold_state, load_events, next_events,
)

DATES = pd.bdate_range("2026-01-01", periods=140)


def _px(**paths):
    """Adjusted closes: each kwarg is a symbol -> list of closes aligned to
    the END of DATES (shorter lists start later)."""
    frame = pd.DataFrame(index=DATES)
    for sym, closes in paths.items():
        series = pd.Series(closes, index=DATES[-len(closes):])
        frame[sym] = series
    return frame


def _flat_then(start, end, n=140):
    """Flat at `start`, then the last close is `end`."""
    return [start] * (n - 1) + [end]


def _by_symbol(rows):
    return {r["symbol"]: r for r in rows}


def test_deep_drop_on_a_small_cheap_position_never_alerts():
    # 40% down, but 10 shares of a $5 stock lost $20: below every floor.
    px = _px(CHEAP=_flat_then(5.0, 3.0), BIG=_flat_then(100.0, 100.0))
    rows = evaluate_holdings(px, {"CHEAP": 10, "BIG": 500},
                             {"CHEAP": "EQUITY", "BIG": "ETF"})
    assert _by_symbol(rows)["CHEAP"]["alerts"] == []


def test_large_dollar_drop_past_the_line_alerts():
    # 25% off the high on 100 shares of $80 -> $2,000 lost.
    px = _px(LARGE=_flat_then(80.0, 60.0), FUND=_flat_then(100.0, 100.0))
    rows = evaluate_holdings(px, {"LARGE": 100, "FUND": 1000},
                             {"LARGE": "EQUITY", "FUND": "ETF"})
    large = _by_symbol(rows)["LARGE"]
    assert large["drawdown"] == pytest.approx(-0.25)
    assert large["lost_from_high"] == pytest.approx(2000.0)
    assert "drawdown" in large["alerts"]


def test_drop_short_of_the_line_does_not_alert_however_large():
    px = _px(LARGE=_flat_then(100.0, 85.0), FUND=_flat_then(100.0, 100.0))
    rows = evaluate_holdings(px, {"LARGE": 1000, "FUND": 1000},
                             {"LARGE": "EQUITY", "FUND": "ETF"})
    assert "drawdown" not in _by_symbol(rows)["LARGE"]["alerts"]


def test_high_older_than_the_window_is_ignored():
    # A spike 135 sessions ago is outside the 126-session window.
    closes = [200.0] + [100.0] * 138 + [100.0]
    px = _px(OLDPEAK=closes, FUND=_flat_then(100.0, 100.0))
    rows = evaluate_holdings(px, {"OLDPEAK": 100, "FUND": 1000},
                             {"OLDPEAK": "EQUITY", "FUND": "ETF"})
    assert _by_symbol(rows)["OLDPEAK"]["drawdown"] == pytest.approx(0.0)


def test_single_stock_over_the_size_cap_alerts_but_a_fund_does_not():
    px = _px(STOCK=_flat_then(100.0, 100.0), FUND=_flat_then(100.0, 100.0),
             OTHER=_flat_then(100.0, 100.0))
    # STOCK 20%, FUND 60%, OTHER 20% of the book.
    rows = _by_symbol(evaluate_holdings(
        px, {"STOCK": 10, "FUND": 30, "OTHER": 10},
        {"STOCK": "EQUITY", "FUND": "ETF", "OTHER": "EQUITY"}))
    assert "size" in rows["STOCK"]["alerts"]
    assert rows["STOCK"]["weight"] == pytest.approx(0.2)
    assert "size" not in rows["FUND"]["alerts"]


def test_holding_with_no_recent_prices_is_flagged_stale():
    closes = [50.0] * 137   # stops 3 sessions before the latest date
    px = _px(LIVE=_flat_then(100.0, 100.0))
    px["DEAD"] = pd.Series(closes, index=DATES[:137])
    rows = _by_symbol(evaluate_holdings(
        px, {"LIVE": 10, "DEAD": 10, "GONE": 5},
        {"LIVE": "ETF", "DEAD": "EQUITY", "GONE": "EQUITY"}))
    assert rows["DEAD"]["alerts"] == ["stale"]
    assert rows["GONE"]["alerts"] == ["stale"]   # no prices at all
    assert rows["LIVE"]["alerts"] == []


def _noisy(n=300, drift=0.0, last_drop=None, seed=1):
    """A random-walk close series; optionally the last 21 sessions fall by
    `last_drop` in total."""
    rng = pd.Series(range(n)).sample(frac=1, random_state=seed).values
    rets = pd.Series(((rng % 7) - 3) / 1000.0 + drift)  # +-0.3% daily
    if last_drop is not None:
        rets.iloc[-21:] = (1 + last_drop) ** (1 / 21) - 1
    return pd.Series((1 + rets).cumprod().values * 100.0,
                     index=pd.bdate_range("2025-06-02", periods=n))


def test_speed_measures_the_month_against_the_stocks_own_swings():
    calm = alert_factors(_noisy(), _noisy(seed=2))
    crash = alert_factors(_noisy(last_drop=-0.30), _noisy(seed=2))
    assert abs(calm["speed_sigma"]) < 2
    assert crash["speed_sigma"] < -5     # a -30% month in a 0.3%-a-day name


def test_move_versus_market_separates_stock_from_market():
    stock = _noisy(last_drop=-0.20)
    market = _noisy(seed=2, last_drop=-0.05)
    f = alert_factors(stock, market)
    assert f["vs_market_21d"] == pytest.approx(-0.20 - (-0.05), abs=0.01)


def test_confirmed_needs_two_closes_past_the_line():
    base = [100.0] * 130
    once = pd.Series(base + [100.0, 75.0],
                     index=pd.bdate_range("2026-01-01", periods=132))
    twice = pd.Series(base + [78.0, 75.0],
                      index=pd.bdate_range("2026-01-01", periods=132))
    spy = pd.Series([100.0] * 132, index=once.index)
    assert alert_factors(once, spy)["confirmed"] is False
    assert alert_factors(twice, spy)["confirmed"] is True


def _row(symbol="AAA", alerts=(), drawdown=0.0, weight=0.05):
    return {"symbol": symbol, "alerts": list(alerts), "drawdown": drawdown,
            "weight": weight}


def test_an_alert_fires_once_per_episode():
    events, fired = next_events([_row(alerts=["drawdown"], drawdown=-0.25)],
                                fold_state([]), "2026-10-13")
    assert fired == [("AAA", "drawdown")]
    state = fold_state(events)
    events2, fired2 = next_events([_row(alerts=["drawdown"], drawdown=-0.30)],
                                  state, "2026-10-14")
    assert fired2 == [] and events2 == []   # still the same episode


def test_drawdown_rearms_only_after_recovering_half_the_line():
    state = fold_state([{"date": "2026-10-13", "symbol": "AAA",
                         "alert": "drawdown", "event": "fired"}])
    # Back to -12%: under the 20% line but not recovered past -10%.
    events, _ = next_events([_row(drawdown=-0.12)], state, "2026-10-20")
    assert events == []
    events, _ = next_events([_row(drawdown=-0.08)], state, "2026-10-27")
    assert events == [{"date": "2026-10-27", "symbol": "AAA",
                       "alert": "drawdown", "event": "rearmed"}]
    _, fired = next_events([_row(alerts=["drawdown"], drawdown=-0.21)],
                           fold_state(events), "2026-11-02")
    assert fired == [("AAA", "drawdown")]   # a new episode fires again


def test_size_rearms_below_ninety_percent_of_the_cap():
    state = fold_state([{"date": "2026-10-13", "symbol": "AAA",
                         "alert": "size", "event": "fired"}])
    assert next_events([_row(weight=0.095)], state, "2026-10-14")[0] == []
    events, _ = next_events([_row(weight=0.085)], state, "2026-10-15")
    assert events[0]["event"] == "rearmed"


def test_event_log_is_append_only_jsonl(tmp_path):
    path = tmp_path / "guard_events.jsonl"
    first = [{"date": "2026-10-13", "symbol": "AAA", "alert": "drawdown",
              "event": "fired"}]
    append_events(path, first)
    append_events(path, [{**first[0], "date": "2026-10-27",
                          "event": "rearmed"}])
    assert [e["event"] for e in load_events(path)] == ["fired", "rearmed"]
    assert load_events(tmp_path / "missing.jsonl") == []


def test_default_limits_are_the_proposed_risk_memo_values():
    assert DEFAULT_LIMITS == {
        "drawdown": 0.20, "dollar_floor": 1000.0, "portfolio_floor": 0.01,
        "single_stock_cap": 0.10, "window": 126, "stale_sessions": 2}
