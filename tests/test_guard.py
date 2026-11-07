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

from common.guard import DEFAULT_LIMITS, evaluate_holdings

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


def test_default_limits_are_the_proposed_risk_memo_values():
    assert DEFAULT_LIMITS == {
        "drawdown": 0.20, "dollar_floor": 1000.0, "portfolio_floor": 0.01,
        "single_stock_cap": 0.10, "window": 126, "stale_sessions": 2}
