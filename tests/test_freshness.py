"""Tests for the tiered freshness gate.

A dead ticker contributes no rows and the job stays green - that is the failure
mode this gate exists to end. The tiering is equally deliberate: a gate that
goes red every time one ticker hiccups gets disabled, and a disabled gate
protects nothing.
"""
import sys
import os
from datetime import date

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import pytest

from common.quality import STALE_TOLERANCE, check_freshness, last_trading_session

SESSION = date(2026, 3, 2)
FRESH = "2026-03-02"
STALE = "2026-02-27"


def _universe(n, first=0):
    return {f"SYM{i}": FRESH for i in range(first, first + n)}


def test_all_fresh_passes_clean():
    errors, warnings = check_freshness(_universe(50), SESSION, critical={"SYM0"})
    assert errors == []
    assert warnings == []


def test_stale_critical_symbol_is_an_error():
    data = _universe(50)
    data["SYM0"] = STALE
    errors, _ = check_freshness(data, SESSION, critical={"SYM0"})
    assert len(errors) == 1
    assert "SYM0" in errors[0]


def test_missing_critical_symbol_is_an_error():
    """A symbol that produced no rows at all is the silent case - it must not
    pass simply by being absent from the input."""
    data = _universe(50)
    del data["SYM0"]
    errors, _ = check_freshness(data, SESSION, critical={"SYM0"})
    assert errors and "SYM0" in errors[0]


def test_one_stale_non_critical_symbol_only_warns():
    """Single-ticker flakiness must not fail the run, or the gate gets turned
    off and stops protecting the symbols that matter."""
    data = _universe(100)
    data["SYM50"] = STALE
    errors, warnings = check_freshness(data, SESSION, critical={"SYM0"})
    assert errors == []
    assert len(warnings) == 1
    assert "SYM50" in warnings[0]


def test_widespread_staleness_is_an_error():
    """Many stale symbols means the source broke, not that one ticker died."""
    data = _universe(100)
    for i in range(50, 60):
        data[f"SYM{i}"] = STALE
    errors, _ = check_freshness(data, SESSION, critical={"SYM0"})
    assert errors and "Stale rate above" in errors[0]


def test_tolerance_boundary_is_exclusive():
    """Exactly at tolerance passes; above it fails."""
    data = _universe(101)
    critical = {"SYM100"}
    at_limit = int(100 * STALE_TOLERANCE)
    for i in range(at_limit):
        data[f"SYM{i}"] = STALE
    assert check_freshness(data, SESSION, critical)[0] == []

    data[f"SYM{at_limit}"] = STALE
    assert check_freshness(data, SESSION, critical)[0] != []


def test_stale_symbols_are_always_named_never_dropped():
    data = _universe(100)
    data["SYM77"] = STALE
    errors, warnings = check_freshness(data, SESSION, critical={"SYM0"})
    assert "SYM77" in (errors + warnings)[0]


def test_last_trading_session_skips_weekends():
    # 2026-03-08 is a Sunday; the prior session is Friday the 6th.
    assert last_trading_session(date(2026, 3, 8)) == date(2026, 3, 6)


def test_last_trading_session_returns_same_day_when_open():
    result = last_trading_session(date(2026, 3, 6))
    assert result == date(2026, 3, 6)


def test_last_trading_session_skips_holidays():
    # Christmas Day 2026 falls on a Friday; the prior session is the 24th.
    assert last_trading_session(date(2026, 12, 25)) == date(2026, 12, 24)
