"""
Unit tests for indicator calculations.
Run locally with: pytest tests/ -v
No Spark or Databricks required.
"""
import numpy as np
import pandas as pd
import pytest
from src.common.indicators import (
    calculate_rsi,
    calculate_macd,
    calculate_bollinger_bands,
    calculate_change,
    calculate_atr,
    calculate_moving_average,
    calculate_volume_trend,
    calculate_eps,
    calculate_pe_ratio,
    calculate_dividend_yield,
    determine_signal,
    build_signal_row,
    safe_float,
)


@pytest.fixture
def sample_close():
    """200 days of synthetic price data with upward trend + noise."""
    np.random.seed(42)
    return pd.Series(np.cumsum(np.random.randn(200)) + 100)


@pytest.fixture
def sample_ohlcv():
    """200 days of synthetic OHLCV data with lowercase columns."""
    np.random.seed(42)
    n = 200
    close = np.cumsum(np.random.randn(n)) + 100
    df = pd.DataFrame({
        "open": close + np.random.randn(n) * 0.5,
        "high": close + np.abs(np.random.randn(n)),
        "low": close - np.abs(np.random.randn(n)),
        "close": close,
        "volume": np.random.randint(1_000_000, 10_000_000, n),
    })
    df.index = pd.date_range("2024-01-01", periods=n)
    return df


class TestSafeFloat:
    def test_none(self):
        assert np.isnan(safe_float(None))

    def test_string_number(self):
        assert safe_float("42.5") == 42.5

    def test_string_garbage(self):
        assert np.isnan(safe_float("not a number"))

    def test_inf(self):
        assert np.isnan(safe_float(float("inf")))

    def test_int(self):
        assert safe_float(42) == 42.0


class TestRSI:
    def test_rsi_bounds(self, sample_close):
        rsi = calculate_rsi(sample_close)
        assert 0 <= rsi <= 100

    def test_rsi_with_custom_window(self, sample_close):
        rsi = calculate_rsi(sample_close, window=21)
        assert 0 <= rsi <= 100

    def test_rsi_insufficient_data(self):
        short = pd.Series([100, 101, 102])
        assert np.isnan(calculate_rsi(short))

    def test_rsi_all_gains(self):
        data = pd.Series(range(100, 200))
        rsi = calculate_rsi(data)
        assert rsi > 90

    def test_rsi_all_losses(self):
        data = pd.Series(range(200, 100, -1))
        rsi = calculate_rsi(data)
        assert rsi < 10


class TestMACD:
    def test_returns_two_floats(self, sample_close):
        macd, signal = calculate_macd(sample_close)
        assert isinstance(macd, float)
        assert isinstance(signal, float)

    def test_insufficient_data(self):
        short = pd.Series([100, 101, 102])
        macd, signal = calculate_macd(short)
        assert np.isnan(macd)
        assert np.isnan(signal)


class TestBollingerBands:
    def test_upper_above_lower(self, sample_close):
        upper, lower = calculate_bollinger_bands(sample_close)
        assert upper > lower

    def test_insufficient_data(self):
        short = pd.Series([100, 101])
        upper, lower = calculate_bollinger_bands(short)
        assert np.isnan(upper)


class TestATR:
    def test_atr_positive(self, sample_ohlcv):
        atr = calculate_atr(sample_ohlcv, 14)
        assert atr > 0

    def test_atr_insufficient_data(self, sample_ohlcv):
        short = sample_ohlcv.head(3)
        atr = calculate_atr(short, 14)
        assert np.isnan(atr)


class TestChange:
    def test_basic(self, sample_close):
        abs_chg, pct_chg = calculate_change(sample_close, 5)
        assert not np.isnan(abs_chg)
        assert not np.isnan(pct_chg)

    def test_insufficient_data(self):
        short = pd.Series([100, 101])
        abs_chg, pct_chg = calculate_change(short, 10)
        assert np.isnan(abs_chg)

    def test_zero_start_price(self):
        data = pd.Series([0, 100])
        abs_chg, pct_chg = calculate_change(data, 2)
        assert np.isnan(pct_chg)


class TestFundamentals:
    def test_eps_trailing(self):
        info = {"trailingEps": 5.89}
        assert calculate_eps(info) == 5.89

    def test_eps_fallback(self):
        info = {"netIncomeToCommon": 1_000_000, "sharesOutstanding": 100_000}
        assert calculate_eps(info) == 10.0

    def test_eps_missing(self):
        assert np.isnan(calculate_eps({}))

    def test_eps_string_values(self):
        info = {"trailingEps": "5.89"}
        assert calculate_eps(info) == 5.89

    def test_pe_trailing(self):
        info = {"trailingPE": 25.3}
        assert calculate_pe_ratio(info, 200.0) == 25.3

    def test_pe_fallback(self):
        info = {"trailingEps": 10.0}
        pe = calculate_pe_ratio(info, 200.0)
        assert pe == 20.0

    def test_dividend_yield_zero_price(self):
        assert calculate_dividend_yield({"dividendRate": 2.0}, 0) == 0.0


class TestSignal:
    def test_buy(self):
        assert determine_signal(1.5, 1.0, 40) == "Buy"

    def test_sell(self):
        assert determine_signal(0.5, 1.0, 60) == "Sell"

    def test_hold(self):
        assert determine_signal(1.5, 1.0, 80) == "Hold"

    def test_nan_inputs(self):
        assert determine_signal(np.nan, 1.0, 40) == "Insufficient Data"


class TestBuildSignalRow:
    def test_returns_consistent_keys(self, sample_ohlcv):
        row1 = build_signal_row(sample_ohlcv, "TEST1", {})
        row2 = build_signal_row(sample_ohlcv.head(60), "TEST2", {})
        assert set(row1.keys()) == set(row2.keys()), \
            f"Key mismatch: {set(row1.keys()).symmetric_difference(set(row2.keys()))}"

    def test_symbol_preserved(self, sample_ohlcv):
        row = build_signal_row(sample_ohlcv, "AAPL", {})
        assert row["symbol"] == "AAPL"

    def test_all_values_serializable(self, sample_ohlcv):
        row = build_signal_row(sample_ohlcv, "TEST", {})
        for key, value in row.items():
            assert isinstance(value, (str, int, float, type(None), np.floating, np.integer)), \
                f"Key '{key}' has unexpected type: {type(value)}"
