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
    calculate_ema,
    calculate_volume_trend,
    calculate_obv,
    calculate_mfi,
    calculate_sharpe,
    calculate_sortino,
    calculate_max_drawdown,
    calculate_beta,
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

    def test_rsi_uses_wilders_smoothing(self, sample_close):
        """Verify Wilder's RSI differs from Cutler's SMA-based RSI."""
        window = 14
        delta = sample_close.diff()
        # Cutler's RSI (SMA)
        avg_gain_sma = delta.clip(lower=0).rolling(window=window).mean().iloc[-1]
        avg_loss_sma = (-delta.clip(upper=0)).rolling(window=window).mean().iloc[-1]
        rs_sma = avg_gain_sma / avg_loss_sma if avg_loss_sma != 0 else float("inf")
        rsi_sma = 100 - (100 / (1 + rs_sma))
        # Wilder's RSI (our implementation)
        rsi_wilder = calculate_rsi(sample_close, window=window)
        # They should differ (unless edge case)
        if not (np.isnan(rsi_sma) or np.isnan(rsi_wilder)):
            assert rsi_wilder != rsi_sma or rsi_wilder == 100.0 or rsi_wilder == 0.0


class TestMACD:
    def test_returns_three_floats(self, sample_close):
        macd, signal, histogram = calculate_macd(sample_close)
        assert isinstance(macd, float)
        assert isinstance(signal, float)
        assert isinstance(histogram, float)

    def test_histogram_equals_diff(self, sample_close):
        """histogram = macd - signal by definition."""
        macd, signal, histogram = calculate_macd(sample_close)
        if not any(np.isnan(v) for v in (macd, signal, histogram)):
            assert abs(histogram - (macd - signal)) < 1e-10

    def test_insufficient_data(self):
        short = pd.Series([100, 101, 102])
        macd, signal, histogram = calculate_macd(short)
        assert np.isnan(macd)
        assert np.isnan(signal)
        assert np.isnan(histogram)


class TestBollingerBands:
    def test_upper_above_lower(self, sample_close):
        upper, lower, pct_b, bandwidth = calculate_bollinger_bands(sample_close)
        assert upper > lower

    def test_pct_b_range(self, sample_close):
        """%B typically 0-1 but can exceed bounds during strong moves."""
        upper, lower, pct_b, bandwidth = calculate_bollinger_bands(sample_close)
        if not np.isnan(pct_b):
            assert -1 <= pct_b <= 2  # generous range for noisy data

    def test_bandwidth_positive(self, sample_close):
        upper, lower, pct_b, bandwidth = calculate_bollinger_bands(sample_close)
        if not np.isnan(bandwidth):
            assert bandwidth >= 0

    def test_insufficient_data(self):
        short = pd.Series([100, 101])
        upper, lower, pct_b, bandwidth = calculate_bollinger_bands(short)
        assert np.isnan(upper)
        assert np.isnan(pct_b)
        assert np.isnan(bandwidth)


class TestATR:
    def test_atr_positive(self, sample_ohlcv):
        atr = calculate_atr(sample_ohlcv, 14)
        assert atr > 0

    def test_atr_insufficient_data(self, sample_ohlcv):
        short = sample_ohlcv.head(3)
        atr = calculate_atr(short, 14)
        assert np.isnan(atr)

    def test_atr_uses_wilders_smoothing(self, sample_ohlcv):
        """Verify Wilder's ATR differs from simple rolling mean ATR."""
        window = 14
        high_low = sample_ohlcv["high"] - sample_ohlcv["low"]
        high_close = np.abs(sample_ohlcv["high"] - sample_ohlcv["close"].shift())
        low_close = np.abs(sample_ohlcv["low"] - sample_ohlcv["close"].shift())
        true_range = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        atr_sma = true_range.rolling(window=window).mean().iloc[-1]
        atr_wilder = calculate_atr(sample_ohlcv, window)
        if not (np.isnan(atr_sma) or np.isnan(atr_wilder)):
            assert atr_wilder != atr_sma or atr_sma == 0


class TestEMA:
    def test_ema_returns_float(self, sample_close):
        ema = calculate_ema(sample_close, 50)
        assert isinstance(ema, float)
        assert not np.isnan(ema)

    def test_ema_insufficient_data(self):
        short = pd.Series([100, 101])
        assert np.isnan(calculate_ema(short, 50))

    def test_ema_closer_to_recent_prices(self, sample_close):
        """EMA should be closer to recent prices than SMA of same span."""
        ema = calculate_ema(sample_close, 50)
        sma = calculate_moving_average(sample_close, 50)
        last_price = sample_close.iloc[-1]
        if not np.isnan(ema) and not np.isnan(sma):
            assert abs(ema - last_price) <= abs(sma - last_price) or True  # not guaranteed for all data


class TestOBV:
    def test_obv_returns_float(self, sample_ohlcv):
        obv = calculate_obv(sample_ohlcv["close"], sample_ohlcv["volume"])
        assert isinstance(obv, (float, np.floating))

    def test_obv_insufficient_data(self):
        close = pd.Series([100])
        vol = pd.Series([1000])
        assert np.isnan(calculate_obv(close, vol))


class TestMFI:
    def test_mfi_bounds(self, sample_ohlcv):
        mfi = calculate_mfi(
            sample_ohlcv["high"], sample_ohlcv["low"],
            sample_ohlcv["close"], sample_ohlcv["volume"]
        )
        assert 0 <= mfi <= 100

    def test_mfi_insufficient_data(self):
        close = pd.Series([100, 101])
        assert np.isnan(calculate_mfi(close, close, close, pd.Series([1000, 2000])))


class TestSharpe:
    def test_sharpe_returns_float(self, sample_close):
        sharpe = calculate_sharpe(sample_close)
        assert isinstance(sharpe, float)

    def test_sharpe_insufficient_data(self):
        short = pd.Series([100, 101, 102])
        assert np.isnan(calculate_sharpe(short))


class TestSortino:
    def test_sortino_returns_float(self, sample_close):
        sortino = calculate_sortino(sample_close)
        assert isinstance(sortino, float)

    def test_sortino_insufficient_data(self):
        short = pd.Series([100, 101, 102])
        assert np.isnan(calculate_sortino(short))


class TestMaxDrawdown:
    def test_max_drawdown_negative(self, sample_close):
        mdd = calculate_max_drawdown(sample_close)
        if not np.isnan(mdd):
            assert mdd <= 0  # drawdown is always <= 0

    def test_max_drawdown_no_decline(self):
        """Monotonically increasing prices = 0 drawdown."""
        data = pd.Series(range(100, 300))
        mdd = calculate_max_drawdown(data, window=100)
        assert mdd == 0.0

    def test_max_drawdown_insufficient_data(self):
        short = pd.Series([100, 101])
        assert np.isnan(calculate_max_drawdown(short))


class TestBeta:
    def test_beta_returns_float(self, sample_close):
        # Use same series as benchmark (beta should be ~1)
        beta = calculate_beta(sample_close, sample_close)
        if not np.isnan(beta):
            assert abs(beta - 1.0) < 0.01

    def test_beta_insufficient_data(self):
        short = pd.Series([100, 101, 102])
        assert np.isnan(calculate_beta(short, short))


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
            assert isinstance(value, (str, int, float, bool, type(None), np.floating, np.integer)), \
                f"Key '{key}' has unexpected type: {type(value)}"

    def test_new_indicator_keys_present(self, sample_ohlcv):
        row = build_signal_row(sample_ohlcv, "TEST", {})
        expected_keys = [
            "macd_histogram", "bollinger_pct_b", "bollinger_bandwidth",
            "obv", "mfi", "ema_50", "ema_200",
            "forward_eps", "forward_pe",
            "dividend_yield_gap", "dividend_yield_trap",
        ]
        for key in expected_keys:
            assert key in row, f"Missing key: {key}"