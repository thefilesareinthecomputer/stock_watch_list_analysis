"""
Technical indicator calculations.

All functions are pure: they take Pandas Series/DataFrames as input
and return numeric scalars or tuples. No side effects, no I/O.
Unit-testable without Spark or Databricks.

IMPORTANT: All DataFrames/Series passed to these functions must have
lowercase column names: 'open', 'high', 'low', 'close', 'volume'.
"""
import numpy as np
import pandas as pd


# ── Price change ──────────────────────────────────────────────────

def calculate_change(close: pd.Series, days: int) -> tuple[float, float]:
    """Return (absolute_change, percent_change) over N trading days."""
    if len(close) < days or days < 1:
        return np.nan, np.nan
    start_price = close.iloc[-days]
    end_price = close.iloc[-1]
    if start_price == 0 or np.isnan(start_price) or np.isnan(end_price):
        return np.nan, np.nan
    abs_change = end_price - start_price
    pct_change = (abs_change / start_price) * 100
    return abs_change, pct_change


# ── Moving averages ───────────────────────────────────────────────

def calculate_moving_average(close: pd.Series, window: int) -> float:
    """Simple moving average, returns most recent value."""
    if len(close) < window:
        return np.nan
    return close.rolling(window=window).mean().iloc[-1]


# ── RSI ───────────────────────────────────────────────────────────

def calculate_rsi(close: pd.Series, window: int = 14) -> float:
    """Relative Strength Index (0-100)."""
    if len(close) < window + 1:
        return np.nan
    delta = close.diff()
    gain = delta.clip(lower=0).rolling(window=window).mean()
    loss = (-delta.clip(upper=0)).rolling(window=window).mean()
    loss_val = loss.iloc[-1]
    gain_val = gain.iloc[-1]
    if loss_val == 0:
        return 100.0
    if gain_val == 0:
        return 0.0
    rs = gain_val / loss_val
    return 100 - (100 / (1 + rs))


# ── MACD ──────────────────────────────────────────────────────────

def calculate_macd(close: pd.Series) -> tuple[float, float]:
    """MACD line and signal line, returns (macd, signal)."""
    if len(close) < 35:
        return np.nan, np.nan
    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    macd = ema12 - ema26
    signal = macd.ewm(span=9, adjust=False).mean()
    return macd.iloc[-1], signal.iloc[-1]


# ── Bollinger Bands ───────────────────────────────────────────────

def calculate_bollinger_bands(close: pd.Series) -> tuple[float, float]:
    """Returns (upper_band, lower_band) based on 20-day SMA ± 2 std."""
    if len(close) < 20:
        return np.nan, np.nan
    sma20 = close.rolling(window=20).mean()
    std20 = close.rolling(window=20).std()
    upper = (sma20 + (std20 * 2)).iloc[-1]
    lower = (sma20 - (std20 * 2)).iloc[-1]
    return upper, lower


# ── Volume ────────────────────────────────────────────────────────

def calculate_volume_trend(volume: pd.Series, days: int) -> float:
    """Average volume over N trailing days."""
    if len(volume) < days:
        return np.nan
    return volume.iloc[-days:].mean()


# ── ATR ───────────────────────────────────────────────────────────

def calculate_atr(df: pd.DataFrame, window: int) -> float:
    """
    Average True Range.
    Expects df with lowercase columns: 'high', 'low', 'close'.
    """
    if len(df) < window + 1:
        return np.nan
    high_low = df["high"] - df["low"]
    high_close = np.abs(df["high"] - df["close"].shift())
    low_close = np.abs(df["low"] - df["close"].shift())
    ranges = pd.concat([high_low, high_close, low_close], axis=1)
    true_range = ranges.max(axis=1)
    atr = true_range.rolling(window=window).mean()
    return atr.iloc[-1]


# ── Fundamentals ──────────────────────────────────────────────────

def safe_float(value, default=np.nan) -> float:
    """Safely convert a value to float, returning default on failure."""
    if value is None:
        return default
    try:
        result = float(value)
        if np.isinf(result):
            return default
        return result
    except (TypeError, ValueError):
        return default


def calculate_eps(info: dict) -> float:
    """
    Earnings per share from fundamentals dict.
    Uses trailingEps directly if available (preferred — comes from Yahoo).
    Falls back to netIncomeToCommon / sharesOutstanding.
    """
    trailing_eps = safe_float(info.get("trailingEps"))
    if not np.isnan(trailing_eps):
        return trailing_eps
    net_income = safe_float(info.get("netIncomeToCommon"))
    shares = safe_float(info.get("sharesOutstanding"))
    if not np.isnan(net_income) and not np.isnan(shares) and shares != 0:
        return net_income / shares
    return np.nan


def calculate_pe_ratio(info: dict, current_price: float) -> float:
    """
    Price-to-earnings ratio.
    Uses trailingPE directly if available (preferred).
    Falls back to current_price / EPS.
    """
    trailing_pe = safe_float(info.get("trailingPE"))
    if not np.isnan(trailing_pe):
        return trailing_pe
    eps = calculate_eps(info)
    if not np.isnan(eps) and eps != 0:
        return current_price / eps
    return np.nan


def calculate_dividend_yield(info: dict, current_price: float) -> float:
    """Dividend yield as a decimal."""
    yf_yield = safe_float(info.get("dividendYield"))
    if not np.isnan(yf_yield):
        return yf_yield
    dividend_rate = safe_float(info.get("dividendRate"), default=0.0)
    price = safe_float(current_price, default=0.0)
    if price != 0:
        return dividend_rate / price
    return 0.0


# ── Signal ───────────────────────────────────────────────────────

def determine_signal(macd_val: float, signal_val: float, rsi: float) -> str:
    """Simple Buy/Sell/Hold signal based on MACD crossover and RSI."""
    if np.isnan(macd_val) or np.isnan(signal_val) or np.isnan(rsi):
        return "Insufficient Data"
    if macd_val > signal_val and rsi < 70:
        return "Buy"
    elif macd_val < signal_val and rsi > 30:
        return "Sell"
    else:
        return "Hold"


# ── Master row builder ────────────────────────────────────────────

CHANGE_WINDOWS = [2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14,
                 30, 60, 90, 180, 365, 730, 1095, 1460, 1825, 3650]
HIGH_LOW_WINDOWS = [3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14,
                    21, 30, 60, 90, 180, 365, 730, 1095, 1460, 1825, 3650]
MA_WINDOWS = [3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14,
              21, 30, 40, 50, 60, 100, 200, 300]
VOLUME_WINDOWS = [3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14,
                  21, 30, 40, 50, 60, 90]
ATR_WINDOWS = [2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14,
               21, 30, 60, 90, 180]


def build_signal_row(
    stock_data: pd.DataFrame,
    symbol: str,
    info: dict,
) -> dict:
    """
    Build a single row of the signals table for one ticker.

    Expects stock_data with DatetimeIndex and lowercase columns:
    'open', 'high', 'low', 'close', 'volume'

    Returns a flat dict with a FIXED, CONSISTENT set of keys regardless
    of data length. Missing values are np.nan, never missing keys.
    """
    close = stock_data["close"]
    volume = stock_data["volume"]
    current_price = close.iloc[-1]

    macd_val, signal_val = calculate_macd(close)
    upper_band, lower_band = calculate_bollinger_bands(close)
    rsi = calculate_rsi(close)
    trade_signal = determine_signal(macd_val, signal_val, rsi)

    eps = calculate_eps(info)
    pe_ratio = calculate_pe_ratio(info, current_price)
    dividend_yield = calculate_dividend_yield(info, current_price)

    row = {
        "symbol": symbol,
        "trade_signal": trade_signal,
        "rsi": rsi,
        "macd": macd_val,
        "macd_signal_line": signal_val,
        "last_closing_price": current_price,
        "last_opening_price": stock_data["open"].iloc[-1],
        "bollinger_upper": upper_band,
        "bollinger_lower": lower_band,
        "eps": eps,
        "pe_ratio": pe_ratio,
        "dividend_yield": dividend_yield,
    }

    if len(close) >= 2:
        prev = close.iloc[-2]
        if prev != 0 and not np.isnan(prev):
            row["last_day_change_abs"] = current_price - prev
            row["last_day_change_pct"] = ((current_price - prev) / prev) * 100
        else:
            row["last_day_change_abs"] = np.nan
            row["last_day_change_pct"] = np.nan
    else:
        row["last_day_change_abs"] = np.nan
        row["last_day_change_pct"] = np.nan

    for days in CHANGE_WINDOWS:
        abs_chg, pct_chg = calculate_change(close, days)
        row[f"change_{days}d_abs"] = abs_chg
        row[f"change_{days}d_pct"] = pct_chg

    for days in HIGH_LOW_WINDOWS:
        if len(stock_data) >= days:
            row[f"high_{days}d"] = stock_data["high"].tail(days).max()
            row[f"low_{days}d"] = stock_data["low"].tail(days).min()
        else:
            row[f"high_{days}d"] = np.nan
            row[f"low_{days}d"] = np.nan

    for window in MA_WINDOWS:
        row[f"ma_{window}"] = calculate_moving_average(close, window)

    for days in VOLUME_WINDOWS:
        row[f"vol_trend_{days}d"] = calculate_volume_trend(volume, days)

    for window in ATR_WINDOWS:
        row[f"atr_{window}d"] = calculate_atr(stock_data, window)

    for key, value in row.items():
        if isinstance(value, float) and not np.isnan(value):
            row[key] = round(value, 4)

    return row
