"""
Technical indicator calculations.

All functions are pure: they take Pandas Series/DataFrames as input
and return numeric scalars or tuples. No side effects, no I/O.
Unit-testable without Spark or Databricks.

IMPORTANT: All DataFrames/Series passed to these functions must have
lowercase column names: 'open', 'high', 'low', 'close', 'volume'.

Formulas follow industry-standard definitions:
- RSI: Wilder's smoothing (RMA/ewm alpha=1/N), NOT Cutler's SMA
  Ref: Wilder, "New Concepts in Technical Trading Systems" (1978)
- ATR: Wilder's smoothing, same as RSI
- MACD: EMA(12) - EMA(26), signal = EMA(9) of MACD
- Bollinger: SMA(20) +/- 2*std(20)
  Ref: Bollinger, "Bollinger on Bollinger Bands" (2001)
- OBV: Cumulative +/- volume
  Ref: Granville, "New Key to the Three Banks" (1963)
- MFI: Money Flow Index, 14-period
  Ref: Quong & Soudack, S&C Magazine (1989)
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


def calculate_ema(close: pd.Series, span: int) -> float:
    """Exponential moving average, returns most recent value."""
    if len(close) < span:
        return np.nan
    return close.ewm(span=span, adjust=False).mean().iloc[-1]


# ── RSI ───────────────────────────────────────────────────────────

def calculate_rsi(close: pd.Series, window: int = 14) -> float:
    """
    Relative Strength Index (0-100) using Wilder's smoothing.

    Uses RMA (Running Moving Average): ewm(alpha=1/window, adjust=False).
    This matches TradingView, Bloomberg, and all major platforms.
    NOT Cutler's RSI which uses simple rolling mean.

    Ref: J. Welles Wilder Jr., "New Concepts in Technical Trading
    Systems" (1978), p. 63-70.
    """
    if len(close) < window + 1:
        return np.nan
    delta = close.diff()
    gain = delta.clip(lower=0).ewm(alpha=1/window, adjust=False).mean()
    loss = (-delta.clip(upper=0)).ewm(alpha=1/window, adjust=False).mean()
    loss_val = loss.iloc[-1]
    gain_val = gain.iloc[-1]
    if loss_val == 0:
        return 100.0
    if gain_val == 0:
        return 0.0
    rs = gain_val / loss_val
    return 100 - (100 / (1 + rs))


# ── MACD ──────────────────────────────────────────────────────────

def calculate_macd(close: pd.Series) -> tuple[float, float, float]:
    """
    MACD line, signal line, and histogram.

    Returns (macd, signal, histogram) where histogram = macd - signal.
    Ref: Appel, "The Major Movements of the Stock Market" (1979).
    """
    if len(close) < 35:
        return np.nan, np.nan, np.nan
    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    macd = ema12 - ema26
    signal = macd.ewm(span=9, adjust=False).mean()
    histogram = macd - signal
    return macd.iloc[-1], signal.iloc[-1], histogram.iloc[-1]


# ── Bollinger Bands ───────────────────────────────────────────────

def calculate_bollinger_bands(close: pd.Series) -> tuple[float, float, float, float]:
    """
    Returns (upper_band, lower_band, pct_b, bandwidth).

    - upper/lower: 20-day SMA +/- 2 standard deviations
    - pct_b: %B = (price - lower) / (upper - lower), ranges ~0-1
    - bandwidth: (upper - lower) / middle, measures volatility squeeze

    Ref: Bollinger, "Bollinger on Bollinger Bands" (2001).
    """
    if len(close) < 20:
        return np.nan, np.nan, np.nan, np.nan
    sma20 = close.rolling(window=20).mean()
    std20 = close.rolling(window=20).std()
    upper = (sma20 + (std20 * 2)).iloc[-1]
    lower = (sma20 - (std20 * 2)).iloc[-1]
    middle = sma20.iloc[-1]
    current_price = close.iloc[-1]

    # %B: position within bands (0 = at lower, 1 = at upper)
    if upper != lower and not np.isnan(upper) and not np.isnan(lower):
        pct_b = (current_price - lower) / (upper - lower)
        bandwidth = (upper - lower) / middle if middle != 0 else np.nan
    else:
        pct_b = np.nan
        bandwidth = np.nan

    return upper, lower, pct_b, bandwidth


# ── Volume ────────────────────────────────────────────────────────

def calculate_volume_trend(volume: pd.Series, days: int) -> float:
    """Average volume over N trailing days."""
    if len(volume) < days:
        return np.nan
    return volume.iloc[-days:].mean()


def calculate_obv(close: pd.Series, volume: pd.Series) -> float:
    """
    On-Balance Volume. Cumulative volume with sign based on price direction.

    Ref: Granville, "New Key to the Three Banks" (1963).
    """
    if len(close) < 2 or len(volume) < 2:
        return np.nan
    direction = np.sign(close.diff())
    obv = (direction * volume).cumsum()
    return obv.iloc[-1]


def calculate_mfi(high: pd.Series, low: pd.Series,
                  close: pd.Series, volume: pd.Series,
                  window: int = 14) -> float:
    """
    Money Flow Index (0-100). Volume-weighted RSI equivalent.

    MFI = 100 - (100 / (1 + money_ratio))
    where money_ratio = positive_money_flow / negative_money_flow over window.

    Ref: Quong & Soudack, S&C Magazine (1989).
    """
    if len(close) < window + 1 or len(volume) < window + 1:
        return np.nan
    typical_price = (high + low + close) / 3
    raw_money_flow = typical_price * volume

    # Positive/negative money flow based on whether typical price rose
    tp_diff = typical_price.diff()
    positive_mf = raw_money_flow.where(tp_diff > 0, 0.0)
    negative_mf = raw_money_flow.where(tp_diff < 0, 0.0)

    pos_sum = positive_mf.rolling(window=window).sum().iloc[-1]
    neg_sum = negative_mf.rolling(window=window).sum().iloc[-1]

    if neg_sum == 0:
        return 100.0
    if pos_sum == 0:
        return 0.0

    money_ratio = pos_sum / neg_sum
    return 100 - (100 / (1 + money_ratio))


# ── ATR ───────────────────────────────────────────────────────────

def calculate_atr(df: pd.DataFrame, window: int) -> float:
    """
    Average True Range using Wilder's smoothing (RMA).

    Uses ewm(alpha=1/window, adjust=False) to match industry standard.
    NOT simple rolling mean.

    Ref: Wilder, "New Concepts in Technical Trading Systems" (1978), p. 23-30.
    """
    if len(df) < window + 1:
        return np.nan
    high_low = df["high"] - df["low"]
    high_close = np.abs(df["high"] - df["close"].shift())
    low_close = np.abs(df["low"] - df["close"].shift())
    ranges = pd.concat([high_low, high_close, low_close], axis=1)
    true_range = ranges.max(axis=1)
    atr = true_range.ewm(alpha=1/window, adjust=False).mean()
    return atr.iloc[-1]


# ── Risk metrics ─────────────────────────────────────────────────

def calculate_sharpe(close: pd.Series, window: int = 252,
                    risk_free_rate: float = 0.0) -> float:
    """
    Sharpe ratio: (annualized_return - risk_free) / annualized_volatility.

    Assumes daily close prices. Annualization: 252 trading days.
    Ref: Sharpe, "The Sharpe Ratio" J. of Portfolio Mgmt (1994).
    """
    if len(close) < window:
        return np.nan
    daily_returns = close.pct_change().dropna()
    if len(daily_returns) < window:
        return np.nan
    recent_returns = daily_returns.iloc[-window:]
    mean_return = recent_returns.mean() * 252
    vol = recent_returns.std() * np.sqrt(252)
    if vol == 0:
        return np.nan
    return (mean_return - risk_free_rate) / vol


def calculate_sortino(close: pd.Series, window: int = 252,
                      target_return: float = 0.0) -> float:
    """
    Sortino ratio: (annualized_return - target) / downside_deviation.

    Only penalizes downside volatility, not upside.
    Ref: Sortino & Price, "Performance Measurement in a Downside
    Risk Framework" J. of Investing (1994).
    """
    if len(close) < window:
        return np.nan
    daily_returns = close.pct_change().dropna()
    if len(daily_returns) < window:
        return np.nan
    recent_returns = daily_returns.iloc[-window:]
    mean_return = recent_returns.mean() * 252
    downside = recent_returns[recent_returns < target_return / 252]
    if len(downside) == 0:
        return np.inf if mean_return > target_return / 252 else np.nan
    downside_dev = downside.std() * np.sqrt(252)
    if downside_dev == 0:
        return np.nan
    return (mean_return - target_return) / downside_dev


def calculate_max_drawdown(close: pd.Series, window: int = 252) -> float:
    """
    Maximum drawdown over the last N days. Returns a negative number.

    Drawdown = (trough - peak) / peak, measured from running maximum.
    """
    if len(close) < window:
        return np.nan
    recent = close.iloc[-window:]
    running_max = recent.cummax()
    drawdown = (recent - running_max) / running_max
    return drawdown.min()  # Most negative value = max drawdown


def calculate_beta(close: pd.Series, benchmark_close: pd.Series,
                  window: int = 252) -> float:
    """
    Beta vs benchmark. Cov(stock, benchmark) / Var(benchmark).

    Uses daily returns over the last `window` trading days.
    """
    if len(close) < window + 1 or len(benchmark_close) < window + 1:
        return np.nan
    stock_returns = close.pct_change().dropna()
    bench_returns = benchmark_close.pct_change().dropna()

    # Align on common dates
    common_len = min(len(stock_returns), len(bench_returns), window)
    if common_len < 20:
        return np.nan
    sr = stock_returns.iloc[-common_len:]
    br = bench_returns.iloc[-common_len:]

    cov = sr.cov(br)
    var = br.var()
    if var == 0:
        return np.nan
    return cov / var


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
EMA_WINDOWS = [50, 200]
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

    # ── Core indicators (Wilder's RSI, MACD with histogram, Bollinger with %B) ──
    macd_val, signal_val, macd_histogram = calculate_macd(close)
    upper_band, lower_band, bollinger_pct_b, bollinger_bandwidth = calculate_bollinger_bands(close)
    rsi = calculate_rsi(close)
    obv = calculate_obv(close, volume)
    mfi = calculate_mfi(stock_data["high"], stock_data["low"], close, volume)
    trade_signal = determine_signal(macd_val, signal_val, rsi)

    # ── Fundamentals ──────────────────────────────────────
    eps = calculate_eps(info)
    pe_ratio = calculate_pe_ratio(info, current_price)
    dividend_yield = calculate_dividend_yield(info, current_price)

    # Forward EPS/PE (already in FUNDAMENTALS_NUMERIC_FIELDS, surfaced here)
    forward_eps = safe_float(info.get("forwardEps"))
    forward_pe = safe_float(info.get("forwardPE"))

    # Dividend yield gap detection (TTM vs FWD, flag yield traps)
    trailing_yield = dividend_yield
    forward_yield = safe_float(info.get("dividendYield"))
    dividend_yield_gap = trailing_yield - forward_yield if (not np.isnan(trailing_yield) and not np.isnan(forward_yield)) else np.nan
    # Flag if TTM yield > FWD yield by >1.5 percentage points (potential yield trap)
    dividend_yield_trap = True if (not np.isnan(dividend_yield_gap) and dividend_yield_gap > 0.015) else False

    row = {
        "symbol": symbol,
        "trade_signal": trade_signal,
        "rsi": rsi,
        "macd": macd_val,
        "macd_signal_line": signal_val,
        "macd_histogram": macd_histogram,
        "last_closing_price": current_price,
        "last_opening_price": stock_data["open"].iloc[-1],
        "bollinger_upper": upper_band,
        "bollinger_lower": lower_band,
        "bollinger_pct_b": bollinger_pct_b,
        "bollinger_bandwidth": bollinger_bandwidth,
        "obv": obv,
        "mfi": mfi,
        "eps": eps,
        "pe_ratio": pe_ratio,
        "forward_eps": forward_eps,
        "forward_pe": forward_pe,
        "dividend_yield": dividend_yield,
        "dividend_yield_gap": dividend_yield_gap,
        "dividend_yield_trap": dividend_yield_trap,
    }

    # Last day change (need at least 2 days)
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

    # Price changes
    for days in CHANGE_WINDOWS:
        abs_chg, pct_chg = calculate_change(close, days)
        row[f"change_{days}d_abs"] = abs_chg
        row[f"change_{days}d_pct"] = pct_chg

    # High/low ranges
    for days in HIGH_LOW_WINDOWS:
        if len(stock_data) >= days:
            row[f"high_{days}d"] = stock_data["high"].tail(days).max()
            row[f"low_{days}d"] = stock_data["low"].tail(days).min()
        else:
            row[f"high_{days}d"] = np.nan
            row[f"low_{days}d"] = np.nan

    # SMA moving averages
    for window in MA_WINDOWS:
        row[f"ma_{window}"] = calculate_moving_average(close, window)

    # EMA moving averages
    for span in EMA_WINDOWS:
        row[f"ema_{span}"] = calculate_ema(close, span)

    # Volume trends
    for days in VOLUME_WINDOWS:
        row[f"vol_trend_{days}d"] = calculate_volume_trend(volume, days)

    # ATR (Wilder's smoothing)
    for window in ATR_WINDOWS:
        row[f"atr_{window}d"] = calculate_atr(stock_data, window)

    # Round all numeric values to 4 decimal places
    for key, value in row.items():
        if isinstance(value, float) and not np.isnan(value):
            row[key] = round(value, 4)

    return row