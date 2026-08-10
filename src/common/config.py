"""
Central configuration for the stock analytics pipeline.

Ticker policy: src/common/tickers.txt is the single source of truth (private, gitignored).
A fresh clone falls back to the tracked tickers.example.txt starter list.
One ticker per line, # comments ok. File deploys with bundle to Databricks.
"""
import os

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

CATALOG = "stock_analytics"
SCHEMA_BRONZE = "bronze"
SCHEMA_SILVER = "silver"
SCHEMA_GOLD = "gold"

# -------------------------------------------------------------------
# Table names
# -------------------------------------------------------------------
TABLE_BRONZE_PRICES = f"{CATALOG}.{SCHEMA_BRONZE}.daily_prices"
TABLE_BRONZE_FUNDAMENTALS = f"{CATALOG}.{SCHEMA_BRONZE}.company_fundamentals"
TABLE_BRONZE_MACRO = f"{CATALOG}.{SCHEMA_BRONZE}.macro_indicators"
TABLE_BRONZE_FACTORS = f"{CATALOG}.{SCHEMA_BRONZE}.factor_returns"
TABLE_BRONZE_CONGRESSIONAL = f"{CATALOG}.{SCHEMA_BRONZE}.congressional_trades"
TABLE_BRONZE_SPLITS = f"{CATALOG}.{SCHEMA_BRONZE}.stock_splits"
# Silver: 3 tables separated by business process
TABLE_SILVER_PRICES = f"{CATALOG}.{SCHEMA_SILVER}.daily_prices"
TABLE_SILVER_SIGNALS = f"{CATALOG}.{SCHEMA_SILVER}.daily_signals"
TABLE_SILVER_FUNDAMENTALS = f"{CATALOG}.{SCHEMA_SILVER}.fundamentals_current"
TABLE_SILVER_CONGRESSIONAL = f"{CATALOG}.{SCHEMA_SILVER}.congressional_trades"
TABLE_SILVER_SPLITS = f"{CATALOG}.{SCHEMA_SILVER}.stock_splits"
# Gold: Kimball star schema (dims + facts) + serving tables
TABLE_GOLD_DIM_DATE = f"{CATALOG}.{SCHEMA_GOLD}.dim_date"
TABLE_GOLD_DIM_SECURITY = f"{CATALOG}.{SCHEMA_GOLD}.dim_security"
TABLE_GOLD_FACT_MARKET = f"{CATALOG}.{SCHEMA_GOLD}.fact_market_price_daily"
TABLE_GOLD_FACT_FUNDAMENTALS = f"{CATALOG}.{SCHEMA_GOLD}.fact_fundamental_snapshot"
TABLE_GOLD_FACT_SIGNALS = f"{CATALOG}.{SCHEMA_GOLD}.fact_signal_snapshot"
TABLE_GOLD_WATCHLIST = f"{CATALOG}.{SCHEMA_GOLD}.watchlist_ranked"
# Append-only. Never CREATE OR REPLACE — see src/scoring/snapshot.py.
TABLE_GOLD_RECOMMENDATIONS = f"{CATALOG}.{SCHEMA_GOLD}.recommendations"
TABLE_GOLD_SIGNAL_ALERTS = f"{CATALOG}.{SCHEMA_GOLD}.signal_alerts"
TABLE_GOLD_SIGNAL_HISTORY = f"{CATALOG}.{SCHEMA_GOLD}.signal_history"
TABLE_GOLD_BENCHMARK_COMPARE = f"{CATALOG}.{SCHEMA_GOLD}.benchmark_compare"
TABLE_GOLD_PORTFOLIO_CANDIDATES = f"{CATALOG}.{SCHEMA_GOLD}.portfolio_candidates"
TABLE_GOLD_TRADE_LOG = f"{CATALOG}.{SCHEMA_GOLD}.trade_log"
TABLE_GOLD_DAILY_ANALYTICS = f"{CATALOG}.{SCHEMA_GOLD}.daily_analytics"
TABLE_GOLD_CONGRESSIONAL_SUMMARY = f"{CATALOG}.{SCHEMA_GOLD}.congressional_trades_summary"

# -------------------------------------------------------------------
# WATCHLIST — Single source of truth: src/common/tickers.txt
# Deploys with bundle. One ticker per line, # comments ok.
# -------------------------------------------------------------------
def _parse_tickers(text):
    # Accept commas or newlines interchangeably, so the same value works in
    # .env, a GitHub secret, or a file with no conversion.
    parts = text.replace(",", "\n").splitlines()
    return [t.strip().upper() for t in parts
            if t.strip() and not t.strip().startswith("#")]


def _load_tickers():
    # 1. WATCHLIST env var (local .env) — single source, comma or newline.
    raw = os.getenv("WATCHLIST", "")
    if raw.strip():
        tickers = _parse_tickers(raw)
        if tickers:
            return tickers
    # 2. tickers.txt file (Databricks reads this; CI materializes it from the secret).
    # 3. tickers.example.txt fallback for a fresh clone.
    try:
        _dir = os.path.dirname(os.path.abspath(__file__))
    except NameError:
        _dir = os.path.join(os.getcwd(), "src", "common")
    path = os.path.join(_dir, "tickers.txt")
    if not os.path.exists(path):
        path = os.path.join(_dir, "tickers.example.txt")
    with open(path) as f:
        return _parse_tickers(f.read())


TICKERS = _load_tickers()

# Benchmark tickers for relative performance (always included in pipeline)
BENCHMARK_TICKERS = ["SPY", "QQQ"]

HISTORY_START_DATE = "2010-01-01"

FUNDAMENTALS_NUMERIC_FIELDS = [
    "marketCap", "enterpriseValue", "trailingPE", "forwardPE", "priceToBook",
    "trailingEps", "forwardEps", "dividendRate", "dividendYield", "payoutRatio",
    "beta", "profitMargins", "operatingMargins", "grossMargins", "revenueGrowth",
    "earningsGrowth", "returnOnAssets", "returnOnEquity", "totalRevenue", "grossProfits",
    "ebitda", "netIncomeToCommon", "totalCash", "totalDebt", "totalCashPerShare",
    "debtToEquity", "currentRatio", "operatingCashflow", "freeCashflow",
    "fiveYearAvgDividendYield",
    "sharesOutstanding", "floatShares", "bookValue", "enterpriseToRevenue",
    "enterpriseToEbitda", "52WeekChange", "targetMeanPrice", "targetMedianPrice",
    "targetHighPrice", "targetLowPrice", "numberOfAnalystOpinions",
]

FUNDAMENTALS_STRING_FIELDS = [
    "shortName", "longName", "sector", "industry", "country",
    "currency", "exchange", "quoteType", "recommendationKey",
]
