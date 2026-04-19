"""
Central configuration for the stock analytics pipeline.
Edit TICKERS to change your watchlist.
"""

CATALOG = "stock_analytics"
SCHEMA_BRONZE = "bronze"
SCHEMA_SILVER = "silver"
SCHEMA_GOLD = "gold"

TABLE_BRONZE_PRICES = f"{CATALOG}.{SCHEMA_BRONZE}.daily_prices"
TABLE_BRONZE_FUNDAMENTALS = f"{CATALOG}.{SCHEMA_BRONZE}.company_fundamentals"
TABLE_SILVER_SIGNALS = f"{CATALOG}.{SCHEMA_SILVER}.daily_signals"
TABLE_GOLD_WATCHLIST = f"{CATALOG}.{SCHEMA_GOLD}.watchlist_ranked"
TABLE_GOLD_SIGNAL_HISTORY = f"{CATALOG}.{SCHEMA_GOLD}.signal_history"
TABLE_GOLD_TRADE_LOG = f"{CATALOG}.{SCHEMA_GOLD}.trade_log"

# -------------------------------------------------------------------
# WATCHLIST — Edit this list with your actual tickers.
# Keep under ~200 for Free Edition quota.
# -------------------------------------------------------------------
TICKERS = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA",
    "META", "TSLA", "BRK-B", "JPM", "V",
    "JNJ", "WMT", "PG", "MA", "HD",
    "XOM", "UNH", "BAC", "PFE", "ABBV",
]

HISTORY_START_DATE = "2010-01-01"

FUNDAMENTALS_NUMERIC_FIELDS = [
    "marketCap", "enterpriseValue", "trailingPE", "forwardPE", "priceToBook",
    "trailingEps", "forwardEps", "dividendRate", "dividendYield", "payoutRatio",
    "beta", "profitMargins", "operatingMargins", "grossMargins", "revenueGrowth",
    "earningsGrowth", "returnOnAssets", "returnOnEquity", "totalRevenue", "grossProfits",
    "ebitda", "netIncomeToCommon", "totalCash", "totalDebt", "totalCashPerShare",
    "debtToEquity", "currentRatio", "operatingCashflow", "freeCashflow",
    "sharesOutstanding", "floatShares", "bookValue", "enterpriseToRevenue",
    "enterpriseToEbitda", "52WeekChange", "targetMeanPrice", "targetMedianPrice",
    "targetHighPrice", "targetLowPrice", "numberOfAnalystOpinions",
]

FUNDAMENTALS_STRING_FIELDS = [
    "shortName", "longName", "sector", "industry", "country",
    "currency", "exchange", "quoteType", "recommendationKey",
]
