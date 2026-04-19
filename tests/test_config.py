"""Smoke tests for config validity — run before deploy."""
import pytest

# Minimal config values for local testing without Databricks
CATALOG = "stock_analytics"
SCHEMA_BRONZE = "bronze"
SCHEMA_SILVER = "silver"
SCHEMA_GOLD = "gold"

TICKERS = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA",
    "META", "TSLA", "BRK-B", "JPM", "V",
    "JNJ", "WMT", "PG", "MA", "HD",
    "XOM", "UNH", "BAC", "PFE", "ABBV",
]

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


def test_tickers_not_empty():
    assert len(TICKERS) > 0


def test_tickers_are_strings():
    for t in TICKERS:
        assert isinstance(t, str)
        assert len(t) <= 10


def test_no_duplicate_tickers():
    assert len(TICKERS) == len(set(TICKERS))


def test_catalog_name():
    assert CATALOG in ("stock_analytics", "main")


def test_fundamentals_fields_no_overlap():
    overlap = set(FUNDAMENTALS_NUMERIC_FIELDS) & set(FUNDAMENTALS_STRING_FIELDS)
    assert not overlap, f"Fields in both numeric and string: {overlap}"
