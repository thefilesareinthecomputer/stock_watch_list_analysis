"""
Bronze layer: Ingest daily OHLCV price data from yfinance.
Writes to stock_analytics.bronze.daily_prices as a Delta table.

Mode: OVERWRITE (full refresh each run).
"""
import sys
import os
try:
    _src_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
except NameError:
    _src_dir = os.path.join(os.getcwd(), "..")
sys.path.insert(0, os.path.normpath(_src_dir))

import time
import pandas as pd
import yfinance as yf
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType, DateType
)
from common.config import TICKERS, TABLE_BRONZE_PRICES, HISTORY_START_DATE


PRICE_SCHEMA = StructType([
    StructField("date", DateType(), False),
    StructField("open", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("close", DoubleType(), True),
    StructField("volume", LongType(), True),
    StructField("symbol", StringType(), False),
])


def download_single_ticker(ticker: str) -> pd.DataFrame | None:
    """
    Download OHLCV for a single ticker. Returns a clean DataFrame
    with lowercase columns and a 'symbol' column, or None on failure.
    """
    try:
        df = yf.download(
            ticker,
            period="max",
            auto_adjust=True,
            progress=False,
            threads=False,
        )

        if df.empty:
            print(f"[ingest_prices] WARNING: No data returned for {ticker}")
            return None

        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.droplevel("Ticker")

        df.columns = [c.lower().strip() for c in df.columns]

        required = {"open", "high", "low", "close", "volume"}
        missing = required - set(df.columns)
        if missing:
            print(f"[ingest_prices] WARNING: {ticker} missing columns: {missing}")
            return None

        df = df.dropna(subset=["close"])
        if df.empty:
            print(f"[ingest_prices] WARNING: {ticker} has no valid close prices after cleaning")
            return None

        df = df[df.index >= HISTORY_START_DATE]
        df["symbol"] = ticker
        df.index.name = "date"
        df = df.reset_index()
        df["date"] = pd.to_datetime(df["date"]).dt.date
        df["volume"] = df["volume"].fillna(0).astype("int64")
        df = df[["date", "open", "high", "low", "close", "volume", "symbol"]]

        return df

    except Exception as e:
        print(f"[ingest_prices] ERROR downloading {ticker}: {e}")
        return None


def main():
    spark = SparkSession.builder.getOrCreate()

    print(f"[ingest_prices] Downloading data for {len(TICKERS)} tickers...")

    frames = []
    for i, ticker in enumerate(TICKERS):
        df = download_single_ticker(ticker)
        if df is not None:
            frames.append(df)
            print(f"[ingest_prices] ({i+1}/{len(TICKERS)}) {ticker}: {len(df)} rows")
        if i > 0 and i % 10 == 0:
            time.sleep(1)

    if not frames:
        raise RuntimeError("[ingest_prices] FATAL: No ticker data was successfully downloaded.")

    bronze_df = pd.concat(frames, ignore_index=True)

    assert len(bronze_df) > 0, "Bronze prices DataFrame is empty"
    assert bronze_df["close"].notna().sum() > 0, "No valid close prices in any ticker"

    null_pct = bronze_df["close"].isna().sum() / len(bronze_df) * 100
    if null_pct > 5:
        print(f"[ingest_prices] WARNING: {null_pct:.1f}% of close prices are null")

    ticker_counts = bronze_df.groupby("symbol").size()
    print(f"[ingest_prices] Tickers with data: {len(ticker_counts)}")
    print(f"[ingest_prices] Total rows: {len(bronze_df)}")
    print(f"[ingest_prices] Date range: {bronze_df['date'].min()} to {bronze_df['date'].max()}")

    # Cast dtypes for Spark compatibility
    # Keep date as string for Row serialization, cast via Spark SQL after
    bronze_df["date"] = pd.to_datetime(bronze_df["date"]).dt.strftime("%Y-%m-%d")
    bronze_df["volume"] = bronze_df["volume"].astype("int64")
    for col in ["open", "high", "low", "close"]:
        bronze_df[col] = bronze_df[col].astype("float64")
    bronze_df["symbol"] = bronze_df["symbol"].astype(str)

    # Use Row-based createDataFrame to bypass PySpark Connect ChunkedArray bug
    from pyspark.sql import Row
    rows = [Row(**row.to_dict()) for _, row in bronze_df.iterrows()]
    spark_df = spark.createDataFrame(rows)
    # Cast date string column to proper date type
    spark_df = spark_df.selectExpr(
        "CAST(date AS DATE) as date",
        "open", "high", "low", "close", "volume", "symbol"
    )
    spark_df.write.mode("overwrite").saveAsTable(TABLE_BRONZE_PRICES)
    print(f"[ingest_prices] Written to {TABLE_BRONZE_PRICES}")


if __name__ == "__main__":
    main()
