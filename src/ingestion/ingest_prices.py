"""
Bronze layer: Ingest daily OHLCV price data from yfinance.
Writes to stock_analytics.bronze.daily_prices as a Delta table.

Mode: APPEND (immutable evidence — never overwrite).
Each row carries point-in-time metadata:
  _run_id, _ingest_ts, _source_system, _source_event_ts, _load_type

Deduplication happens in Silver (MERGE on symbol + date).
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
from common.run_context import new_run_id, now_ts, source_event_ts, SOURCE_SYSTEM, LOAD_TYPE


PRICE_SCHEMA = StructType([
    StructField("date", DateType(), False),
    StructField("open", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("close", DoubleType(), True),
    StructField("volume", LongType(), True),
    StructField("symbol", StringType(), False),
    # Point-in-time metadata
    StructField("_run_id", StringType(), False),
    StructField("_ingest_ts", StringType(), False),
    StructField("_source_system", StringType(), False),
    StructField("_source_event_ts", StringType(), False),
    StructField("_load_type", StringType(), False),
])


def download_single_ticker(ticker: str, run_id: str, ingest_ts: str) -> pd.DataFrame | None:
    """
    Download OHLCV for a single ticker. Returns a clean DataFrame
    with lowercase columns, a 'symbol' column, and PIT metadata,
    or None on failure.
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

        # Preserve NaN as NULL — zero and missing are semantically different.
        # Volume NaN means "no data", not "0 shares traded".
        # (Previously fillna(0) — this was wrong for quality tracking.)
        df["volume"] = df["volume"].astype("Int64")  # nullable integer

        # Point-in-time metadata columns
        df["_run_id"] = run_id
        df["_ingest_ts"] = ingest_ts
        df["_source_system"] = SOURCE_SYSTEM
        # _source_event_ts = the trade date (when the market event occurred)
        df["_source_event_ts"] = df["date"].apply(lambda d: source_event_ts(d))
        df["_load_type"] = LOAD_TYPE

        df = df[["date", "open", "high", "low", "close", "volume", "symbol",
                 "_run_id", "_ingest_ts", "_source_system", "_source_event_ts", "_load_type"]]

        return df

    except Exception as e:
        print(f"[ingest_prices] ERROR downloading {ticker}: {e}")
        return None


def main():
    spark = SparkSession.builder.getOrCreate()

    run_id = new_run_id()
    ingest_ts = now_ts()

    print(f"[ingest_prices] Run ID: {run_id}")
    print(f"[ingest_prices] Ingest TS: {ingest_ts}")
    print(f"[ingest_prices] Downloading data for {len(TICKERS)} tickers...")

    frames = []
    for i, ticker in enumerate(TICKERS):
        df = download_single_ticker(ticker, run_id, ingest_ts)
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

    # Quality checks
    from common.quality import check_bronze_prices
    issues = check_bronze_prices(bronze_df)
    for issue in issues:
        print(f"[ingest_prices] QUALITY: {issue}")

    # Cast dtypes for Spark compatibility
    # Keep date as string for Row serialization, cast via Spark SQL after
    bronze_df["date"] = pd.to_datetime(bronze_df["date"]).dt.strftime("%Y-%m-%d")
    # Volume: nullable int → float for Row serialization (Spark LongType handles NULL)
    bronze_df["volume"] = bronze_df["volume"].astype("float64")
    for col in ["open", "high", "low", "close"]:
        bronze_df[col] = bronze_df[col].astype("float64")
    bronze_df["symbol"] = bronze_df["symbol"].astype(str)

    # Use Row-based createDataFrame to bypass PySpark Connect ChunkedArray bug
    from pyspark.sql import Row
    rows = [Row(**{k: (None if pd.isna(v) else v) for k, v in row.items()})
            for _, row in bronze_df.iterrows()]
    spark_df = spark.createDataFrame(rows)
    # Cast date string column to proper date type, volume to long
    spark_df = spark_df.selectExpr(
        "CAST(date AS DATE) as date",
        "open", "high", "low", "close",
        "CAST(volume AS LONG) as volume",
        "symbol",
        "_run_id", "_ingest_ts", "_source_system", "_source_event_ts", "_load_type"
    )
    # APPEND — Bronze stores evidence, not truth. Never overwrite.
    spark_df.write.mode("append").option("mergeSchema", "true").saveAsTable(TABLE_BRONZE_PRICES)
    print(f"[ingest_prices] Appended {len(bronze_df)} rows to {TABLE_BRONZE_PRICES}")


if __name__ == "__main__":
    main()