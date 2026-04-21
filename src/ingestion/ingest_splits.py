"""
Bronze layer: Ingest stock split history from yfinance.

Downloads split events for each ticker in the watchlist. yfinance
provides split ratios as a pandas Series indexed by date, where a
value of 2.0 means a 2-for-1 split (2 new shares per 1 old share).

With auto_adjust=True (used in ingest_prices), yfinance already
adjusts all historical OHLCV data. This table provides split metadata
for dashboard annotations and data quality verification only.

Writes to bronze.stock_splits (append mode with PIT metadata).
"""
import sys
import os
try:
    _src_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
except NameError:
    _src_dir = os.path.join(os.getcwd(), "..")
sys.path.insert(0, os.path.normpath(_src_dir))

import pandas as pd
from pyspark.sql import SparkSession
from common.config import TABLE_BRONZE_SPLITS, TICKERS, BENCHMARK_TICKERS
from common.run_context import new_run_id, now_ts, SOURCE_SYSTEM, LOAD_TYPE


def fetch_splits(ticker: str) -> pd.DataFrame | None:
    """Fetch stock split history for a single ticker from yfinance."""
    try:
        import yfinance as yf
        stock = yf.Ticker(ticker)
        splits = stock.splits
        if splits is None or splits.empty:
            return None
        df = splits.reset_index()
        df.columns = ["split_date", "split_ratio"]
        df["symbol"] = ticker
        df["split_date"] = pd.to_datetime(df["split_date"]).dt.date
        return df
    except Exception as e:
        print(f"[ingest_splits] ERROR fetching splits for {ticker}: {e}")
        return None


def main():
    spark = SparkSession.builder.getOrCreate()
    run_id = new_run_id()
    ingest_ts = now_ts()

    all_tickers = list(set(TICKERS + BENCHMARK_TICKERS))
    print(f"[ingest_splits] Fetching splits for {len(all_tickers)} tickers...")

    frames = []
    for ticker in all_tickers:
        df = fetch_splits(ticker)
        if df is not None and not df.empty:
            df["_run_id"] = run_id
            df["_ingest_ts"] = ingest_ts
            df["_source_system"] = "yfinance"
            df["_source_event_ts"] = df["split_date"].apply(
                lambda d: f"{d}T00:00:00Z" if d else None)
            df["_load_type"] = LOAD_TYPE
            frames.append(df)
            print(f"[ingest_splits] {ticker}: {len(df)} splits")
        else:
            print(f"[ingest_splits] {ticker}: no splits found")

    if not frames:
        print("[ingest_splits] No split data found. Exiting without error.")
        return

    all_splits = pd.concat(frames, ignore_index=True)

    # Clean for Spark
    all_splits = all_splits.where(pd.notna(all_splits), None)
    for col in all_splits.columns:
        if all_splits[col].dtype == object:
            all_splits[col] = all_splits[col].astype(object).where(
                all_splits[col].notna(), None)

    from pyspark.sql import Row
    rows = [Row(**{k: (None if isinstance(v, float) and pd.isna(v) else v)
                   for k, v in row.items()})
            for _, row in all_splits.iterrows()]
    spark_df = spark.createDataFrame(rows)

    # Cast types
    if "split_date" in spark_df.columns:
        spark_df = spark_df.withColumn(
            "split_date", spark_df["split_date"].cast("date"))
    if "split_ratio" in spark_df.columns:
        spark_df = spark_df.withColumn(
            "split_ratio", spark_df["split_ratio"].cast("double"))

    spark_df.write.mode("append").option("mergeSchema", "true").saveAsTable(
        TABLE_BRONZE_SPLITS)
    print(f"[ingest_splits] Appended {len(all_splits)} split records to "
          f"{TABLE_BRONZE_SPLITS}")


if __name__ == "__main__":
    main()