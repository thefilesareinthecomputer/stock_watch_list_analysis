"""
Silver layer: Compute full technical indicator matrix for each ticker.

Reads from Bronze tables, runs all indicator calculations, writes to
stock_analytics.silver.daily_signals with an as_of_date column.

Mode: IDEMPOTENT APPEND — deletes any existing rows for today's date
before appending. Safe to retry on failure.
"""
import sys
import os
try:
    _src_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
except NameError:
    _src_dir = os.path.join(os.getcwd(), "..")
sys.path.insert(0, os.path.normpath(_src_dir))

from datetime import date
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from common.config import (
    TICKERS,
    TABLE_BRONZE_PRICES,
    TABLE_BRONZE_FUNDAMENTALS,
    TABLE_SILVER_SIGNALS,
)
from common.indicators import build_signal_row


def main():
    spark = SparkSession.builder.getOrCreate()
    today = date.today()

    print(f"[build_silver] Building signals for {today}...")

    prices_df = spark.table(TABLE_BRONZE_PRICES).toPandas()
    print(f"[build_silver] Bronze prices: {len(prices_df)} rows, "
          f"{prices_df['symbol'].nunique()} tickers")

    try:
        fundamentals_df = spark.table(TABLE_BRONZE_FUNDAMENTALS).toPandas()
        print(f"[build_silver] Bronze fundamentals: {len(fundamentals_df)} rows")
    except Exception as e:
        print(f"[build_silver] WARNING: Could not read fundamentals: {e}")
        print(f"[build_silver] Proceeding without fundamentals data.")
        fundamentals_df = pd.DataFrame()

    rows = []
    errors = []
    skipped = []

    for symbol in TICKERS:
        try:
            ticker_prices = prices_df[prices_df["symbol"] == symbol].copy()

            if ticker_prices.empty:
                skipped.append((symbol, "no price data in Bronze"))
                continue

            ticker_prices["date"] = pd.to_datetime(ticker_prices["date"])
            ticker_prices = ticker_prices.set_index("date").sort_index()

            if len(ticker_prices) < 50:
                skipped.append((symbol, f"only {len(ticker_prices)} rows"))
                continue

            sym_info = {}
            if not fundamentals_df.empty and "symbol" in fundamentals_df.columns:
                matches = fundamentals_df[fundamentals_df["symbol"] == symbol]
                if not matches.empty:
                    sym_info = matches.iloc[0].to_dict()
                    sym_info = {k: (v if not (isinstance(v, float) and np.isnan(v)) else None)
                                for k, v in sym_info.items()}

            row = build_signal_row(ticker_prices, symbol, sym_info)
            row["as_of_date"] = today
            rows.append(row)

        except Exception as e:
            errors.append((symbol, str(e)))
            print(f"[build_silver] ERROR processing {symbol}: {e}")

    if not rows:
        raise RuntimeError(
            f"[build_silver] FATAL: No signals computed. "
            f"Errors: {errors}. Skipped: {skipped}"
        )

    silver_df = pd.DataFrame(rows)

    expected_cols = set(rows[0].keys())
    for i, row in enumerate(rows):
        if set(row.keys()) != expected_cols:
            diff = set(row.keys()).symmetric_difference(expected_cols)
            raise RuntimeError(
                f"[build_silver] FATAL: Row {i} ({row.get('symbol')}) has "
                f"inconsistent columns. Diff: {diff}"
            )

    print(f"[build_silver] Computed signals for {len(silver_df)} tickers")
    print(f"[build_silver] Skipped: {len(skipped)}, Errors: {len(errors)}")
    if skipped:
        print(f"[build_silver] Skipped tickers: {skipped}")
    if errors:
        print(f"[build_silver] Errors: {errors}")

    try:
        spark.sql(f"DELETE FROM {TABLE_SILVER_SIGNALS} WHERE as_of_date = '{today}'")
        print(f"[build_silver] Deleted existing rows for {today}")
    except Exception:
        print(f"[build_silver] Table doesn't exist yet, will be created on first write.")

    # Ensure clean dtypes for Spark serialization
    # Convert date column to string, cast back via Spark SQL
    if "as_of_date" in silver_df.columns:
        silver_df["as_of_date"] = silver_df["as_of_date"].astype(str)
    for col in silver_df.columns:
        if silver_df[col].dtype == object:
            silver_df[col] = silver_df[col].astype(object).where(silver_df[col].notna(), None)
    silver_df = silver_df.where(pd.notna(silver_df), None)

    # Use Row-based createDataFrame to bypass PySpark Connect ChunkedArray bug
    from pyspark.sql import Row
    rows = [Row(**{k: (None if isinstance(v, float) and pd.isna(v) else v) for k, v in row.items()}) for _, row in silver_df.iterrows()]
    spark_df = spark.createDataFrame(rows)
    # Cast date string back to proper date type
    if "as_of_date" in spark_df.columns:
        spark_df = spark_df.withColumn("as_of_date", spark_df["as_of_date"].cast("date"))
    spark_df.write \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable(TABLE_SILVER_SIGNALS)

    print(f"[build_silver] Written {len(silver_df)} rows to {TABLE_SILVER_SIGNALS}")


if __name__ == "__main__":
    main()
