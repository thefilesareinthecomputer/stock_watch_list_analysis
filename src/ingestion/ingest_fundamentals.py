"""
Bronze layer: Ingest company fundamental data from yfinance.
Writes to stock_analytics.bronze.company_fundamentals as a Delta table.

Mode: OVERWRITE (latest snapshot).
"""
import sys
import os
try:
    _src_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
except NameError:
    _src_dir = os.path.join(os.getcwd(), "..")
sys.path.insert(0, os.path.normpath(_src_dir))

import time
import numpy as np
import pandas as pd
import yfinance as yf
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
)
from common.config import (
    TICKERS,
    TABLE_BRONZE_FUNDAMENTALS,
    FUNDAMENTALS_NUMERIC_FIELDS,
    FUNDAMENTALS_STRING_FIELDS,
)


def build_fundamentals_schema() -> StructType:
    """Build explicit Spark schema for the fundamentals table."""
    fields = [StructField("symbol", StringType(), False)]
    for col in FUNDAMENTALS_STRING_FIELDS:
        fields.append(StructField(col, StringType(), True))
    for col in FUNDAMENTALS_NUMERIC_FIELDS:
        fields.append(StructField(col, DoubleType(), True))
    return StructType(fields)


def extract_fundamentals(ticker: str) -> dict | None:
    """Fetch .info for a single ticker and extract curated fields."""
    try:
        stock = yf.Ticker(ticker)
        info = stock.info

        if not info or not isinstance(info, dict):
            print(f"[ingest_fundamentals] WARNING: No info returned for {ticker}")
            return None

        if len(info) < 10:
            print(f"[ingest_fundamentals] WARNING: Sparse info for {ticker} ({len(info)} keys), skipping")
            return None

        row = {"symbol": ticker}

        for field in FUNDAMENTALS_STRING_FIELDS:
            val = info.get(field)
            row[field] = str(val) if val is not None else None

        for field in FUNDAMENTALS_NUMERIC_FIELDS:
            val = info.get(field)
            if val is None or val == "None":
                row[field] = None
            else:
                try:
                    f = float(val)
                    row[field] = f if not np.isinf(f) else None
                except (TypeError, ValueError):
                    row[field] = None

        return row

    except Exception as e:
        print(f"[ingest_fundamentals] ERROR fetching {ticker}: {e}")
        return None


def main():
    spark = SparkSession.builder.getOrCreate()
    schema = build_fundamentals_schema()

    print(f"[ingest_fundamentals] Fetching info for {len(TICKERS)} tickers...")

    rows = []
    for i, ticker in enumerate(TICKERS):
        row = extract_fundamentals(ticker)
        if row is not None:
            rows.append(row)
            print(f"[ingest_fundamentals] ({i+1}/{len(TICKERS)}) {ticker}: OK")
        else:
            print(f"[ingest_fundamentals] ({i+1}/{len(TICKERS)}) {ticker}: SKIPPED")
        time.sleep(0.5)

    if not rows:
        raise RuntimeError("[ingest_fundamentals] FATAL: No fundamental data was fetched.")

    column_order = ["symbol"] + FUNDAMENTALS_STRING_FIELDS + FUNDAMENTALS_NUMERIC_FIELDS
    fundamentals_df = pd.DataFrame(rows, columns=column_order)

    print(f"[ingest_fundamentals] Tickers with data: {len(fundamentals_df)}")
    null_counts = fundamentals_df[FUNDAMENTALS_NUMERIC_FIELDS].isna().sum()
    high_null = null_counts[null_counts > len(fundamentals_df) * 0.5]
    if not high_null.empty:
        print(f"[ingest_fundamentals] WARNING: Fields with >50% nulls: {high_null.to_dict()}")

    # Cast dtypes for Spark compatibility — replace NaN with None for clean Row serialization
    fundamentals_df["symbol"] = fundamentals_df["symbol"].astype(str)
    for col in FUNDAMENTALS_STRING_FIELDS:
        if col in fundamentals_df.columns:
            fundamentals_df[col] = fundamentals_df[col].astype(object).where(fundamentals_df[col].notna(), None)
    for col in FUNDAMENTALS_NUMERIC_FIELDS:
        if col in fundamentals_df.columns:
            fundamentals_df[col] = pd.to_numeric(fundamentals_df[col], errors="coerce")
    fundamentals_df = fundamentals_df.where(pd.notna(fundamentals_df), None)

    # Use Row-based createDataFrame to bypass PySpark Connect ChunkedArray bug
    from pyspark.sql import Row
    rows = [Row(**{k: (None if pd.isna(v) else v) for k, v in row.items()}) for _, row in fundamentals_df.iterrows()]
    spark_df = spark.createDataFrame(rows)
    spark_df.write.mode("overwrite").saveAsTable(TABLE_BRONZE_FUNDAMENTALS)
    print(f"[ingest_fundamentals] Written to {TABLE_BRONZE_FUNDAMENTALS}")


if __name__ == "__main__":
    main()
