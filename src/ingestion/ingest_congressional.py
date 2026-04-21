"""
Bronze layer: Ingest congressional stock trade disclosures.

Sources:
  - Senate Stock Watcher (free, no-auth S3 JSON)
    Ref: https://github.com/timothycarambat/senate-stock-watcher-data
  - Financial Modeling Prep (free tier, House trades)
    Ref: https://site.financialmodelingprep.com/developer/docs/senate-trading-api/

Writes to bronze.congressional_trades (append mode with PIT metadata).

Limitations:
  - STOCK Act: 45-day filing delay (data is never real-time)
  - Amounts are ranges, not exact values (e.g. "$1,001 - $15,000")
  - Unknown tickers appear as "--" in Senate data
  - Senate Stock Watcher covers Senate only; House requires FMP or PDF parsing
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
from common.config import TABLE_BRONZE_CONGRESSIONAL
from common.run_context import new_run_id, now_ts, SOURCE_SYSTEM, LOAD_TYPE

SENATE_S3_URL = (
    "https://senate-stock-watcher-data.s3-us-west-2.amazonaws.com/"
    "aggregate/all_transactions.json"
)

# Amount range midpoints for numerical analysis
AMOUNT_MIDPOINTS = {
    "$1,001 - $15,000": 8000,
    "$15,001 - $50,000": 32500,
    "$50,001 - $100,000": 75000,
    "$100,001 - $250,000": 175000,
    "$250,001 - $500,000": 375000,
    "$500,001 - $1,000,000": 750000,
    "$1,000,001 - $5,000,000": 3000000,
    "$5,000,001 - $25,000,000": 15000000,
    "Over $50,000,000": 50000000,
}


def fetch_senate_trades() -> pd.DataFrame | None:
    """Fetch all Senate stock trades from the Senate Stock Watcher S3 bucket."""
    try:
        import requests
        resp = requests.get(SENATE_S3_URL, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        if not data:
            print("[ingest_congressional] WARNING: Empty Senate data")
            return None
        df = pd.DataFrame(data)
        print(f"[ingest_congressional] Senate: {len(df)} transactions")
        return df
    except Exception as e:
        print(f"[ingest_congressional] ERROR fetching Senate data: {e}")
        return None


def normalize_senate_trades(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize Senate Stock Watcher JSON to our bronze schema."""
    # Rename columns to snake_case
    df = df.rename(columns={
        "first_name": "first_name",
        "last_name": "last_name",
        "office": "office",
        "ptr_link": "filing_url",
        "date_recieved": "disclosure_date_raw",  # typo in source schema
        "transaction_date": "transaction_date_raw",
        "owner": "owner",
        "ticker": "ticker_raw",
        "asset_description": "asset_description",
        "asset_type": "asset_type",
        "type": "trade_type_raw",
        "amount": "amount_range",
        "comment": "comment",
    })

    # Chamber
    df["chamber"] = "Senate"

    # Parse dates (source uses MM/DD/YYYY)
    for col in ["transaction_date_raw", "disclosure_date_raw"]:
        df[col] = pd.to_datetime(df[col], format="mixed", errors="coerce")

    df["transaction_date"] = df["transaction_date_raw"].dt.strftime("%Y-%m-%d")
    df["disclosure_date"] = df["disclosure_date_raw"].dt.strftime("%Y-%m-%d")

    # Clean ticker: "--" means unknown
    df["symbol"] = df["ticker_raw"].replace({"--": None})

    # Normalize trade type
    df["trade_type"] = df["trade_type_raw"].str.strip().str.lower()
    df["trade_type"] = df["trade_type"].map({
        "purchase": "buy",
        "sale": "sell",
        "sale (full)": "sell_full",
        "exchange": "exchange",
    }).fillna(df["trade_type"])

    # Compute amount midpoint
    df["amount_midpoint"] = df["amount_range"].map(AMOUNT_MIDPOINTS)

    # Build office_name: "Last, First (Senator)"
    df["office_name"] = df["last_name"] + ", " + df["first_name"]

    # Select final columns
    result = df[[
        "symbol", "transaction_date", "disclosure_date",
        "chamber", "office_name", "owner", "trade_type",
        "amount_range", "amount_midpoint",
        "asset_description", "asset_type", "filing_url", "comment",
    ]].copy()

    return result


def main():
    spark = SparkSession.builder.getOrCreate()
    run_id = new_run_id()
    ingest_ts = now_ts()

    print(f"[ingest_congressional] Run ID: {run_id}")

    # Fetch Senate trades
    senate_df = fetch_senate_trades()

    if senate_df is None or senate_df.empty:
        print("[ingest_congressional] WARNING: No Senate data fetched.")
        # Don't raise — allow pipeline to continue without congressional data
        return

    # Normalize
    normalized = normalize_senate_trades(senate_df)

    # Filter to tickers in our watchlist (optional — keep all for flexibility)
    # We keep all trades so future ticker additions have historical data

    # Add PIT metadata
    normalized["_run_id"] = run_id
    normalized["_ingest_ts"] = ingest_ts
    normalized["_source_system"] = "senate_stock_watcher"
    normalized["_source_event_ts"] = normalized["transaction_date"].apply(
        lambda d: f"{d}T00:00:00Z" if pd.notna(d) and d else None
    )
    normalized["_load_type"] = LOAD_TYPE

    # Clean for Spark
    normalized = normalized.where(pd.notna(normalized), None)
    for col in normalized.columns:
        if normalized[col].dtype == object:
            normalized[col] = normalized[col].astype(object).where(
                normalized[col].notna(), None)

    # Convert to Spark DataFrame via Row (bypass ChunkedArray bug)
    from pyspark.sql import Row
    rows = [Row(**{k: (None if isinstance(v, float) and pd.isna(v) else v)
                   for k, v in row.items()})
            for _, row in normalized.iterrows()]
    spark_df = spark.createDataFrame(rows)

    # Cast date columns
    for date_col in ["transaction_date", "disclosure_date"]:
        if date_col in spark_df.columns:
            spark_df = spark_df.withColumn(
                date_col, spark_df[date_col].cast("date"))

    # Cast amount_midpoint to double
    if "amount_midpoint" in spark_df.columns:
        spark_df = spark_df.withColumn(
            "amount_midpoint", spark_df["amount_midpoint"].cast("double"))

    spark_df.write.mode("append").option("mergeSchema", "true").saveAsTable(
        TABLE_BRONZE_CONGRESSIONAL)
    print(f"[ingest_congressional] Appended {len(normalized)} rows to "
          f"{TABLE_BRONZE_CONGRESSIONAL}")


if __name__ == "__main__":
    main()