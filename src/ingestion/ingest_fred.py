"""
Bronze layer: Ingest macro indicators from FRED (Federal Reserve Economic Data).

Series:
  - DGS10 / DGS2  — 10yr / 2yr Treasury yield (yield curve spread = DGS10 - DGS2)
  - FEDFUNDS      — Federal Funds Rate
  - UNRATE         — Unemployment Rate
  - CPIAUCSL       — Consumer Price Index (All Urban)

Writes to bronze.macro_indicators (append mode with PIT metadata).

Requires: FRED_API_KEY env var (free at https://fred.stlouisfed.org/docs/api/api_key/)
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
from common.config import TABLE_BRONZE_MACRO
from common.run_context import new_run_id, now_ts, SOURCE_SYSTEM, LOAD_TYPE

FRED_SERIES = {
    "DGS10": "treasury_10y",
    "DGS2": "treasury_2y",
    "FEDFUNDS": "fed_funds_rate",
    "UNRATE": "unemployment_rate",
    "CPIAUCSL": "cpi",
}


def fetch_fred_series(series_id: str, api_key: str) -> pd.DataFrame | None:
    """Fetch a single FRED series as a DataFrame."""
    try:
        from fredapi import Fred
        fred = Fred(api_key=api_key)
        data = fred.get_series(series_id, observation_start="2000-01-01")
        if data.empty:
            print(f"[ingest_fred] WARNING: No data for {series_id}")
            return None
        df = pd.DataFrame({"date": data.index, "value": data.values})
        df["series_id"] = series_id
        df["series_name"] = FRED_SERIES.get(series_id, series_id)
        df["date"] = pd.to_datetime(df["date"]).dt.date
        df = df.dropna(subset=["value"])
        return df
    except Exception as e:
        print(f"[ingest_fred] ERROR fetching {series_id}: {e}")
        return None


def main():
    # FRED_API_KEY: try env var first (local .env), then sys.argv (Databricks job parameter)
    api_key = os.getenv("FRED_API_KEY", "")
    if not api_key and len(sys.argv) > 1:
        api_key = sys.argv[1]
    if not api_key:
        print("[ingest_fred] WARNING: FRED_API_KEY not set. Skipping FRED ingestion.")
        print("[ingest_fred] Get a free key at: https://fred.stlouisfed.org/docs/api/api_key/")
        return

    spark = SparkSession.builder.getOrCreate()
    run_id = new_run_id()
    ingest_ts = now_ts()

    print(f"[ingest_fred] Run ID: {run_id}")
    print(f"[ingest_fred] Fetching {len(FRED_SERIES)} FRED series...")

    frames = []
    for series_id, series_name in FRED_SERIES.items():
        df = fetch_fred_series(series_id, api_key)
        if df is not None:
            df["_run_id"] = run_id
            df["_ingest_ts"] = ingest_ts
            df["_source_system"] = "fred"
            df["_source_event_ts"] = df["date"].apply(lambda d: f"{d}T00:00:00Z")
            df["_load_type"] = LOAD_TYPE
            frames.append(df)
            print(f"[ingest_fred] {series_id} ({series_name}): {len(df)} observations")

    if not frames:
        print("[ingest_fred] WARNING: No FRED data fetched. Exiting without error.")
        return

    macro_df = pd.concat(frames, ignore_index=True)
    macro_df["date"] = pd.to_datetime(macro_df["date"]).dt.strftime("%Y-%m-%d")
    macro_df["value"] = macro_df["value"].astype("float64")
    macro_df = macro_df.where(pd.notna(macro_df), None)

    from pyspark.sql import Row
    rows = [Row(**{k: (None if pd.isna(v) else v) for k, v in row.items()})
            for _, row in macro_df.iterrows()]
    spark_df = spark.createDataFrame(rows)
    spark_df = spark_df.selectExpr(
        "CAST(date AS DATE) as date",
        "value",
        "series_id",
        "series_name",
        "_run_id", "_ingest_ts", "_source_system", "_source_event_ts", "_load_type"
    )
    spark_df.write.mode("append").option("mergeSchema", "true").saveAsTable(TABLE_BRONZE_MACRO)
    print(f"[ingest_fred] Appended {len(macro_df)} rows to {TABLE_BRONZE_MACRO}")


if __name__ == "__main__":
    main()