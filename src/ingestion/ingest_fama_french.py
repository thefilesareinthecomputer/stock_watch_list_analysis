"""
Bronze layer: Ingest Fama-French factor data.

Downloads daily 5-factor data (Mkt-RF, SMB, HML, RMW, CMA) and
momentum factor (UMD) from Ken French's data library at Dartmouth.

Writes to bronze.factor_returns (append mode with PIT metadata).

Ref: French, "Fama-French Factors" (Dartmouth Tuck School).
URL: https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/data_library.html
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
from common.config import TABLE_BRONZE_FACTORS
from common.run_context import new_run_id, now_ts, LOAD_TYPE

# Fama-French data ZIP URLs (direct from Dartmouth)
FF5_URL = "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/Five_Factors_Daily_CSV.zip"
MOMENTUM_URL = "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/Momentum_Factor_Daily_CSV.zip"


def download_ff_factors(url: str, label: str) -> pd.DataFrame | None:
    """Download and parse a Fama-French CSV from the Dartmouth ZIP."""
    try:
        # Read the CSV directly from the ZIP (pandas handles ZIP)
        df = pd.read_csv(url, compression="zip", skiprows=13)  # skip header rows
        # Clean: drop the copyright footer rows
        df = df.dropna(subset=[df.columns[0]])
        # First column is the date as YYYYMMDD integer
        df = df.rename(columns={df.columns[0]: "date"})
        df["date"] = pd.to_datetime(df["date"], format="%Y%m%d", errors="coerce")
        df = df.dropna(subset=["date"])
        # Strip whitespace from column names
        df.columns = [c.strip() for c in df.columns]
        # Convert factor columns to float
        for col in df.columns:
            if col != "date":
                df[col] = pd.to_numeric(df[col], errors="coerce")
        print(f"[ingest_fama_french] {label}: {len(df)} rows, columns: {list(df.columns)}")
        return df
    except Exception as e:
        print(f"[ingest_fama_french] ERROR downloading {label}: {e}")
        return None


def main():
    spark = SparkSession.builder.getOrCreate()
    run_id = new_run_id()
    ingest_ts = now_ts()

    print(f"[ingest_fama_french] Run ID: {run_id}")
    print("[ingest_fama_french] Downloading Fama-French 5-factor data...")

    ff5 = download_ff_factors(FF5_URL, "5-Factor")
    mom = download_ff_factors(MOMENTUM_URL, "Momentum")

    if ff5 is None and mom is None:
        print("[ingest_fama_french] WARNING: No factor data downloaded. Exiting.")
        return

    # Merge 5-factor + momentum on date
    if ff5 is not None and mom is not None:
        # Standardize column names — momentum file has different naming
        mom_cols = {c: c.strip() for c in mom.columns}
        mom = mom.rename(columns=mom_cols)
        # Merge on date
        factors_df = ff5.merge(mom, on="date", how="outer")
    elif ff5 is not None:
        factors_df = ff5
    else:
        factors_df = mom

    # Pivot to long format: one row per (date, factor_name, factor_value)
    id_vars = ["date"]
    value_cols = [c for c in factors_df.columns if c != "date"]

    long_rows = []
    for _, row in factors_df.iterrows():
        for col in value_cols:
            val = row[col]
            if pd.notna(val):
                # FF factors are in % — convert to decimal
                long_rows.append({
                    "date": row["date"],
                    "factor_name": col,
                    "factor_value": val / 100.0,
                })

    long_df = pd.DataFrame(long_rows)

    # Add PIT metadata
    long_df["_run_id"] = run_id
    long_df["_ingest_ts"] = ingest_ts
    long_df["_source_system"] = "fama_french"
    long_df["_source_event_ts"] = long_df["date"].apply(
        lambda d: f"{d.strftime('%Y-%m-%d')}T00:00:00Z" if pd.notna(d) else None
    )
    long_df["_load_type"] = LOAD_TYPE

    long_df["date"] = pd.to_datetime(long_df["date"]).dt.strftime("%Y-%m-%d")
    long_df["factor_value"] = long_df["factor_value"].astype("float64")
    long_df = long_df.where(pd.notna(long_df), None)

    from pyspark.sql import Row
    rows = [Row(**{k: (None if pd.isna(v) else v) for k, v in row.items()})
            for _, row in long_df.iterrows()]
    spark_df = spark.createDataFrame(rows)
    spark_df = spark_df.selectExpr(
        "CAST(date AS DATE) as date",
        "factor_name",
        "factor_value",
        "_run_id", "_ingest_ts", "_source_system", "_source_event_ts", "_load_type"
    )
    spark_df.write.mode("append").option("mergeSchema", "true").saveAsTable(TABLE_BRONZE_FACTORS)
    print(f"[ingest_fama_french] Appended {len(long_df)} rows to {TABLE_BRONZE_FACTORS}")


if __name__ == "__main__":
    main()