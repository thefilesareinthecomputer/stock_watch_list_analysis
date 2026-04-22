"""
Bronze layer: Ingest company fundamental data from yfinance.
Writes to stock_analytics.bronze.company_fundamentals as a Delta table.

Mode: APPEND with SCD2 versioning.

Fundamentals are beliefs, not facts — PE, EPS, analyst targets get revised.
Each new assertion creates a new SCD2 version, never overwriting the previous.
The attr_hash column suppresses phantom versions from unchanged yfinance replays.

SCD2 columns:
  effective_from  — when this version became current (ingest timestamp)
  effective_to    — when this version was superseded (NULL = current)
  is_current      — whether this is the active version
  attr_hash       — hash of mutable business attributes (changes = new version)

Point-in-time metadata on every row:
  _run_id, _ingest_ts, _source_system, _source_event_ts, _load_type
"""
import sys
import os
try:
    _src_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
except NameError:
    _src_dir = os.path.join(os.getcwd(), "..")
sys.path.insert(0, os.path.normpath(_src_dir))

import time
import hashlib
import json
import numpy as np
import pandas as pd
import yfinance as yf
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, BooleanType
)
from common.config import (
    TICKERS,
    TABLE_BRONZE_FUNDAMENTALS,
    FUNDAMENTALS_NUMERIC_FIELDS,
    FUNDAMENTALS_STRING_FIELDS,
)
from common.run_context import new_run_id, now_ts, SOURCE_SYSTEM, LOAD_TYPE


# SCD2-tracked attributes: slowly-changing business attributes that
# warrant a new version when they change (sector reclassification,
# name changes, etc.). NOT daily measures like PE/EPS — those are
# facts that belong in fact tables.
SCD2_TRACKED_FIELDS = [
    "shortName", "longName", "sector", "industry", "country",
    "currency", "exchange",
]


def compute_attr_hash(row: dict) -> str:
    """Hash SCD2-tracked attributes to detect real changes."""
    values = {k: row.get(k) for k in SCD2_TRACKED_FIELDS}
    raw = json.dumps(values, sort_keys=True, default=str)
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def build_fundamentals_schema() -> StructType:
    """Build explicit Spark schema for the fundamentals table."""
    fields = [StructField("symbol", StringType(), False)]
    for col in FUNDAMENTALS_STRING_FIELDS:
        fields.append(StructField(col, StringType(), True))
    for col in FUNDAMENTALS_NUMERIC_FIELDS:
        fields.append(StructField(col, DoubleType(), True))
    # SCD2 columns
    fields.append(StructField("effective_from", StringType(), True))
    fields.append(StructField("effective_to", StringType(), True))
    fields.append(StructField("is_current", BooleanType(), True))
    fields.append(StructField("attr_hash", StringType(), True))
    # PIT metadata
    fields.append(StructField("_run_id", StringType(), False))
    fields.append(StructField("_ingest_ts", StringType(), False))
    fields.append(StructField("_source_system", StringType(), False))
    fields.append(StructField("_source_event_ts", StringType(), True))
    fields.append(StructField("_load_type", StringType(), False))
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


def apply_scd2(spark, new_rows: list[dict], run_id: str, ingest_ts: str):
    """
    Apply SCD2 MERGE logic:
    1. For each symbol with new data, compare attr_hash to current version
    2. If hash changed: expire old version (set effective_to, is_current=false),
       insert new version (effective_from=now, effective_to=NULL, is_current=true)
    3. If hash unchanged: skip (no phantom version)
    """
    from pyspark.sql import Row

    if not new_rows:
        return

    # Add SCD2 and PIT columns to new rows
    for row in new_rows:
        row["attr_hash"] = compute_attr_hash(row)
        row["effective_from"] = ingest_ts
        row["effective_to"] = None
        row["is_current"] = True
        row["_run_id"] = run_id
        row["_ingest_ts"] = ingest_ts
        row["_source_system"] = SOURCE_SYSTEM
        row["_source_event_ts"] = ingest_ts  # fundamentals snapshot time
        row["_load_type"] = LOAD_TYPE

    column_order = (
        ["symbol"]
        + FUNDAMENTALS_STRING_FIELDS
        + FUNDAMENTALS_NUMERIC_FIELDS
        + ["effective_from", "effective_to", "is_current", "attr_hash"]
        + ["_run_id", "_ingest_ts", "_source_system", "_source_event_ts", "_load_type"]
    )

    updates_df = pd.DataFrame(new_rows, columns=column_order)

    # Cast dtypes for Spark compatibility
    updates_df["symbol"] = updates_df["symbol"].astype(str)
    for col in FUNDAMENTALS_STRING_FIELDS:
        if col in updates_df.columns:
            updates_df[col] = updates_df[col].astype(object).where(updates_df[col].notna(), None)
    for col in FUNDAMENTALS_NUMERIC_FIELDS:
        if col in updates_df.columns:
            updates_df[col] = pd.to_numeric(updates_df[col], errors="coerce")
    updates_df = updates_df.where(pd.notna(updates_df), None)

    # Row-based createDataFrame to bypass ChunkedArray bug
    rows = [Row(**{k: (None if pd.isna(v) else v) for k, v in row.items()})
            for _, row in updates_df.iterrows()]
    schema = build_fundamentals_schema()
    spark_df = spark.createDataFrame(rows, schema=schema)

    # Create temp view for MERGE
    spark_df.createOrReplaceTempView("fundamentals_updates")

    # SCD2 MERGE: expire old versions where attr_hash changed, insert new versions
    spark.sql(f"""
        MERGE INTO {TABLE_BRONZE_FUNDAMENTALS} t
        USING fundamentals_updates s
        ON t.symbol = s.symbol AND t.is_current = true
        WHEN MATCHED AND t.attr_hash != s.attr_hash THEN
            UPDATE SET
                t.effective_to = s.effective_from,
                t.is_current = false
        WHEN NOT MATCHED THEN
            INSERT (
                symbol, {', '.join(FUNDAMENTALS_STRING_FIELDS)},
                {', '.join(FUNDAMENTALS_NUMERIC_FIELDS)},
                effective_from, effective_to, is_current, attr_hash,
                _run_id, _ingest_ts, _source_system, _source_event_ts, _load_type
            )
            VALUES (
                s.symbol, {', '.join(f's.{f}' for f in FUNDAMENTALS_STRING_FIELDS)},
                {', '.join(f's.{f}' for f in FUNDAMENTALS_NUMERIC_FIELDS)},
                s.effective_from, s.effective_to, s.is_current, s.attr_hash,
                s._run_id, s._ingest_ts, s._source_system, s._source_event_ts, s._load_type
            )
    """)

    # Also insert new versions for symbols where hash changed (the expired row was updated,
    # now we need to insert the new current version)
    spark.sql(f"""
        MERGE INTO {TABLE_BRONZE_FUNDAMENTALS} t
        USING fundamentals_updates s
        ON t.symbol = s.symbol AND t._run_id = s._run_id AND t.attr_hash = s.attr_hash
        WHEN NOT MATCHED THEN
            INSERT (
                symbol, {', '.join(FUNDAMENTALS_STRING_FIELDS)},
                {', '.join(FUNDAMENTALS_NUMERIC_FIELDS)},
                effective_from, effective_to, is_current, attr_hash,
                _run_id, _ingest_ts, _source_system, _source_event_ts, _load_type
            )
            VALUES (
                s.symbol, {', '.join(f's.{f}' for f in FUNDAMENTALS_STRING_FIELDS)},
                {', '.join(f's.{f}' for f in FUNDAMENTALS_NUMERIC_FIELDS)},
                s.effective_from, s.effective_to, s.is_current, s.attr_hash,
                s._run_id, s._ingest_ts, s._source_system, s._source_event_ts, s._load_type
            )
    """)


def apply_scd2_first_run(spark, new_rows: list[dict], run_id: str, ingest_ts: str):
    """
    First run when table doesn't exist yet — just write all rows as current.
    """
    from pyspark.sql import Row

    for row in new_rows:
        row["attr_hash"] = compute_attr_hash(row)
        row["effective_from"] = ingest_ts
        row["effective_to"] = None
        row["is_current"] = True
        row["_run_id"] = run_id
        row["_ingest_ts"] = ingest_ts
        row["_source_system"] = SOURCE_SYSTEM
        row["_source_event_ts"] = ingest_ts
        row["_load_type"] = LOAD_TYPE

    column_order = (
        ["symbol"]
        + FUNDAMENTALS_STRING_FIELDS
        + FUNDAMENTALS_NUMERIC_FIELDS
        + ["effective_from", "effective_to", "is_current", "attr_hash"]
        + ["_run_id", "_ingest_ts", "_source_system", "_source_event_ts", "_load_type"]
    )

    fundamentals_df = pd.DataFrame(new_rows, columns=column_order)

    # Cast dtypes for Spark compatibility
    fundamentals_df["symbol"] = fundamentals_df["symbol"].astype(str)
    for col in FUNDAMENTALS_STRING_FIELDS:
        if col in fundamentals_df.columns:
            fundamentals_df[col] = fundamentals_df[col].astype(object).where(fundamentals_df[col].notna(), None)
    for col in FUNDAMENTALS_NUMERIC_FIELDS:
        if col in fundamentals_df.columns:
            fundamentals_df[col] = pd.to_numeric(fundamentals_df[col], errors="coerce")
    fundamentals_df = fundamentals_df.where(pd.notna(fundamentals_df), None)

    rows = [Row(**{k: (None if pd.isna(v) else v) for k, v in row.items()})
            for _, row in fundamentals_df.iterrows()]
    schema = build_fundamentals_schema()
    spark_df = spark.createDataFrame(rows, schema=schema)
    spark_df.write.mode("overwrite").saveAsTable(TABLE_BRONZE_FUNDAMENTALS)
    print(f"[ingest_fundamentals] First run — written {len(fundamentals_df)} rows to {TABLE_BRONZE_FUNDAMENTALS}")


def main():
    spark = SparkSession.builder.getOrCreate()

    run_id = new_run_id()
    ingest_ts = now_ts()

    print(f"[ingest_fundamentals] Run ID: {run_id}")
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

    null_counts = pd.DataFrame(rows)[FUNDAMENTALS_NUMERIC_FIELDS].isna().sum()
    high_null = null_counts[null_counts > len(rows) * 0.5]
    if not high_null.empty:
        print(f"[ingest_fundamentals] WARNING: Fields with >50% nulls: {high_null.to_dict()}")

    # Check if table already exists and has SCD2 columns
    table_exists = spark.catalog.tableExists(TABLE_BRONZE_FUNDAMENTALS)

    if table_exists:
        # Check if the table has SCD2 columns (migration from pre-SCD2 schema)
        existing_cols = [col.name for col in spark.table(TABLE_BRONZE_FUNDAMENTALS).schema.fields]
        has_scd2 = "attr_hash" in existing_cols
        if not has_scd2:
            print(f"[ingest_fundamentals] Table exists but lacks SCD2 columns. Rebuilding with SCD2 schema.")
            spark.sql(f"DROP TABLE IF EXISTS {TABLE_BRONZE_FUNDAMENTALS}")
            table_exists = False
        else:
            # Add any new columns from FUNDAMENTALS_NUMERIC_FIELDS that don't exist yet
            for field in FUNDAMENTALS_NUMERIC_FIELDS:
                if field not in existing_cols:
                    print(f"[ingest_fundamentals] Adding missing column: {field}")
                    spark.sql(f"ALTER TABLE {TABLE_BRONZE_FUNDAMENTALS} ADD COLUMN `{field}` DOUBLE")

    if table_exists:
        apply_scd2(spark, rows, run_id, ingest_ts)
        print(f"[ingest_fundamentals] SCD2 MERGE applied for {len(rows)} tickers to {TABLE_BRONZE_FUNDAMENTALS}")
    else:
        apply_scd2_first_run(spark, rows, run_id, ingest_ts)


if __name__ == "__main__":
    main()