"""
Silver layer: Cleanse and deduplicate congressional trade disclosures.

Validates ticker symbols, normalizes trade types, deduplicates on
(symbol, transaction_date, office_name, trade_type).

Writes to silver.congressional_trades via MERGE on (symbol, transaction_date, office_name, trade_type).
"""
import sys
import os
try:
    _src_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
except NameError:
    _src_dir = os.path.join(os.getcwd(), "..")
sys.path.insert(0, os.path.normpath(_src_dir))

import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from common.config import TABLE_BRONZE_CONGRESSIONAL, TABLE_SILVER_CONGRESSIONAL


def main():
    spark = SparkSession.builder.getOrCreate()

    print("[build_silver_congressional] Building silver congressional trades...")

    try:
        bronze_df = spark.table(TABLE_BRONZE_CONGRESSIONAL).toPandas()
    except Exception as e:
        print(f"[build_silver_congressional] WARNING: Could not read bronze table: {e}")
        print("[build_silver_congressional] Skipping — no congressional data yet.")
        return

    if bronze_df.empty:
        print("[build_silver_congressional] No data in bronze. Skipping.")
        return

    print(f"[build_silver_congressional] Bronze: {len(bronze_df)} rows")

    # ── Validation ──
    # Drop rows with no ticker (unknown symbol)
    silver_df = bronze_df.dropna(subset=["symbol"]).copy()
    unknown_pct = (1 - len(silver_df) / len(bronze_df)) * 100
    if unknown_pct > 0:
        print(f"[build_silver_congressional] Dropped {unknown_pct:.1f}% rows with unknown tickers")

    # Drop rows with no transaction date
    silver_df = silver_df.dropna(subset=["transaction_date"])

    # Normalize trade type
    valid_types = {"buy", "sell", "sell_full", "exchange"}
    silver_df = silver_df[silver_df["trade_type"].isin(valid_types)]

    # Deduplicate: keep latest _run_id for duplicate (symbol, transaction_date, office_name, trade_type)
    silver_df = (silver_df
        .sort_values("_run_id")
        .drop_duplicates(
            subset=["symbol", "transaction_date", "office_name", "trade_type"],
            keep="last"
        ))

    # Drop PIT metadata from bronze (silver has its own lineage)
    for col in ["_run_id", "_ingest_ts", "_source_system", "_source_event_ts", "_load_type"]:
        if col in silver_df.columns:
            silver_df = silver_df.drop(columns=[col])

    print(f"[build_silver_congressional] Silver: {len(silver_df)} rows, "
          f"{silver_df['symbol'].nunique()} tickers")

    # Clean for Spark
    silver_df = silver_df.where(pd.notna(silver_df), None)
    for col in silver_df.columns:
        if silver_df[col].dtype == object:
            silver_df[col] = silver_df[col].astype(object).where(
                silver_df[col].notna(), None)

    from pyspark.sql import Row
    spark_rows = [Row(**{k: (None if isinstance(v, float) and pd.isna(v) else v)
                         for k, v in row.items()})
                  for _, row in silver_df.iterrows()]
    spark_df = spark.createDataFrame(spark_rows)

    # Cast types
    for date_col in ["transaction_date", "disclosure_date"]:
        if date_col in spark_df.columns:
            spark_df = spark_df.withColumn(
                date_col, spark_df[date_col].cast("date"))
    if "amount_midpoint" in spark_df.columns:
        spark_df = spark_df.withColumn(
            "amount_midpoint", spark_df["amount_midpoint"].cast("double"))

    # Create or MERGE
    table_exists = spark.catalog.tableExists(TABLE_SILVER_CONGRESSIONAL)

    if not table_exists:
        spark_df.createOrReplaceTempView("congressional_schema")
        spark.sql(f"""
            CREATE TABLE {TABLE_SILVER_CONGRESSIONAL}
            USING DELTA
            AS SELECT * FROM congressional_schema
        """)
        try:
            spark.sql(f"ALTER TABLE {TABLE_SILVER_CONGRESSIONAL} "
                      f"CLUSTER BY (symbol, transaction_date)")
        except Exception:
            pass
        print(f"[build_silver_congressional] Created {TABLE_SILVER_CONGRESSIONAL}")
    else:
        spark_df.createOrReplaceTempView("congressional_updates")
        all_cols = spark_df.columns
        key_cols = {"symbol", "transaction_date", "office_name", "trade_type"}
        update_set = ", ".join(
            f"t.`{c}` = s.`{c}`" for c in all_cols if c not in key_cols)
        insert_cols = ", ".join(f"`{c}`" for c in all_cols)
        insert_vals = ", ".join(f"s.`{c}`" for c in all_cols)

        spark.sql(f"""
            MERGE INTO {TABLE_SILVER_CONGRESSIONAL} t
            USING congressional_updates s
            ON t.symbol = s.symbol
               AND t.transaction_date = s.transaction_date
               AND t.office_name = s.office_name
               AND t.trade_type = s.trade_type
            WHEN MATCHED THEN UPDATE SET {update_set}
            WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
        """)
        print(f"[build_silver_congressional] Merged {len(silver_df)} rows")


if __name__ == "__main__":
    main()