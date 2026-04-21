"""
Silver layer: Validate and deduplicate stock split events.

Cleans bronze.stock_splits by:
  - Removing rows with null symbols or null split dates
  - Validating split_ratio > 0
  - Computing price adjustment factors
  - Deduplicating on (symbol, split_date) — keep latest run

Writes to silver.stock_splits via MERGE on (symbol, split_date).

Price adjustment factors:
  - pre_split_price_factor = 1 / split_ratio
    Multiply pre-split prices by this to get post-split equivalent
    (e.g., for a 2:1 split, pre-split price of $200 * 0.5 = $100 post-split)
  - post_split_price_factor = split_ratio
    Multiply post-split prices by this to get pre-split equivalent
    (e.g., for a 2:1 split, post-split price of $100 * 2 = $200 pre-split)
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
from common.config import TABLE_BRONZE_SPLITS, TABLE_SILVER_SPLITS


def main():
    spark = SparkSession.builder.getOrCreate()

    print("[build_silver_splits] Building silver stock splits...")

    try:
        bronze_df = spark.table(TABLE_BRONZE_SPLITS).toPandas()
    except Exception as e:
        print(f"[build_silver_splits] WARNING: Could not read bronze table: {e}")
        print("[build_silver_splits] Skipping — no split data yet.")
        return

    if bronze_df.empty:
        print("[build_silver_splits] No data in bronze. Skipping.")
        return

    print(f"[build_silver_splits] Bronze: {len(bronze_df)} rows")

    # Validation
    silver_df = bronze_df.dropna(subset=["symbol", "split_date"]).copy()
    silver_df = silver_df[silver_df["split_ratio"] > 0]

    # Compute price adjustment factors
    silver_df["pre_split_price_factor"] = 1.0 / silver_df["split_ratio"]
    silver_df["post_split_price_factor"] = silver_df["split_ratio"]

    # Deduplicate: keep latest run_id per (symbol, split_date)
    silver_df = (silver_df
        .sort_values("_run_id")
        .drop_duplicates(subset=["symbol", "split_date"], keep="last"))

    # Drop PIT metadata
    for col in ["_run_id", "_ingest_ts", "_source_system", "_source_event_ts", "_load_type"]:
        if col in silver_df.columns:
            silver_df = silver_df.drop(columns=[col])

    print(f"[build_silver_splits] Silver: {len(silver_df)} split events, "
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

    for date_col in ["split_date"]:
        if date_col in spark_df.columns:
            spark_df = spark_df.withColumn(
                date_col, spark_df[date_col].cast("date"))
    for double_col in ["split_ratio", "pre_split_price_factor", "post_split_price_factor"]:
        if double_col in spark_df.columns:
            spark_df = spark_df.withColumn(
                double_col, spark_df[double_col].cast("double"))

    # Create or MERGE
    table_exists = spark.catalog.tableExists(TABLE_SILVER_SPLITS)

    if not table_exists:
        spark_df.createOrReplaceTempView("splits_schema")
        spark.sql(f"""
            CREATE TABLE {TABLE_SILVER_SPLITS}
            USING DELTA
            AS SELECT * FROM splits_schema
        """)
        try:
            spark.sql(f"ALTER TABLE {TABLE_SILVER_SPLITS} "
                      f"CLUSTER BY (symbol, split_date)")
        except Exception:
            pass
        print(f"[build_silver_splits] Created {TABLE_SILVER_SPLITS}")
    else:
        spark_df.createOrReplaceTempView("splits_updates")
        all_cols = spark_df.columns
        update_set = ", ".join(
            f"t.`{c}` = s.`{c}`" for c in all_cols
            if c not in ("symbol", "split_date"))
        insert_cols = ", ".join(f"`{c}`" for c in all_cols)
        insert_vals = ", ".join(f"s.`{c}`" for c in all_cols)

        spark.sql(f"""
            MERGE INTO {TABLE_SILVER_SPLITS} t
            USING splits_updates s
            ON t.symbol = s.symbol AND t.split_date = s.split_date
            WHEN MATCHED THEN UPDATE SET {update_set}
            WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
        """)
        print(f"[build_silver_splits] Merged {len(silver_df)} rows")


if __name__ == "__main__":
    main()