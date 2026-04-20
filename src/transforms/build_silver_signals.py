"""
Silver layer: Compute full technical indicator matrix for each ticker.

Reads from Silver daily_prices (canonical deduped), runs all indicator
calculations, writes to silver.daily_signals via MERGE on (symbol, as_of_date).

Uses Silver fundamentals_current for fundamental data (PE, EPS, etc.).
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
    TABLE_SILVER_PRICES,
    TABLE_SILVER_FUNDAMENTALS,
    TABLE_SILVER_SIGNALS,
)
from common.indicators import build_signal_row


def main():
    spark = SparkSession.builder.getOrCreate()
    today = date.today()

    print(f"[build_silver_signals] Building signals for {today}...")

    # Read from Silver prices (canonical deduped) instead of raw Bronze
    prices_df = spark.table(TABLE_SILVER_PRICES).toPandas()
    print(f"[build_silver_signals] Silver prices: {len(prices_df)} rows, "
          f"{prices_df['symbol'].nunique()} tickers")

    try:
        fundamentals_df = spark.table(TABLE_SILVER_FUNDAMENTALS).toPandas()
        print(f"[build_silver_signals] Silver fundamentals: {len(fundamentals_df)} rows")
    except Exception as e:
        print(f"[build_silver_signals] WARNING: Could not read fundamentals: {e}")
        print(f"[build_silver_signals] Proceeding without fundamentals data.")
        fundamentals_df = pd.DataFrame()

    rows = []
    errors = []
    skipped = []

    for symbol in TICKERS:
        try:
            ticker_prices = prices_df[prices_df["symbol"] == symbol].copy()

            if ticker_prices.empty:
                skipped.append((symbol, "no price data in Silver"))
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
            print(f"[build_silver_signals] ERROR processing {symbol}: {e}")

    if not rows:
        raise RuntimeError(
            f"[build_silver_signals] FATAL: No signals computed. "
            f"Errors: {errors}. Skipped: {skipped}"
        )

    silver_df = pd.DataFrame(rows)

    expected_cols = set(rows[0].keys())
    for i, row in enumerate(rows):
        if set(row.keys()) != expected_cols:
            diff = set(row.keys()).symmetric_difference(expected_cols)
            raise RuntimeError(
                f"[build_silver_signals] FATAL: Row {i} ({row.get('symbol')}) has "
                f"inconsistent columns. Diff: {diff}"
            )

    print(f"[build_silver_signals] Computed signals for {len(silver_df)} tickers")
    print(f"[build_silver_signals] Skipped: {len(skipped)}, Errors: {len(errors)}")
    if skipped:
        print(f"[build_silver_signals] Skipped tickers: {skipped}")
    if errors:
        print(f"[build_silver_signals] Errors: {errors}")

    # Ensure clean dtypes for Spark serialization
    if "as_of_date" in silver_df.columns:
        silver_df["as_of_date"] = silver_df["as_of_date"].astype(str)
    for col in silver_df.columns:
        if silver_df[col].dtype == object:
            silver_df[col] = silver_df[col].astype(object).where(silver_df[col].notna(), None)
    silver_df = silver_df.where(pd.notna(silver_df), None)

    # Use Row-based createDataFrame to bypass PySpark Connect ChunkedArray bug
    from pyspark.sql import Row
    spark_rows = [Row(**{k: (None if isinstance(v, float) and pd.isna(v) else v) for k, v in row.items()})
                  for _, row in silver_df.iterrows()]
    spark_df = spark.createDataFrame(spark_rows)
    # Cast date string back to proper date type
    if "as_of_date" in spark_df.columns:
        spark_df = spark_df.withColumn("as_of_date", spark_df["as_of_date"].cast("date"))

    # Create table with Liquid Clustering if not exists, or migrate schema if needed
    table_exists = spark.catalog.tableExists(TABLE_SILVER_SIGNALS)

    if table_exists:
        # Check if existing table has all columns — add missing ones
        existing_cols = {col.name for col in spark.table(TABLE_SILVER_SIGNALS).schema.fields}
        new_cols = set(silver_df.columns) - existing_cols
        if new_cols:
            print(f"[build_silver_signals] Adding {len(new_cols)} new columns: {new_cols}")
            for col in new_cols:
                try:
                    spark.sql(f"ALTER TABLE {TABLE_SILVER_SIGNALS} ADD COLUMN `{col}` DOUBLE")
                except Exception as e:
                    print(f"[build_silver_signals] Could not add column {col}: {e}")

    if not table_exists:
        # Infer schema from the DataFrame and create with clustering
        spark_df.createOrReplaceTempView("signals_schema_infer")
        cols = spark_df.columns
        col_exprs = []
        for c in cols:
            col_exprs.append(f"`{c}`")
        spark.sql(f"""
            CREATE TABLE {TABLE_SILVER_SIGNALS}
            USING DELTA
            AS SELECT * FROM signals_schema_infer
        """)
        # Add Liquid Clustering (requires ALTER TABLE for existing table)
        try:
            spark.sql(f"ALTER TABLE {TABLE_SILVER_SIGNALS} CLUSTER BY (symbol, as_of_date)")
        except Exception as e:
            print(f"[build_silver_signals] Note: Could not add Liquid Clustering: {e}")
        print(f"[build_silver_signals] Created {TABLE_SILVER_SIGNALS}")
    else:
        # MERGE on (symbol, as_of_date) — update existing, insert new
        spark_df.createOrReplaceTempView("signals_updates")

        # Build column lists for MERGE
        all_cols = spark_df.columns
        update_set = ", ".join(f"t.`{c}` = s.`{c}`" for c in all_cols if c not in ("symbol", "as_of_date"))
        insert_cols = ", ".join(f"`{c}`" for c in all_cols)
        insert_vals = ", ".join(f"s.`{c}`" for c in all_cols)

        spark.sql(f"""
            MERGE INTO {TABLE_SILVER_SIGNALS} t
            USING signals_updates s
            ON t.symbol = s.symbol AND t.as_of_date = s.as_of_date
            WHEN MATCHED THEN UPDATE SET {update_set}
            WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
        """)

    print(f"[build_silver_signals] Written {len(silver_df)} rows to {TABLE_SILVER_SIGNALS} (MERGE)")


if __name__ == "__main__":
    main()