"""
Gold layer: Security dimension with SCD2 tracking.

Creates gold.dim_security — slowly-changing dimension for security attributes.
Tracks: short_name, long_name, sector, industry, exchange, country, currency.
Does NOT track daily measures (PE, EPS, market_cap) — those belong in fact tables.

SCD2 pattern: MERGE INTO with NULL merge_key trick for upsert.
Only creates new version when tracked attributes actually change.

Includes "UNKNOWN" member row (security_key = -1) for unmatched foreign keys.
"""
import sys
import os
try:
    _src_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
except NameError:
    _src_dir = os.path.join(os.getcwd(), "..")
sys.path.insert(0, os.path.normpath(_src_dir))

import hashlib
import json
from datetime import date
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, BooleanType
)
from common.config import (
    TICKERS,
    TABLE_SILVER_FUNDAMENTALS,
    TABLE_GOLD_DIM_SECURITY,
)

# SCD2-tracked attributes (slowly-changing — sector reclassification, name changes)
SCD2_ATTRIBUTES = ["shortName", "longName", "sector", "industry", "exchange", "country", "currency"]

# Explicit schema to avoid CANNOT_DETERMINE_TYPE when all values are None
SECURITY_DIM_SCHEMA = StructType([
    StructField("symbol", StringType(), False),
    *[StructField(attr, StringType(), True) for attr in SCD2_ATTRIBUTES],
    StructField("attr_hash", StringType(), True),
    StructField("effective_start_date", StringType(), True),  # string date, cast via SQL
    StructField("effective_end_date", StringType(), True),    # string date, cast via SQL
    StructField("is_current", BooleanType(), True),
])


def compute_attr_hash(row: dict) -> str:
    """Hash SCD2 attributes to detect real changes."""
    values = {k: row.get(k) for k in SCD2_ATTRIBUTES}
    raw = json.dumps(values, sort_keys=True, default=str)
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def main():
    spark = SparkSession.builder.getOrCreate()
    today = date.today().isoformat()

    print("[build_dim_security] Building security dimension...")

    # Read current fundamentals from Silver for SCD2 attributes
    try:
        fundamentals_df = spark.table(TABLE_SILVER_FUNDAMENTALS).toPandas()
    except Exception as e:
        print(f"[build_dim_security] WARNING: Could not read Silver fundamentals: {e}")
        print("[build_dim_security] Creating minimal dimension from tickers only.")
        fundamentals_df = pd.DataFrame()

    # Build new dimension rows from current data
    new_rows = []
    for symbol in TICKERS:
        row = {"symbol": symbol}
        for attr in SCD2_ATTRIBUTES:
            row[attr] = None

        if not fundamentals_df.empty and "symbol" in fundamentals_df.columns:
            matches = fundamentals_df[fundamentals_df["symbol"] == symbol]
            if not matches.empty:
                rec = matches.iloc[0].to_dict()
                for attr in SCD2_ATTRIBUTES:
                    val = rec.get(attr)
                    row[attr] = str(val) if val and str(val) != "nan" else None

        row["attr_hash"] = compute_attr_hash(row)
        row["effective_start_date"] = today
        row["effective_end_date"] = None
        row["is_current"] = True
        new_rows.append(row)

    updates_df = pd.DataFrame(new_rows)
    updates_df = updates_df.where(pd.notna(updates_df), None)

    # Create temp view for MERGE
    from pyspark.sql import Row
    spark_rows = [Row(**{k: (None if pd.isna(v) else v) for k, v in row.items()})
                  for _, row in updates_df.iterrows()]
    spark_updates = spark.createDataFrame(spark_rows, schema=SECURITY_DIM_SCHEMA)
    spark_updates.createOrReplaceTempView("security_updates")

    table_exists = spark.catalog.tableExists(TABLE_GOLD_DIM_SECURITY)

    if not table_exists:
        # First run: create table with IDENTITY column + UNKNOWN member row
        spark.sql(f"""
            CREATE TABLE {TABLE_GOLD_DIM_SECURITY}
            (
                security_key        BIGINT GENERATED ALWAYS AS IDENTITY,
                symbol              STRING  NOT NULL,
                shortName           STRING,
                longName            STRING,
                sector              STRING,
                industry            STRING,
                exchange            STRING,
                country             STRING,
                currency            STRING,
                attr_hash           STRING,
                effective_start_date DATE,
                effective_end_date   DATE,
                is_current          BOOLEAN
            )
            USING DELTA
            CLUSTER BY (symbol)
        """)

        # Insert UNKNOWN member row
        spark.sql(f"""
            INSERT INTO {TABLE_GOLD_DIM_SECURITY}
            (symbol, shortName, longName, sector, industry, exchange,
             country, currency, attr_hash, effective_start_date, effective_end_date, is_current)
            VALUES
            ('UNKNOWN', 'Unknown', 'Unknown Security', 'Unknown', 'Unknown',
             'Unknown', 'Unknown', 'Unknown', 'unknown', '2000-01-01', NULL, true)
        """)
        print(f"[build_dim_security] Created {TABLE_GOLD_DIM_SECURITY} with UNKNOWN member row")

        # Insert all current securities
        spark.sql(f"""
            INSERT INTO {TABLE_GOLD_DIM_SECURITY}
            (symbol, shortName, longName, sector, industry, exchange,
             country, currency, attr_hash, effective_start_date, effective_end_date, is_current)
            SELECT symbol, shortName, longName, sector, industry, exchange,
                   country, currency, attr_hash,
                   CAST(effective_start_date AS DATE),
                   CAST(effective_end_date AS DATE),
                   is_current
            FROM security_updates
        """)
        print(f"[build_dim_security] Inserted {len(new_rows)} security rows")
    else:
        # SCD2 MERGE: expire old versions where attr_hash changed, insert new
        # Step 1: Close out changed current rows
        spark.sql(f"""
            MERGE INTO {TABLE_GOLD_DIM_SECURITY} t
            USING security_updates s
            ON t.symbol = s.symbol AND t.is_current = true
            WHEN MATCHED AND t.attr_hash != s.attr_hash THEN
                UPDATE SET
                    t.effective_end_date = CAST(s.effective_start_date AS DATE),
                    t.is_current = false
        """)

        # Step 2: Insert new current versions (for changed + new symbols)
        spark.sql(f"""
            MERGE INTO {TABLE_GOLD_DIM_SECURITY} t
            USING security_updates s
            ON t.symbol = s.symbol AND t.attr_hash = s.attr_hash AND t.is_current = true
            WHEN NOT MATCHED THEN
                INSERT (symbol, shortName, longName, sector, industry, exchange,
                        country, currency, attr_hash, effective_start_date,
                        effective_end_date, is_current)
                VALUES (s.symbol, s.shortName, s.longName, s.sector, s.industry,
                        s.exchange, s.country, s.currency, s.attr_hash,
                        CAST(s.effective_start_date AS DATE),
                        CAST(s.effective_end_date AS DATE), s.is_current)
        """)
        print(f"[build_dim_security] SCD2 MERGE applied for {len(new_rows)} securities")

    count = spark.table(TABLE_GOLD_DIM_SECURITY).count()
    print(f"[build_dim_security] {TABLE_GOLD_DIM_SECURITY}: {count} total rows")


if __name__ == "__main__":
    main()