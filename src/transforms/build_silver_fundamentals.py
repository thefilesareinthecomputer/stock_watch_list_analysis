"""
Silver layer: Current-state fundamentals from Bronze SCD2.

Reads Bronze company_fundamentals (SCD2 versioned), filters to
is_current = true, writes to silver.fundamentals_current via MERGE.

Grain: One row per symbol (current snapshot).
"""
import sys
import os
try:
    _src_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
except NameError:
    _src_dir = os.path.join(os.getcwd(), "..")
sys.path.insert(0, os.path.normpath(_src_dir))

from pyspark.sql import SparkSession
from common.config import (
    TABLE_BRONZE_FUNDAMENTALS,
    TABLE_SILVER_FUNDAMENTALS,
    FUNDAMENTALS_NUMERIC_FIELDS,
    FUNDAMENTALS_STRING_FIELDS,
)


def main():
    spark = SparkSession.builder.getOrCreate()

    print(f"[build_silver_fundamentals] Reading from {TABLE_BRONZE_FUNDAMENTALS}...")

    all_columns = ["symbol"] + FUNDAMENTALS_STRING_FIELDS + FUNDAMENTALS_NUMERIC_FIELDS
    col_list = ", ".join(all_columns)

    # Create table if not exists with Liquid Clustering
    table_exists = spark.catalog.tableExists(TABLE_SILVER_FUNDAMENTALS)

    if not table_exists:
        # Build column definitions
        col_defs = ["symbol STRING NOT NULL"]
        for f in FUNDAMENTALS_STRING_FIELDS:
            col_defs.append(f"{f} STRING")
        for f in FUNDAMENTALS_NUMERIC_FIELDS:
            col_defs.append(f"{f} DOUBLE")

        spark.sql(f"""
            CREATE TABLE {TABLE_SILVER_FUNDAMENTALS}
            ({', '.join(col_defs)})
            USING DELTA
            CLUSTER BY (symbol)
        """)
        print(f"[build_silver_fundamentals] Created {TABLE_SILVER_FUNDAMENTALS} with Liquid Clustering")

    # Filter Bronze to current SCD2 version and MERGE into Silver
    # Deduplicate: if multiple is_current rows exist for same symbol,
    # pick the one with latest effective_from (prevents MERGE ambiguity)
    spark.sql(f"""
        MERGE INTO {TABLE_SILVER_FUNDAMENTALS} t
        USING (
            SELECT {col_list}
            FROM (
                SELECT {col_list},
                       ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY effective_from DESC) AS rn
                FROM {TABLE_BRONZE_FUNDAMENTALS}
                WHERE is_current = true
            )
            WHERE rn = 1
        ) s
        ON t.symbol = s.symbol
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

    count = spark.table(TABLE_SILVER_FUNDAMENTALS).count()
    print(f"[build_silver_fundamentals] {TABLE_SILVER_FUNDAMENTALS}: {count} rows (MERGE complete)")


if __name__ == "__main__":
    main()