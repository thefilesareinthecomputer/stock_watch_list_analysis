"""
One-time (idempotent) setup of Unity Catalog namespace.
Creates catalog and schemas if they don't exist.
Run as the first task in the pipeline DAG.
"""
import sys
import os
try:
    _src_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
except NameError:
    _src_dir = os.path.join(os.getcwd(), "..")
sys.path.insert(0, os.path.normpath(_src_dir))

from pyspark.sql import SparkSession
from common.config import CATALOG, SCHEMA_BRONZE, SCHEMA_SILVER, SCHEMA_GOLD


def main():
    spark = SparkSession.builder.getOrCreate()

    try:
        spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
        print(f"[setup] Catalog '{CATALOG}' created/verified.")
    except Exception as e:
        print(f"[setup] WARNING: Could not create catalog '{CATALOG}': {e}")
        print(f"[setup] If on Free Edition, set CATALOG='main' in config.py and re-deploy.")

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA_BRONZE}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA_SILVER}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA_GOLD}")
    print(f"[setup] Schemas {SCHEMA_BRONZE}, {SCHEMA_SILVER}, {SCHEMA_GOLD} created/verified.")


if __name__ == "__main__":
    main()
