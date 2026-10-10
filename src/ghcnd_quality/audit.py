"""Run the audit queries in sql/ against the Parquet tables.

Every audit is a plain Spark SQL file; this module only registers the tables as views and
runs a file with named parameters (`:name` markers).
"""
import pathlib

import pandas as pd
from pyspark.sql import SparkSession

SQL_DIR = pathlib.Path(__file__).resolve().parents[2] / "sql"


def register_views(spark: SparkSession, parquet_dir: str | pathlib.Path) -> None:
    base = pathlib.Path(parquet_dir)
    for name in ("daily", "stations", "inventory"):
        spark.read.parquet(str(base / name)).createOrReplaceTempView(name)


def run_sql(spark: SparkSession, name: str, **params) -> pd.DataFrame:
    """Run sql/<name>.sql and return the (small) result as a pandas DataFrame."""
    text = (SQL_DIR / f"{name}.sql").read_text()
    return spark.sql(text, args=params or None).toPandas()


def run_sql_df(spark: SparkSession, name: str, **params):
    """Like run_sql, but returns the Spark DataFrame (for results too big for pandas)."""
    text = (SQL_DIR / f"{name}.sql").read_text()
    return spark.sql(text, args=params or None)


def register_zscores(spark: SparkSession, derived_dir: str | pathlib.Path) -> None:
    """The z-score table written by `python -m ghcnd_quality.derive`."""
    spark.read.parquet(str(pathlib.Path(derived_dir) / "zscores")).createOrReplaceTempView("zscores")
