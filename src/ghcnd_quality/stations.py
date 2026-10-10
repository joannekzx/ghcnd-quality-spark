"""Fixed-width readers for ghcnd-stations.txt and ghcnd-inventory.txt (1-based columns).

stations:   id 1-11 | latitude 13-20 | longitude 22-30 | elevation 32-37 | state 39-40 | name 42-71
inventory:  id 1-11 | latitude 13-20 | longitude 22-30 | element 32-35 | first year 37-40 | last year 42-45
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def read_stations(spark: SparkSession, path: str) -> DataFrame:
    lines = spark.read.text(path).withColumnRenamed("value", "line")
    return lines.selectExpr(
        "substring(line, 1, 11) as station_id",
        "substring(line, 1, 2) as country_code",
        "try_cast(trim(substring(line, 13, 8)) as double) as latitude",
        "try_cast(trim(substring(line, 22, 9)) as double) as longitude",
        "try_cast(trim(substring(line, 32, 6)) as double) as elevation_m",
        "nullif(trim(substring(line, 39, 2)), '') as state",
        "trim(substring(line, 42, 30)) as name",
    ).withColumn(  # -999.9 is the file's "no elevation" sentinel
        "elevation_m", F.when(F.col("elevation_m") == -999.9, None).otherwise(F.col("elevation_m"))
    )


def read_inventory(spark: SparkSession, path: str) -> DataFrame:
    lines = spark.read.text(path).withColumnRenamed("value", "line")
    return lines.selectExpr(
        "substring(line, 1, 11) as station_id",
        "substring(line, 32, 4) as element",
        "try_cast(substring(line, 37, 4) as int) as first_year",
        "try_cast(substring(line, 42, 4) as int) as last_year",
    )
