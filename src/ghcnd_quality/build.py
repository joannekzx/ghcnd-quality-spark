"""Parse raw GHCN-Daily into partitioned Parquet.

    python -m ghcnd_quality.build --raw data/raw --out data/parquet

Layout: data/raw/ghcnd_all/*.dly, data/raw/ghcnd-stations.txt, data/raw/ghcnd-inventory.txt
The same code handles the full ~130k-station archive; only SPARK_DRIVER_MEMORY needs raising.
"""
import argparse
import pathlib
import time

from pyspark.sql import functions as F

from .dly import read_dly
from .spark import get_spark
from .stations import read_inventory, read_stations


def build(raw: pathlib.Path, out: pathlib.Path) -> None:
    spark = get_spark("ghcnd-build")
    t0 = time.time()

    daily = (
        read_dly(spark, str(raw / "ghcnd_all" / "*.dly"))
        .withColumn("decade", (F.floor(F.col("year") / 10) * 10).cast("int"))
        # one shuffle so each (element, decade) partition is written by one task,
        # avoiding thousands of tiny files
        .repartition("element", "decade")
    )
    daily.write.mode("overwrite").partitionBy("element", "decade").parquet(str(out / "daily"))
    read_stations(spark, str(raw / "ghcnd-stations.txt")).write.mode("overwrite").parquet(str(out / "stations"))
    read_inventory(spark, str(raw / "ghcnd-inventory.txt")).write.mode("overwrite").parquet(str(out / "inventory"))

    n = spark.read.parquet(str(out / "daily")).count()
    print(f"daily rows written: {n:,}  ({time.time() - t0:.0f}s)")
    spark.stop()


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--raw", type=pathlib.Path, default=pathlib.Path("data/raw"))
    ap.add_argument("--out", type=pathlib.Path, default=pathlib.Path("data/parquet"))
    args = ap.parse_args()
    build(args.raw, args.out)
