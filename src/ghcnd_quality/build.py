"""Parse raw GHCN-Daily into partitioned Parquet.

    python -m ghcnd_quality.build --raw data/raw --out data/parquet

Layout: data/raw/ghcnd_all/*.dly, data/raw/ghcnd-stations.txt, data/raw/ghcnd-inventory.txt
The same code handles the full ~130k-station archive; only SPARK_DRIVER_MEMORY needs raising.
"""
import argparse
import pathlib
import shutil
import time

from pyspark.sql import functions as F

from .dly import read_dly
from .spark import get_spark
from .stations import read_inventory, read_stations


def station_prefixes(raw: pathlib.Path) -> list[str]:
    """First five characters of every station id (country + network code + first digit), the batch key."""
    return sorted({f.name[:5] for f in (raw / "ghcnd_all").glob("*.dly")})


def build(raw: pathlib.Path, out: pathlib.Path, resume: bool = False) -> None:
    """Parse in batches keyed by station-id prefix, appending to the partitioned Parquet.

    Listing and reading ~100k small files in one Spark job is slow and loses everything on failure, so each
    batch is one job and finished batches are recorded in out/_done.txt; --resume skips them.
    """
    spark = get_spark("ghcnd-build")
    t0 = time.time()
    out.mkdir(parents=True, exist_ok=True)
    done_file = out / "_done.txt"
    if not resume:
        for d in ("daily",):
            shutil.rmtree(out / d, ignore_errors=True)
        done_file.unlink(missing_ok=True)
    done = set(done_file.read_text().split()) if done_file.exists() else set()

    batches = [b for b in station_prefixes(raw) if b not in done]
    print(f"{len(batches)} batches to run, {len(done)} already done")
    for i, prefix in enumerate(batches, 1):
        t1 = time.time()
        daily = (
            read_dly(spark, str(raw / "ghcnd_all" / f"{prefix}*.dly"))
            .withColumn("decade", (F.floor(F.col("year") / 10) * 10).cast("int"))
            .repartition("element", "decade")
        )
        daily.write.mode("append").partitionBy("element", "decade").parquet(str(out / "daily"))
        with done_file.open("a") as f:
            f.write(prefix + "\n")
        print(f"[{i}/{len(batches)}] {prefix} {time.time() - t1:.0f}s", flush=True)

    read_stations(spark, str(raw / "ghcnd-stations.txt")).write.mode("overwrite").parquet(str(out / "stations"))
    read_inventory(spark, str(raw / "ghcnd-inventory.txt")).write.mode("overwrite").parquet(str(out / "inventory"))

    n = spark.read.parquet(str(out / "daily")).count()
    print(f"daily rows written: {n:,}  ({time.time() - t0:.0f}s this run)")
    spark.stop()


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--raw", type=pathlib.Path, default=pathlib.Path("data/raw"))
    ap.add_argument("--out", type=pathlib.Path, default=pathlib.Path("data/parquet"))
    ap.add_argument("--resume", action="store_true", help="skip batches already listed in OUT/_done.txt")
    args = ap.parse_args()
    build(args.raw, args.out, args.resume)
