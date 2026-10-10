"""Write the table of climatology z-scores that audits 09 and 10 read.

    python -m ghcnd_quality.derive --parquet data/parquet --out data/derived
"""
import argparse
import pathlib

from .audit import register_views, run_sql_df
from .spark import get_spark


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--parquet", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--min-obs", type=int, default=30)
    ap.add_argument("--min-scale", type=float, default=2.0)
    a = ap.parse_args()
    spark = get_spark("derive")
    register_views(spark, a.parquet)
    df = run_sql_df(spark, "08_climatology_zscores", min_obs=a.min_obs, min_scale=a.min_scale)
    out = pathlib.Path(a.out) / "zscores"
    df.write.mode("overwrite").parquet(str(out))
    print("rows written:", spark.read.parquet(str(out)).count())


if __name__ == "__main__":
    main()
