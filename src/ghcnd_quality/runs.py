"""Runs of identical daily values, and what chance alone would produce.

A sensor stuck on one reading gives many identical days in a row. But some stations report to the whole
degree Fahrenheit and sit in a steady climate, so the same value can repeat without any fault. To tell the
two apart, compare the real series with the same station's values shuffled within each calendar month: that
keeps the station's value distribution (including how coarse its rounding is) and which days exist, but
destroys day-to-day persistence. If the real series has far more long runs than the shuffled ones, chance
and rounding do not explain them.
"""
import numpy as np
import pandas as pd
import pyarrow.dataset as ds

BUCKETS = [(10, 14, "10-14 days"), (15, 29, "15-29 days"), (30, 10**9, "30+ days")]


def load_series(parquet_dir, elements=("TMAX", "TMIN")) -> pd.DataFrame:
    """station, element, day number, month, value (tenths), qflag; sorted by (station, element, date)."""
    cols = ["station_id", "element", "date", "value", "qflag"]
    t = ds.dataset(str(parquet_dir) + "/daily", format="parquet", partitioning="hive") \
          .to_table(columns=cols, filter=ds.field("element").isin(list(elements))).to_pandas()
    t["day"] = (pd.to_datetime(t["date"]) - pd.Timestamp("1800-01-01")).dt.days.astype("int32")
    t["month"] = pd.to_datetime(t["date"]).dt.month.astype("int8")
    t["group"] = pd.factorize(t["station_id"].astype(str) + "|" + t["element"].astype(str))[0].astype("int32")
    return t.sort_values(["group", "day"]).reset_index(drop=True)[["group", "station_id", "element", "day", "month", "value", "qflag"]]


def run_lengths(group: np.ndarray, day: np.ndarray, value: np.ndarray):
    """(start index, length) of every maximal run of consecutive days with the same value in one series."""
    new = np.ones(len(value), dtype=bool)
    new[1:] = (group[1:] != group[:-1]) | (day[1:] != day[:-1] + 1) | (value[1:] != value[:-1])
    start = np.flatnonzero(new)
    length = np.diff(np.append(start, len(value)))
    return start, length


def bucket_counts(length: np.ndarray) -> dict:
    return {label: int(((length >= lo) & (length <= hi)).sum()) for lo, hi, label in BUCKETS}


def shuffled_within_month(df: pd.DataFrame, rng: np.random.Generator) -> np.ndarray:
    """Permute each station-element's values among the days of the same calendar month."""
    key = df["group"].to_numpy().astype(np.int64) * 16 + df["month"].to_numpy()
    by_date = np.lexsort((df["day"].to_numpy(), key))
    by_random = np.lexsort((rng.random(len(df)), key))
    out = np.empty(len(df), dtype=df["value"].dtype)
    out[by_date] = df["value"].to_numpy()[by_random]
    return out


def observed_vs_chance(df: pd.DataFrame, shuffles=5, seed=0) -> pd.DataFrame:
    """Runs of 10+ identical days: observed, and the mean and maximum over shuffled copies."""
    g, d = df["group"].to_numpy(), df["day"].to_numpy()
    _, ln = run_lengths(g, d, df["value"].to_numpy())
    rows = {"observed": bucket_counts(ln)}
    rng = np.random.default_rng(seed)
    sims = [bucket_counts(run_lengths(g, d, shuffled_within_month(df, rng))[1]) for _ in range(shuffles)]
    out = pd.DataFrame(rows["observed"], index=["observed"]).T
    out["shuffled (mean)"] = pd.DataFrame(sims).mean().round(1)
    out["shuffled (max)"] = pd.DataFrame(sims).max()
    return out
