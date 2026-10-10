"""Stuck-sensor run logic on a real line: USC00010178 TMAX, October 2004 (30.0 C on 24 days in a row, flagged K)."""
import numpy as np
import pandas as pd

from ghcnd_quality.dly import read_dly
from ghcnd_quality.runs import observed_vs_chance, run_lengths, shuffled_within_month


def _series(spark, fixtures_dir):
    d = read_dly(spark, str(fixtures_dir / "real_stuck_run.dly")).toPandas()
    d = d[d["element"] == "TMAX"].sort_values("date")
    day = (pd.to_datetime(d["date"]) - pd.Timestamp("1800-01-01")).dt.days.to_numpy()
    return pd.DataFrame({"group": 0, "day": day, "month": pd.to_datetime(d["date"]).dt.month.to_numpy(),
                         "value": d["value"].to_numpy()})


def test_longest_real_run_is_found(spark, fixtures_dir):
    df = _series(spark, fixtures_dir)
    _, ln = run_lengths(df["group"].to_numpy(), df["day"].to_numpy(), df["value"].to_numpy())
    assert ln.sum() == len(df)
    assert ln.max() == 24


def test_shuffle_keeps_values_and_breaks_the_run(spark, fixtures_dir):
    df = _series(spark, fixtures_dir)
    shuffled = shuffled_within_month(df, np.random.default_rng(0))
    assert sorted(shuffled) == sorted(df["value"])           # same values, only moved
    out = observed_vs_chance(df, shuffles=3)
    assert out.loc["15-29 days", "observed"] == 1
