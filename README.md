# ghcnd-quality-spark

A Spark pipeline and data-quality audit over **NOAA GHCN-Daily**, the global archive of daily station weather observations. Real data throughout: 474 stations, 39.4 million daily observations (TMAX, TMIN, TAVG, PRCP, SNOW, SNWD), 1838 to October 2026.

The question the project answers: **how far can you trust station data, and where does it break?** NOAA records its own quality-control result next to every value. This repo parses the raw fixed-width files with Spark, stores them as partitioned Parquet, and audits both NOAA's flags and the data underneath them with Spark SQL.

Stack: PySpark / Spark SQL, pandas, Parquet, pytest, Jupyter.

## Read in this order

| Notebook | What it shows |
|---|---|
| [01 · The `.dly` format](notebooks/01_the_dly_format.ipynb) | Hand-parse one station in pandas, then require the Spark parser to produce identical rows (202,097 of 202,097 match on value and all three flags). |
| [02 · The Spark pipeline](notebooks/02_spark_pipeline.ipynb) | Raw text to partitioned Parquet: row counts match per element, no duplicate keys, partition pruning visible in the plan, raw-vs-Parquet timing. |
| [03 · Quality audit](notebooks/03_quality_audit.ipynb) | Coverage, NOAA flag rates and reasons, an independent bounds check, stuck-sensor runs, completeness, and year-to-year jumps. |

The audits themselves are plain SQL files in [`sql/`](sql/), each parameterised with named parameters and run by `ghcnd_quality.audit.run_sql`. The notebooks only call them and plot the small results.

## Findings (all from the executed notebooks)

- **NOAA flags 0.14% of observations** (56,962 of 39,358,220). The most common reasons are the internal-consistency check (`I`), the duplicate check (`D`) and the climatological-outlier check (`O`).
- **An independent bounds check found nothing NOAA missed.** Rules: TMAX above 57 C, TMIN below -90 C, daily rainfall above 1,900 mm, negative amounts, TMAX below TMIN on the same day. 9,396 violations, 0 unflagged. The raw extremes in the data (TMAX 999, TMIN -999) are all flagged values, which is why the pipeline keeps flagged rows rather than dropping them.
- **Long stuck-sensor runs are caught; short ones mostly are not.** Runs of 30 or more identical TMAX days: 10 runs, 99.9% of their days flagged. Runs of 10 to 14 days: 165 runs, 2.1% flagged. The short runs are mostly stations reporting to the whole degree Fahrenheit (58% of them convert to a whole F value), so repeats there are weak evidence of a fault. The clearest faults are repeats of a value that is *not* a whole F, such as -2.0 C held for months at several Philippine stations.
- **Records are patchy inside a station's life.** Mean TMAX completeness (observed days over days in the year, full years only) is about 0.70 in the 2010s, and more than half of station-years in the 2000s and 2010s are under 90% complete.
- **Annual-mean jumps:** 72 year-to-year steps of 3 C or more across 43 stations. Only 3 are reversed the following year, so these are mostly persistent level shifts or unusual years; the heuristic cannot say which.
- **Format conversion pays:** the same aggregate took 97.6 s from raw text and 1.6 s from Parquet on this machine (2 cores, single run each, so an order of magnitude, not a benchmark). 474 text files became 102 Parquet files, 145 MB.

## Caveats

- **The sample is not random.** It is up to six stations per country, in station-id order, among stations that have TMAX, TMIN and PRCP records starting by 1960 and still reporting in 2025. Early decades are survivors by construction, so coverage and completeness numbers are not global statistics. The full archive is about 130,000 stations; the pipeline is written for it but was only run on this sample.
- The stuck-value audit and the annual-mean-jump audit are heuristics, not proof of faults.
- The bounds thresholds are deliberately loose, near the all-time physical records.

## Data

Downloaded from NOAA NCEI (not included):

- `ghcnd_all.tar.gz`, `ghcnd-stations.txt`, `ghcnd-inventory.txt` from <https://www.ncei.noaa.gov/pub/data/ghcn/daily/>
- Format documentation: `readme.txt` in the same directory.

Put the station files in `data/raw/ghcnd_all/` and the two text files in `data/raw/`.

## Run it

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[notebooks,test]"          # needs Java for Spark

python -m ghcnd_quality.build --raw data/raw --out data/parquet
jupyter lab notebooks/
pytest                                       # parser tests on real .dly lines
```

## Layout

```
src/ghcnd_quality/   dly.py (fixed-width parser in Spark SQL), stations.py, build.py, audit.py, spark.py
sql/                 01_coverage ... 07_annual_mean_jumps
notebooks/           01-03, committed with their outputs
tests/               8 tests; fixtures are real lines copied from the archive
```

## Design notes

- Each `.dly` line holds one station-month-element with 31 day slots. The parser unpivots them with `stack(31, ...)` in Spark SQL, with no Python UDFs, and uses `try_cast`/`try_to_date` so Spark's ANSI mode does not fail on padding days such as 31 February.
- Missing (`-9999`) and padding slots are dropped at parse time; every flag column is kept, and flagged values are never filtered out.
- Parquet is partitioned by element and decade, with a repartition before the write so each partition is written by one task.
