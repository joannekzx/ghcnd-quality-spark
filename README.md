# ghcnd-quality-spark

A Spark pipeline and data-quality audit over **NOAA GHCN-Daily**, the global archive of daily station weather observations. Real data throughout: 474 stations, 39.4 million daily observations (TMAX, TMIN, TAVG, PRCP, SNOW, SNWD), 1838 to October 2026.

The question the project answers: **how far can you trust station data, and where does it break?** NOAA records its own quality-control result next to every value. This repo parses the raw fixed-width files with Spark, stores them as partitioned Parquet, and audits both NOAA's flags and the data underneath them with Spark SQL.

Stack: PySpark / Spark SQL, pandas, numpy, Parquet, pytest, Jupyter.

## Read in this order

| Notebook | What it shows |
|---|---|
| [01 · The `.dly` format](notebooks/01_the_dly_format.ipynb) | Hand-parse one station in pandas, then require the Spark parser to produce identical rows (202,097 of 202,097 match on value and all three flags). |
| [02 · The Spark pipeline](notebooks/02_spark_pipeline.ipynb) | Raw text to partitioned Parquet: row counts match per element, no duplicate keys, partition pruning visible in the plan, raw-vs-Parquet timing. |
| [03 · Quality audit](notebooks/03_quality_audit.ipynb) | Coverage, NOAA flag rates and reasons, a fixed-limits bounds check, stuck-sensor runs, completeness, and year-to-year jumps. |
| [04 · Independent detector](notebooks/04_independent_detector.ipynb) | A per-station climatology z-score detector that uses none of NOAA's rules, compared with NOAA's flags; TAVG outside TMIN/TMAX; stuck runs against a within-month shuffle baseline; the batched build checked against the first build; what scales, with timings. |

The audits themselves are plain SQL files in [`sql/`](sql/), each parameterised with named parameters and run by `ghcnd_quality.audit.run_sql`. The notebooks only call them and plot the small results.

## Findings (all from the executed notebooks)

- **NOAA flags 0.14% of observations** (56,962 of 39,358,220). The most common reasons are the internal-consistency check (`I`), the duplicate check (`D`) and the climatological-outlier check (`O`).
- **A fixed-limits check found nothing NOAA missed, but that proves little.** Rules: TMAX above 57 C, TMIN below -90 C, daily rainfall above 1,900 mm, negative amounts, TMAX below TMIN on the same day. 9,396 violations, 0 unflagged. These limits overlap NOAA's own bounds check, so this is a sanity check, not an independent one. The raw extremes in the data (TMAX 999, TMIN -999) are all flagged values, which is why the pipeline keeps flagged rows rather than dropping them.
- **An independent detector agrees with NOAA where it matters.** A robust z-score from each station's own weekly climatology (median and MAD, no NOAA flag or bound used) is, at |z| of 8 or more, flagged by NOAA for 99.8% of TMAX (1,522 of 1,525) and 100% of TMIN (1,131 of 1,131) observations; only 3 observations in the sample reach that level without a flag. In the middle, 23,572 observations with 4 <= |z| < 8 are unflagged, and NOAA flags 35,406 observations with |z| under 3. The unflagged extremes are probably mostly real weather; notebook 04 lists the top ones with neighbouring days.
- **TAVG sometimes falls outside the day's TMIN to TMAX range:** 18,667 of 5,065,662 days with all three reported (0.37%, tolerance 0.5 C), 16,612 with no flag. Likely definition differences, not necessarily faults.
- **Long stuck-sensor runs are caught; short ones mostly are not.** Runs of 30 or more identical days (TMAX and TMIN): 21 runs, all with at least one flagged day. 15 to 29 days: 67 runs, 31 with a flagged day. 10 to 14 days: 238 runs, only 11. Against the same series shuffled within each calendar month (five shuffles), which keeps each station's rounding, there are about 0.2 runs of 10 to 14 days and none longer, so rounding and chance do not explain them. Whether each unflagged one is a fault is not decided. The clearest faults are repeats of a value that is not a whole F, such as -2.0 C held for months at several Philippine stations.
- **Records are patchy inside a station's life.** Mean TMAX completeness (observed days over days in the year, full years only) is about 0.70 in the 2010s, and more than half of station-years in the 2000s and 2010s are under 90% complete.
- **Annual-mean jumps:** 72 year-to-year steps of 3 C or more across 43 stations. Only 3 are reversed the following year, so these are mostly persistent level shifts or unusual years; the heuristic cannot say which.
- **Format conversion pays, Spark does not at this size.** Mean TMAX per station-year took 35.7 s in Spark from raw text, 1.9 s in Spark from Parquet and 2.1 s in pandas from Parquet (2 cores, single runs, so an order of magnitude, not a benchmark). 474 text files became 102 Parquet files, 145 MB. The 19.7M-row TMAX/TMIN series is 0.47 GB in pandas (24 bytes per row).
- **The batched build is faithful:** 172 batches keyed by station-id prefix, resumable, reproduce the first build exactly (39,358,220 rows, same counts per element, 0 rows in one and not the other, 0 duplicate keys).

## Caveats

- **The full archive was not run.** The environment this was developed in could not reach NOAA, so every number is for the 474-station sample. The build is batched and resumable for the full archive (about 130,000 stations), but I have not run it; a full run is pending.
- **The sample is not random.** It is up to six stations per country, in station-id order, among stations that have TMAX, TMIN and PRCP records starting by 1960 and still reporting in 2025. Early decades are survivors by construction, so coverage and completeness numbers are not global statistics.
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

python -m ghcnd_quality.build --raw data/raw --out data/parquet   # add --resume to continue a stopped run
python -m ghcnd_quality.derive --parquet data/parquet --out data/derived   # z-scores for notebook 04
jupyter lab notebooks/
pytest                                       # parser tests on real .dly lines
```

## Layout

```
src/ghcnd_quality/   dly.py (fixed-width parser in Spark SQL), stations.py, build.py, derive.py, runs.py, audit.py, spark.py
sql/                 01_coverage ... 07_annual_mean_jumps, 08-11 detector and consistency audits
notebooks/           01-04, committed with their outputs
tests/               10 tests; fixtures are real lines copied from the archive
```

## Design notes

- Each `.dly` line holds one station-month-element with 31 day slots. The parser unpivots them with `stack(31, ...)` in Spark SQL, with no Python UDFs, and uses `try_cast`/`try_to_date` so Spark's ANSI mode does not fail on padding days such as 31 February.
- Missing (`-9999`) and padding slots are dropped at parse time; every flag column is kept, and flagged values are never filtered out.
- Parquet is partitioned by element and decade, with a repartition before the write so each partition is written by one task.
