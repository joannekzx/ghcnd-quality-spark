"""Parse GHCN-Daily `.dly` files (one fixed-width line per station / year / month / element).

Line layout (1-based columns, from the GHCN-Daily readme):
    1-11   station id        12-15  year        16-17  month        18-21  element
    22-269 31 daily slots of 8 characters each:
               value (5, -9999 = missing)  measurement flag (1)  quality flag (1)  source flag (1)

So one text line holds up to 31 observations. Parsing means turning each line into 31 rows;
`stack(31, ...)` does that in Spark SQL without a Python UDF.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# The six elements that carry the core climate record. Everything else (weather-type
# flags, soil temperatures, ...) is dropped at parse time.
CORE_ELEMENTS = ("TMAX", "TMIN", "TAVG", "PRCP", "SNOW", "SNWD")

# Elements stored in tenths of a unit: TMAX/TMIN/TAVG in tenths of deg C, PRCP in tenths of mm.
# SNOW and SNWD are whole millimetres.
TENTHS_ELEMENTS = ("TMAX", "TMIN", "TAVG", "PRCP")

MISSING = -9999

# Quality flags (QFLAG), from the GHCN-Daily readme. A non-blank flag means NOAA's own
# checks marked the observation as suspect; it is kept in the file, not removed.
QFLAG_MEANING = {
    "D": "failed duplicate check",
    "G": "failed gap check",
    "I": "failed internal consistency check",
    "K": "failed streak / frequent-value check",
    "L": "failed check on length of multiday period",
    "M": "failed mega-consistency check",
    "N": "failed naught check",
    "O": "failed climatological outlier check",
    "R": "failed lagged range check",
    "S": "failed spatial consistency check",
    "T": "failed temporal consistency check",
    "W": "temperature too warm for snow",
    "X": "failed bounds check",
    "Z": "flagged as a result of an official Datzilla investigation",
}


def _stack_expr() -> str:
    """`stack(31, day, value, mflag, qflag, sflag, ...)` over the 31 8-character slots."""
    slots = []
    for day in range(1, 32):
        start = 22 + (day - 1) * 8
        slots.append(
            f"{day}, substring(line, {start}, 5), substring(line, {start + 5}, 1), "
            f"substring(line, {start + 6}, 1), substring(line, {start + 7}, 1)"
        )
    return "stack(31, " + ", ".join(slots) + ") as (day, value_raw, mflag, qflag, sflag)"


def read_dly(spark: SparkSession, path: str, elements=CORE_ELEMENTS) -> DataFrame:
    """Read `.dly` text files and return one row per OBSERVED daily value.

    Columns: station_id, date, year, month, day, element, value (raw integer),
             value_native (deg C or mm), mflag, qflag, sflag (null when blank)
    Missing slots (-9999) and padding for non-existent days (e.g. Feb 30) are dropped here;
    notebook 03 measures completeness from what remains.
    """
    lines = spark.read.text(path).withColumnRenamed("value", "line")
    wide = (
        lines.where(F.substring("line", 18, 4).isin(*elements))
        .selectExpr(
            "substring(line, 1, 11) as station_id",
            "try_cast(substring(line, 12, 4) as int) as year",
            "try_cast(substring(line, 16, 2) as int) as month",
            "substring(line, 18, 4) as element",
            _stack_expr(),
        )
    )
    return (
        wide.withColumn("value", F.expr("try_cast(trim(value_raw) as int)"))
        .where(F.col("value").isNotNull() & (F.col("value") != MISSING))
        .withColumn("date", F.expr("try_to_date(concat_ws('-', year, month, day), 'y-M-d')"))
        .where(F.col("date").isNotNull())
        .withColumn(
            "value_native",
            F.when(F.col("element").isin(*TENTHS_ELEMENTS), F.col("value") / 10.0).otherwise(
                F.col("value").cast("double")
            ),
        )
        .select(
            "station_id", "date", "year", "month", "day", "element", "value", "value_native",
            *[F.expr(f"nullif(trim({c}), '')").alias(c) for c in ("mflag", "qflag", "sflag")],
        )
    )
