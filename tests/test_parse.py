"""Parser tests on five real .dly lines (tests/fixtures/real_lines.dly), copied verbatim from
the GHCN-Daily archive: a line that is mostly missing slots, one with an X flag, a leap-year
February, a non-core element, and a precipitation month."""
import datetime as dt

import pytest

from ghcnd_quality.dly import CORE_ELEMENTS, read_dly
from ghcnd_quality.stations import read_inventory, read_stations


def reference_parse(path):
    """Straight-line Python parse of the same format, independent of the Spark code."""
    out = set()
    for line in open(path):
        line = line.rstrip("\n")
        element = line[17:21]
        if element not in CORE_ELEMENTS:
            continue
        year, month = int(line[11:15]), int(line[15:17])
        for day in range(1, 32):
            off = 21 + (day - 1) * 8
            value = int(line[off:off + 5])
            if value == -9999:
                continue
            try:
                date = dt.date(year, month, day)
            except ValueError:
                continue
            qflag = line[off + 6].strip() or None
            out.add((line[:11], date, element, value, qflag))
    return out


@pytest.fixture(scope="module")
def parsed(spark, fixtures_dir):
    return read_dly(spark, str(fixtures_dir / "real_lines.dly")).cache()


def test_matches_independent_python_parse(parsed, fixtures_dir):
    got = {(r.station_id, r.date, r.element, r.value, r.qflag) for r in parsed.collect()}
    assert got == reference_parse(fixtures_dir / "real_lines.dly")
    assert len(got) > 50           # not vacuous


def test_known_observation_and_its_flag_columns(parsed):
    """The three flag columns sit side by side; this row has only a source flag set."""
    row = parsed.where("station_id = 'AE000041196' and date = '1944-03-20' and element = 'TMAX'").collect()[0]
    assert row.value == 380 and row.value_native == 38.0      # tenths of a degree -> deg C
    assert (row.mflag, row.qflag, row.sflag) == (None, None, "I")


def test_bounds_flag_on_impossible_temperature(parsed):
    row = parsed.where("station_id = 'FMW00040308' and date = '1953-01-22' and element = 'TMIN'").collect()[0]
    assert row.value_native == 358.9                          # 358.9 deg C is not a temperature
    assert row.qflag == "X"                                   # NOAA's bounds check caught it


def test_missing_sentinel_never_survives(parsed):
    assert parsed.where("value = -9999 or value is null").count() == 0


def test_padding_days_are_dropped(parsed):
    feb = parsed.where("station_id = 'AG000060590' and year = 1952 and month = 2")
    assert feb.count() > 0
    assert feb.agg({"day": "max"}).collect()[0][0] <= 29      # 1952 is a leap year; no Feb 30/31


def test_non_core_elements_are_dropped(parsed):
    assert {r.element for r in parsed.select("element").distinct().collect()} <= set(CORE_ELEMENTS)


def test_blank_flags_become_null(parsed):
    assert parsed.where("qflag is null").count() > 0
    assert parsed.where("qflag is not null").count() > 0
    assert parsed.where("trim(qflag) = ''").count() == 0


def test_stations_and_inventory(spark, fixtures_dir):
    st = {r.station_id: r for r in read_stations(spark, str(fixtures_dir / "real_stations.txt")).collect()}
    first = st["ACW00011604"]
    assert (first.latitude, first.longitude, first.elevation_m) == (17.1167, -61.7833, 10.1)
    assert first.name == "ST JOHNS COOLIDGE FLD" and first.country_code == "AC"
    assert any(r.elevation_m is None for r in st.values())     # the -999.9 sentinel became null

    inv = read_inventory(spark, str(fixtures_dir / "real_inventory.txt")).collect()
    assert (inv[0].station_id, inv[0].element, inv[0].first_year, inv[0].last_year) == ("ACW00011604", "TMAX", 1949, 1949)
