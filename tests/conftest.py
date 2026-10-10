import pathlib

import pytest

from ghcnd_quality.spark import get_spark

FIXTURES = pathlib.Path(__file__).parent / "fixtures"


@pytest.fixture(scope="session")
def spark():
    s = get_spark("ghcnd-tests", shuffle_partitions=2)
    yield s
    s.stop()


@pytest.fixture(scope="session")
def fixtures_dir() -> pathlib.Path:
    return FIXTURES
