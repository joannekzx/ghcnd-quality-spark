"""One place to create the Spark session, so notebooks, the CLI and the tests agree."""
import os

from pyspark.sql import SparkSession


def get_spark(app_name: str = "ghcnd-quality", shuffle_partitions: int = 16) -> SparkSession:
    """Local-mode session. Set SPARK_DRIVER_MEMORY (e.g. 8g) for bigger runs."""
    return (
        SparkSession.builder.master("local[*]")
        .appName(app_name)
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "3g"))
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
