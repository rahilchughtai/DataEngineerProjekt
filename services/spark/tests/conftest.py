"""Gemeinsame Fixtures der Spark-Tests: lokale Session und Bronze-Zeilen im TLC-Format."""

from collections.abc import Callable, Iterator
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest
from pyspark.sql import DataFrame, SparkSession

from spark_jobs.lake import LakePaths

# Bronze-Schema wie in den TLC-Dateien 2024 (Auszug plus zwei Spalten, die Silver verwirft).
BRONZE_SCHEMA = (
    "VendorID int, tpep_pickup_datetime timestamp_ntz, tpep_dropoff_datetime timestamp_ntz, "
    "passenger_count bigint, trip_distance double, PULocationID int, DOLocationID int, "
    "payment_type bigint, fare_amount double, total_amount double, store_and_fwd_flag string"
)

GOOD_TRIP: dict[str, Any] = {
    "VendorID": 2,
    "tpep_pickup_datetime": datetime(2024, 1, 15, 8, 30),
    "tpep_dropoff_datetime": datetime(2024, 1, 15, 8, 50),
    "passenger_count": 1,
    "trip_distance": 3.2,
    "PULocationID": 161,
    "DOLocationID": 237,
    "payment_type": 1,
    "fare_amount": 17.0,
    "total_amount": 24.5,
    "store_and_fwd_flag": "N",
}


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    session = (
        SparkSession.builder.master("local[2]")
        .appName("spark-jobs-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def paths(tmp_path: Path) -> LakePaths:
    """Lake-Pfade unter tmp_path; die Lake-Funktionen arbeiten dort über file://."""
    root = tmp_path.as_uri()
    return LakePaths(
        bronze=f"{root}/bronze",
        silver=f"{root}/silver",
        gold=f"{root}/gold",
        dataset="yellow_tripdata",
    )


@pytest.fixture
def bronze(spark: SparkSession) -> Callable[..., DataFrame]:
    """Baut einen Bronze-DataFrame; jede Zeile überschreibt Felder einer gültigen Fahrt."""

    def build(*overrides: dict[str, Any]) -> DataFrame:
        rows = [GOOD_TRIP | o for o in overrides]
        return spark.createDataFrame(rows, schema=BRONZE_SCHEMA)

    return build
