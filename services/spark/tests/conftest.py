"""Lokale SparkSession und Bronze-Zeilen im TLC-Format für die Tests der Jobs."""

from collections.abc import Callable, Iterator
from datetime import datetime
from typing import Any

import pytest
from pyspark.sql import DataFrame, SparkSession

# Bronze-Schema wie in den TLC-Dateien 2024 (Auszug plus zwei Spalten, die Silver verwirft).
BRONZE_SCHEMA = (
    "VendorID int, tpep_pickup_datetime timestamp_ntz, tpep_dropoff_datetime timestamp_ntz, "
    "passenger_count bigint, trip_distance double, PULocationID int, DOLocationID int, "
    "payment_type bigint, fare_amount double, total_amount double, store_and_fwd_flag string, "
    "_source_file string"
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
    "_source_file": "s3a://bronze/yellow_tripdata/year=2024/month=01/yellow_tripdata_2024-01.parquet",
}


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    session = (
        SparkSession.builder.master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        # Wie spark-defaults.conf: Partitionen year/month bleiben Strings.
        .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def bronze(spark: SparkSession) -> Callable[..., DataFrame]:
    """Baut Bronze-Zeilen; jedes Argument überschreibt Felder einer gültigen Fahrt."""

    def build(*overrides: dict[str, Any]) -> DataFrame:
        return spark.createDataFrame([GOOD_TRIP | o for o in overrides], schema=BRONZE_SCHEMA)

    return build
