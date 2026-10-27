from collections.abc import Callable
from datetime import datetime

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from spark_jobs.schema import BUSINESS_COLUMNS, SchemaError, harmonize


def test_harmonize_keeps_only_needed_columns(bronze: Callable[..., DataFrame]) -> None:
    df = harmonize(bronze({}))
    assert df.columns == [*BUSINESS_COLUMNS, "_source_file"]
    # Datenminimierung: Hersteller-ID und Store-and-Forward-Flag gelangen nicht nach Silver.
    assert "VendorID" not in df.columns
    assert "store_and_fwd_flag" not in df.columns


def test_harmonize_casts_type_drift(spark: SparkSession) -> None:
    # Ältere Monate speichern passenger_count als double und Spalten in anderer Schreibweise.
    raw = spark.createDataFrame(
        [(datetime(2024, 1, 1, 1), datetime(2024, 1, 1, 2), 2.0, 1.5, 1, 2, 1.0, 9.0, 12.0)],
        "tpep_pickup_datetime timestamp_ntz, tpep_dropoff_datetime timestamp_ntz, "
        "passenger_count double, trip_distance double, pulocationid int, dolocationid int, "
        "payment_type double, fare_amount double, total_amount double",
    )
    row = harmonize(raw).first()
    assert row is not None
    assert row["passenger_count"] == 2
    assert row["pu_location_id"] == 1
    assert row["payment_type"] == 1
    assert dict(harmonize(raw).dtypes)["passenger_count"] == "int"


def test_harmonize_turns_unconvertible_values_into_null(bronze: Callable[..., DataFrame]) -> None:
    raw = (
        bronze({})
        .withColumn("PULocationID", F.lit(1e12))
        .withColumn("passenger_count", F.lit(float("nan")))
    )
    row = harmonize(raw).first()
    assert row is not None
    assert row["pu_location_id"] is None
    assert row["passenger_count"] is None


def test_harmonize_reports_missing_columns(bronze: Callable[..., DataFrame]) -> None:
    with pytest.raises(SchemaError, match="PULocationID"):
        harmonize(bronze({}).drop("PULocationID"))
