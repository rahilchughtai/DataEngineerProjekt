from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest
from pyspark.sql import DataFrame, SparkSession

from bronze_to_silver import SILVER_COLUMNS, to_silver
from common import write_month


def reason(bronze: Callable[..., DataFrame], override: dict[str, Any], month: int = 1) -> Any:
    return to_silver(bronze(override), 2024, month).first()["reject_reason"]


def test_silver_keeps_only_needed_columns(bronze: Callable[..., DataFrame]) -> None:
    columns = to_silver(bronze({}), 2024, 1).columns
    assert columns == [*SILVER_COLUMNS, "_source_file", "reject_reason", "_processed_at"]


def test_type_drift_is_cast_and_unconvertible_values_become_null(spark: SparkSession) -> None:
    # Ältere Monate speichern passenger_count als double und Spalten in anderer Schreibweise.
    raw = spark.createDataFrame(
        [
            (
                datetime(2024, 1, 1, 1),
                datetime(2024, 1, 1, 2),
                2.0,
                1.5,
                1e12,
                2,
                1.0,
                9.0,
                12.0,
                "f",
            )
        ],
        "tpep_pickup_datetime timestamp_ntz, tpep_dropoff_datetime timestamp_ntz, "
        "passenger_count double, trip_distance double, pulocationid double, dolocationid int, "
        "payment_type double, fare_amount double, total_amount double, _source_file string",
    )
    row = to_silver(raw, 2024, 1).first()
    assert row["passenger_count"] == 2
    assert row["pu_location_id"] is None
    assert row["reject_reason"] == "location_invalid"


def test_exact_duplicates_are_removed(bronze: Callable[..., DataFrame]) -> None:
    assert to_silver(bronze({}, {}, {"fare_amount": 18.0}), 2024, 1).count() == 2


@pytest.mark.parametrize(
    "override",
    [{}, {"passenger_count": None}, {"fare_amount": 1e9, "total_amount": 1e9}],
)
def test_valid_trips_pass(bronze: Callable[..., DataFrame], override: dict[str, Any]) -> None:
    assert reason(bronze, override) is None


@pytest.mark.parametrize(
    ("override", "expected"),
    [
        ({"tpep_pickup_datetime": None}, "pickup_null"),
        ({"tpep_dropoff_datetime": None}, "dropoff_null"),
        ({"tpep_pickup_datetime": datetime(2023, 12, 31, 23, 59)}, "pickup_outside_month"),
        ({"tpep_pickup_datetime": datetime(2024, 2, 1, 0, 0)}, "pickup_outside_month"),
        ({"tpep_dropoff_datetime": datetime(2024, 1, 15, 8, 0)}, "negative_duration"),
        ({"tpep_dropoff_datetime": datetime(2024, 1, 16, 9, 0)}, "duration_over_24h"),
        ({"trip_distance": -1.0}, "distance_invalid"),
        ({"trip_distance": 250.0}, "distance_invalid"),
        ({"trip_distance": float("nan")}, "distance_invalid"),
        ({"fare_amount": -5.0}, "amount_invalid"),
        ({"total_amount": None}, "amount_invalid"),
        ({"total_amount": float("inf")}, "amount_invalid"),
        ({"PULocationID": 0}, "location_invalid"),
        ({"DOLocationID": 266}, "location_invalid"),
        ({"passenger_count": 12}, "passenger_count_invalid"),
        # Die erste verletzte Regel gewinnt.
        ({"trip_distance": -1.0, "fare_amount": -5.0}, "distance_invalid"),
    ],
)
def test_rules_reject(
    bronze: Callable[..., DataFrame], override: dict[str, Any], expected: str
) -> None:
    assert reason(bronze, override) == expected


def test_december_bounds_roll_over_into_next_year(bronze: Callable[..., DataFrame]) -> None:
    pickup = datetime(2024, 12, 31, 23, 0)
    override = {"tpep_pickup_datetime": pickup, "tpep_dropoff_datetime": pickup}
    assert reason(bronze, override, month=12) is None


def test_write_month_replaces_only_that_month(
    spark: SparkSession, bronze: Callable[..., DataFrame], tmp_path: Path
) -> None:
    table = str(tmp_path / "silver")
    write_month(bronze({}, {"fare_amount": 18.0}), table, 2024, 1)
    write_month(bronze({}), table, 2024, 2)

    # Wiederholung für Januar: ersetzt den Monat, statt Zeilen zu verdoppeln.
    write_month(bronze({}), table, 2024, 1)

    rows = spark.read.parquet(table).groupBy("year", "month").count().collect()
    assert sorted((r["year"], r["month"], r["count"]) for r in rows) == [
        ("2024", "01", 1),
        ("2024", "02", 1),
    ]
