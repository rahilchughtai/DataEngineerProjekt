from collections.abc import Callable
from datetime import datetime
from typing import Any

import pytest
from pyspark.sql import DataFrame

from spark_jobs.periods import Month
from spark_jobs.quality import classify
from spark_jobs.schema import harmonize

JANUARY = Month(2024, 1)


def reason(bronze: Callable[..., DataFrame], override: dict[str, Any], month: Month = JANUARY):
    row = classify(harmonize(bronze(override)), month).first()
    assert row is not None
    return row["reject_reason"]


def test_good_trip_passes(bronze: Callable[..., DataFrame]) -> None:
    assert reason(bronze, {}) is None


def test_missing_passenger_count_is_allowed(bronze: Callable[..., DataFrame]) -> None:
    assert reason(bronze, {"passenger_count": None}) is None


@pytest.mark.parametrize(
    ("override", "expected"),
    [
        ({"tpep_pickup_datetime": None}, "pickup_null"),
        ({"tpep_dropoff_datetime": None}, "dropoff_null"),
        (
            {
                "tpep_pickup_datetime": datetime(2009, 1, 1, 0, 5),
                "tpep_dropoff_datetime": datetime(2009, 1, 1, 0, 20),
            },
            "pickup_outside_month",
        ),
        ({"tpep_dropoff_datetime": datetime(2024, 1, 15, 8, 0)}, "negative_duration"),
        ({"tpep_dropoff_datetime": datetime(2024, 1, 16, 9, 0)}, "duration_over_24h"),
        ({"trip_distance": -1.0}, "distance_invalid"),
        ({"trip_distance": 250.0}, "distance_invalid"),
        ({"trip_distance": None}, "distance_invalid"),
        ({"fare_amount": -5.0}, "amount_invalid"),
        ({"total_amount": -5.0}, "amount_invalid"),
        ({"total_amount": None}, "amount_invalid"),
        ({"fare_amount": float("nan")}, "amount_invalid"),
        ({"total_amount": float("nan")}, "amount_invalid"),
        ({"trip_distance": float("nan")}, "distance_invalid"),
        ({"PULocationID": 0}, "location_invalid"),
        ({"DOLocationID": 266}, "location_invalid"),
        ({"PULocationID": None}, "location_invalid"),
        ({"passenger_count": 12}, "passenger_count_invalid"),
        ({"passenger_count": -1}, "passenger_count_invalid"),
    ],
)
def test_rule_rejects(
    bronze: Callable[..., DataFrame], override: dict[str, Any], expected: str
) -> None:
    assert reason(bronze, override) == expected


def test_first_violated_rule_wins(bronze: Callable[..., DataFrame]) -> None:
    assert reason(bronze, {"trip_distance": -1.0, "fare_amount": -5.0}) == "distance_invalid"


@pytest.mark.parametrize(
    ("pickup", "expected"),
    [
        (datetime(2024, 1, 1, 0, 0), None),
        (datetime(2024, 1, 31, 23, 59, 59), None),
        (datetime(2023, 12, 31, 23, 59, 59), "pickup_outside_month"),
        (datetime(2024, 2, 1, 0, 0), "pickup_outside_month"),
    ],
)
def test_month_bounds(
    bronze: Callable[..., DataFrame], pickup: datetime, expected: str | None
) -> None:
    override = {"tpep_pickup_datetime": pickup, "tpep_dropoff_datetime": pickup}
    assert reason(bronze, override) == expected


def test_december_bounds_roll_over_year(bronze: Callable[..., DataFrame]) -> None:
    pickup = datetime(2024, 12, 31, 23, 0)
    override = {"tpep_pickup_datetime": pickup, "tpep_dropoff_datetime": pickup}
    assert reason(bronze, override, Month(2024, 12)) is None
