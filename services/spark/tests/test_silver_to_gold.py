from datetime import datetime, timedelta

import pytest
from pyspark.sql import SparkSession

from silver_to_gold import hourly_demand

SILVER_SCHEMA = (
    "pickup_datetime timestamp_ntz, dropoff_datetime timestamp_ntz, pu_location_id int, "
    "trip_distance double, fare_amount double, total_amount double"
)


def trip(pickup: datetime, minutes: int, zone: int = 161, distance: float = 2.0) -> tuple:
    dropoff = pickup + timedelta(minutes=minutes)
    return (pickup, dropoff, zone, distance, 10.0, 15.0)


def test_counts_and_averages_per_zone_and_hour(spark: SparkSession) -> None:
    silver = spark.createDataFrame(
        [
            trip(datetime(2024, 1, 15, 8, 5), 10, distance=2.0),
            trip(datetime(2024, 1, 15, 8, 40), 20, distance=4.0),
            trip(datetime(2024, 1, 15, 9, 0), 10),
            trip(datetime(2024, 1, 15, 8, 10), 10, zone=237),
        ],
        SILVER_SCHEMA,
    )
    rows = {
        (r["pickup_hour"].hour, r["pu_location_id"]): r for r in hourly_demand(silver).collect()
    }

    assert set(rows) == {(8, 161), (9, 161), (8, 237)}
    eight = rows[(8, 161)]
    assert eight["trip_count"] == 2
    assert eight["avg_trip_distance"] == 3.0
    assert eight["avg_duration_minutes"] == 15.0


@pytest.mark.parametrize(
    ("pickup", "day_of_week", "is_weekend"),
    [(datetime(2024, 1, 15, 8), 1, False), (datetime(2024, 1, 21, 8), 7, True)],
)
def test_calendar_features(
    spark: SparkSession, pickup: datetime, day_of_week: int, is_weekend: bool
) -> None:
    row = hourly_demand(spark.createDataFrame([trip(pickup, 10)], SILVER_SCHEMA)).first()
    assert (row["hour_of_day"], row["day_of_week"], row["is_weekend"]) == (
        8,
        day_of_week,
        is_weekend,
    )
