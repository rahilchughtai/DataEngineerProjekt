from datetime import datetime, timedelta

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

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
    gold = hourly_demand(silver, 2024, 1).filter("trip_count > 0")
    rows = {(r["pickup_hour"].hour, r["pu_location_id"]): r for r in gold.collect()}

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
    silver = spark.createDataFrame([trip(pickup, 10)], SILVER_SCHEMA)
    row = hourly_demand(silver, 2024, 1).filter("trip_count > 0").first()
    assert (row["hour_of_day"], row["day_of_week"], row["is_weekend"]) == (
        8,
        day_of_week,
        is_weekend,
    )


def test_every_zone_and_hour_of_the_month_has_a_row(spark: SparkSession) -> None:
    silver = spark.createDataFrame([trip(datetime(2024, 2, 10, 8, 5), 10)], SILVER_SCHEMA)
    gold = hourly_demand(silver, 2024, 2)

    # February 2024 has 29 days; zones 1 to 263.
    assert gold.count() == 29 * 24 * 263
    assert gold.agg(F.sum("trip_count")).first()[0] == 1
    hour = datetime(2024, 2, 10, 9)
    empty = gold.filter((F.col("pickup_hour") == hour) & (F.col("pu_location_id") == 161)).first()
    assert empty["trip_count"] == 0
    assert empty["avg_fare_amount"] is None
    assert gold.agg(F.max("pickup_hour")).first()[0] == datetime(2024, 2, 29, 23)
