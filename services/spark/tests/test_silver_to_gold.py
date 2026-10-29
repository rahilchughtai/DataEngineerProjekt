import json
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from spark_jobs.lake import LakePaths
from spark_jobs.periods import Month
from spark_jobs.silver_to_gold import (
    add_calendar_features,
    aggregate,
    hour_grid,
    process_month,
    run,
)

JANUARY = Month(2024, 1)
FEBRUARY = Month(2024, 2)
RUN_ID = "20240401T000000Z"
SILVER_RUN_ID = "20240331T000000Z"
# Drei Zonen statt 265, damit die Tests nicht für jeden Monat 197.160 Zeilen lokal rechnen.
ZONES = (1, 161, 237)
HOURS_JANUARY = 31 * 24

SILVER_SCHEMA = (
    "pickup_datetime timestamp_ntz, dropoff_datetime timestamp_ntz, pu_location_id int, "
    "do_location_id int, passenger_count int, trip_distance double, fare_amount double, "
    "total_amount double, payment_type int, _run_id string"
)

TRIP: dict[str, Any] = {
    "pickup_datetime": datetime(2024, 1, 15, 8, 30),
    "dropoff_datetime": datetime(2024, 1, 15, 8, 50),
    "pu_location_id": 161,
    "do_location_id": 237,
    "passenger_count": 1,
    "trip_distance": 3.0,
    "fare_amount": 17.0,
    "total_amount": 24.0,
    "payment_type": 1,
    "_run_id": SILVER_RUN_ID,
}


@pytest.fixture
def paths(tmp_path: Path) -> LakePaths:
    root = tmp_path.as_uri()
    return LakePaths(
        bronze=f"{root}/bronze",
        silver=f"{root}/silver",
        gold=f"{root}/gold",
        dataset="yellow_tripdata",
    )


@pytest.fixture
def silver(spark: SparkSession) -> Callable[..., DataFrame]:
    """Baut Silver-Zeilen; jede Zeile überschreibt Felder einer gültigen Fahrt."""

    def build(*overrides: dict[str, Any]) -> DataFrame:
        return spark.createDataFrame([TRIP | o for o in overrides], schema=SILVER_SCHEMA)

    return build


@pytest.fixture
def put_silver(paths: LakePaths) -> Callable[[Month, DataFrame], None]:
    def put(month: Month, df: DataFrame) -> None:
        df.coalesce(1).write.mode("overwrite").parquet(paths.silver_month(month))

    return put


def at(day: int, hour: int, minute: int = 0, month: int = 1) -> dict[str, datetime]:
    pickup = datetime(2024, month, day, hour, minute)
    return {"pickup_datetime": pickup, "dropoff_datetime": pickup.replace(minute=59)}


def gold(spark: SparkSession, paths: LakePaths) -> DataFrame:
    return spark.read.parquet(paths.gold_table())


def gold_partition(paths: LakePaths, month: str) -> Path:
    return Path(paths.gold_table().removeprefix("file://")) / "year=2024" / f"month={month}"


@pytest.mark.parametrize(
    ("month", "hours", "first", "last"),
    [
        (JANUARY, HOURS_JANUARY, datetime(2024, 1, 1, 0), datetime(2024, 1, 31, 23)),
        (FEBRUARY, 29 * 24, datetime(2024, 2, 1, 0), datetime(2024, 2, 29, 23)),
        (Month(2023, 12), 31 * 24, datetime(2023, 12, 1, 0), datetime(2023, 12, 31, 23)),
    ],
)
def test_hour_grid_covers_every_zone_and_hour(
    spark: SparkSession, month: Month, hours: int, first: datetime, last: datetime
) -> None:
    grid = hour_grid(spark, month, ZONES)

    assert grid.count() == len(ZONES) * hours
    bounds = grid.selectExpr("min(pickup_hour) AS first", "max(pickup_hour) AS last").first()
    assert (bounds["first"], bounds["last"]) == (first, last)


def test_aggregate_counts_and_averages(
    spark: SparkSession, silver: Callable[..., DataFrame]
) -> None:
    trips = silver(
        at(15, 8, 0) | {"trip_distance": 2.0, "fare_amount": 10.0},
        at(15, 8, 59) | {"trip_distance": 4.0, "fare_amount": 20.0},
        at(15, 9, 0),
    )

    rows = {
        (r["pickup_hour"], r["pu_location_id"]): r
        for r in aggregate(trips, hour_grid(spark, JANUARY, ZONES)).collect()
    }

    eight = rows[(datetime(2024, 1, 15, 8), 161)]
    assert eight["trip_count"] == 2
    assert eight["avg_trip_distance"] == 3.0
    assert eight["avg_fare_amount"] == 15.0
    assert eight["avg_duration_minutes"] == pytest.approx((59 + 0) / 2)
    assert rows[(datetime(2024, 1, 15, 9), 161)]["trip_count"] == 1

    empty = rows[(datetime(2024, 1, 15, 8), 237)]
    assert empty["trip_count"] == 0
    assert empty["avg_trip_distance"] is None
    assert sum(r["trip_count"] for r in rows.values()) == 3


def test_last_minute_of_month_stays_in_last_hour(
    spark: SparkSession, silver: Callable[..., DataFrame]
) -> None:
    trips = silver(at(31, 23, 59))

    hours = [
        r["pickup_hour"]
        for r in aggregate(trips, hour_grid(spark, JANUARY, ZONES)).collect()
        if r["trip_count"]
    ]

    assert hours == [datetime(2024, 1, 31, 23)]


@pytest.mark.parametrize(
    ("hour", "day_of_week", "is_weekend"),
    [
        (datetime(2024, 1, 15, 8), 1, False),  # Montag
        (datetime(2024, 1, 19, 23), 5, False),  # Freitag
        (datetime(2024, 1, 20, 0), 6, True),  # Samstag
        (datetime(2024, 1, 21, 17), 7, True),  # Sonntag
    ],
)
def test_calendar_features(
    spark: SparkSession, hour: datetime, day_of_week: int, is_weekend: bool
) -> None:
    df = spark.createDataFrame([(hour,)], "pickup_hour timestamp_ntz")

    row = add_calendar_features(df).first()

    assert row["hour_of_day"] == hour.hour
    assert row["day_of_week"] == day_of_week
    assert row["is_weekend"] is is_weekend


def test_process_month_writes_dense_gold(
    spark: SparkSession, silver: Callable[..., DataFrame], put_silver: Any, paths: LakePaths
) -> None:
    put_silver(JANUARY, silver(at(15, 8), at(15, 8, 30), at(20, 18) | {"pu_location_id": 1}))

    entry = process_month(spark, JANUARY, paths, RUN_ID, ZONES)

    assert entry["status"] == "processed"
    assert entry["rows_silver"] == 3
    assert entry["trips_in_gold"] == 3
    assert entry["rows_gold"] == len(ZONES) * HOURS_JANUARY
    assert entry["rows_without_trips"] == entry["rows_gold"] - 2
    assert entry["silver_run_ids"] == [SILVER_RUN_ID]

    rows = gold(spark, paths).collect()
    assert {(r["year"], r["month"]) for r in rows} == {("2024", "01")}
    assert {r["_run_id"] for r in rows} == {RUN_ID}
    assert len(list(gold_partition(paths, "01").glob("*.parquet"))) == 1


def test_rerun_is_idempotent(
    spark: SparkSession, silver: Callable[..., DataFrame], put_silver: Any, paths: LakePaths
) -> None:
    put_silver(JANUARY, silver(at(15, 8)))

    process_month(spark, JANUARY, paths, RUN_ID, ZONES)
    process_month(spark, JANUARY, paths, "20240402T000000Z", ZONES)

    rows = gold(spark, paths).collect()
    assert len(rows) == len(ZONES) * HOURS_JANUARY
    assert sum(r["trip_count"] for r in rows) == 1
    assert {r["_run_id"] for r in rows} == {"20240402T000000Z"}


def test_rerun_leaves_other_months_untouched(
    spark: SparkSession, silver: Callable[..., DataFrame], put_silver: Any, paths: LakePaths
) -> None:
    put_silver(JANUARY, silver(at(15, 8)))
    put_silver(FEBRUARY, silver(at(3, 9, month=2), at(4, 9, month=2)))
    process_month(spark, JANUARY, paths, RUN_ID, ZONES)
    process_month(spark, FEBRUARY, paths, RUN_ID, ZONES)

    process_month(spark, JANUARY, paths, "20240402T000000Z", ZONES)

    by_month = gold(spark, paths).groupBy("month").agg(F.sum("trip_count").alias("trips"))
    trips = {r["month"]: r["trips"] for r in by_month.collect()}
    assert trips == {"01": 1, "02": 2}


def test_missing_silver_month(spark: SparkSession, paths: LakePaths) -> None:
    entry = process_month(spark, JANUARY, paths, RUN_ID, ZONES)
    assert entry["status"] == "missing"


def test_reconciliation_failure_discards_month(
    spark: SparkSession, silver: Callable[..., DataFrame], put_silver: Any, paths: LakePaths
) -> None:
    put_silver(JANUARY, silver(at(15, 8)))
    process_month(spark, JANUARY, paths, RUN_ID, ZONES)

    # Eine Fahrt außerhalb des Rasters fiele beim Join still weg; der Abgleich muss das bemerken.
    put_silver(JANUARY, silver(at(15, 8), at(15, 9) | {"pu_location_id": 42}))
    entry = process_month(spark, JANUARY, paths, "20240402T000000Z", ZONES)

    assert entry["status"] == "failed"
    assert "Gold enthält 1 Fahrten, Silver 2" in entry["error"]
    assert not gold_partition(paths, "01").exists()


def test_run_writes_report(
    spark: SparkSession, silver: Callable[..., DataFrame], put_silver: Any, paths: LakePaths
) -> None:
    put_silver(JANUARY, silver(at(15, 8)))

    report = run(spark, [JANUARY, FEBRUARY], paths, RUN_ID, ZONES)

    assert report["job"] == "silver_to_gold"
    assert report["counts"] == {"processed": 1, "missing": 1}
    stored = json.loads(Path(paths.gold_run_report(RUN_ID).removeprefix("file://")).read_text())
    assert stored == report
