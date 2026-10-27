import json
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from spark_jobs import bronze_to_silver
from spark_jobs.bronze_to_silver import exit_code, process_month, run
from spark_jobs.lake import LakePaths
from spark_jobs.periods import Month

JANUARY = Month(2024, 1)
FEBRUARY = Month(2024, 2)
RUN_ID = "20240401T000000Z"


@pytest.fixture
def paths(tmp_path: Path) -> LakePaths:
    root = tmp_path.as_uri()
    return LakePaths(bronze=f"{root}/bronze", silver=f"{root}/silver", dataset="yellow_tripdata")


@pytest.fixture
def put_bronze(paths: LakePaths) -> Callable[[Month, DataFrame], None]:
    def put(month: Month, df: DataFrame) -> None:
        df.coalesce(1).write.mode("overwrite").parquet(paths.bronze_month(month))

    return put


def january_sample(bronze: Callable[..., DataFrame]) -> DataFrame:
    # Zwei gültige Fahrten, davon eine doppelt, plus zwei abgelehnte.
    second = {"tpep_pickup_datetime": datetime(2024, 1, 20, 18, 0)}
    second |= {"tpep_dropoff_datetime": datetime(2024, 1, 20, 18, 25)}
    return bronze({}, second, second, {"fare_amount": -3.0}, {"PULocationID": 999})


def silver(spark: SparkSession, paths: LakePaths) -> DataFrame:
    return spark.read.parquet(paths.silver_table())


def test_process_month_counts_and_writes(
    spark: SparkSession, bronze: Callable[..., DataFrame], put_bronze: Any, paths: LakePaths
) -> None:
    put_bronze(JANUARY, january_sample(bronze))

    entry = process_month(spark, JANUARY, paths, RUN_ID)

    assert entry["status"] == "processed"
    assert entry["rows_in"] == 5
    assert entry["rows_valid"] == 2
    assert entry["duplicates"] == 1
    assert entry["rows_rejected"] == 2
    assert entry["rejected_by_rule"]["amount_invalid"] == 1
    assert entry["rejected_by_rule"]["location_invalid"] == 1
    assert entry["rows_in"] == entry["rows_valid"] + entry["duplicates"] + entry["rows_rejected"]

    rows = silver(spark, paths).collect()
    assert {(r["year"], r["month"]) for r in rows} == {(2024, 1)}
    assert {r["_run_id"] for r in rows} == {RUN_ID}
    assert all("/bronze/yellow_tripdata/year=2024/month=01/" in r["_source_file"] for r in rows)

    quarantine = spark.read.parquet(paths.quarantine_table()).collect()
    assert sorted(r["reject_reason"] for r in quarantine) == ["amount_invalid", "location_invalid"]


def test_rerun_is_idempotent(
    spark: SparkSession, bronze: Callable[..., DataFrame], put_bronze: Any, paths: LakePaths
) -> None:
    put_bronze(JANUARY, january_sample(bronze))

    process_month(spark, JANUARY, paths, RUN_ID)
    process_month(spark, JANUARY, paths, "20240402T000000Z")

    rows = silver(spark, paths).collect()
    assert len(rows) == 2
    assert {r["_run_id"] for r in rows} == {"20240402T000000Z"}
    assert spark.read.parquet(paths.quarantine_table()).count() == 2


def test_rerun_leaves_other_months_untouched(
    spark: SparkSession, bronze: Callable[..., DataFrame], put_bronze: Any, paths: LakePaths
) -> None:
    february = {
        "tpep_pickup_datetime": datetime(2024, 2, 3, 9, 0),
        "tpep_dropoff_datetime": datetime(2024, 2, 3, 9, 10),
    }
    put_bronze(JANUARY, january_sample(bronze))
    put_bronze(FEBRUARY, bronze(february))
    process_month(spark, JANUARY, paths, RUN_ID)
    process_month(spark, FEBRUARY, paths, RUN_ID)

    process_month(spark, JANUARY, paths, "20240402T000000Z")

    counts = {
        (r["month"]): r["count"] for r in silver(spark, paths).groupBy("month").count().collect()
    }
    assert counts == {1: 2, 2: 1}


def test_rerun_clears_stale_quarantine(
    spark: SparkSession, bronze: Callable[..., DataFrame], put_bronze: Any, paths: LakePaths
) -> None:
    put_bronze(JANUARY, january_sample(bronze))
    process_month(spark, JANUARY, paths, RUN_ID)

    # Korrigierte Quelldatei ohne fehlerhafte Zeilen: die alte Quarantäne darf nicht stehen bleiben.
    put_bronze(JANUARY, bronze({}))
    entry = process_month(spark, JANUARY, paths, "20240402T000000Z")

    assert entry["rows_rejected"] == 0
    quarantine_partition = Path(paths.quarantine_table().removeprefix("file://"))
    assert not (quarantine_partition / "year=2024" / "month=01").exists()


def test_missing_bronze_month(spark: SparkSession, paths: LakePaths) -> None:
    entry = process_month(spark, JANUARY, paths, RUN_ID)
    assert entry["status"] == "missing"


def test_run_writes_report(
    spark: SparkSession, bronze: Callable[..., DataFrame], put_bronze: Any, paths: LakePaths
) -> None:
    put_bronze(JANUARY, january_sample(bronze))

    report = run(spark, [JANUARY, FEBRUARY], paths, RUN_ID)

    assert report["counts"] == {"processed": 1, "missing": 1}
    stored = json.loads(Path(paths.run_report(RUN_ID).removeprefix("file://")).read_text())
    assert stored == report


def test_schema_error_marks_month_failed(
    spark: SparkSession, bronze: Callable[..., DataFrame], put_bronze: Any, paths: LakePaths
) -> None:
    put_bronze(JANUARY, bronze({}).drop("PULocationID"))

    report = run(spark, [JANUARY], paths, RUN_ID)

    assert report["counts"] == {"failed": 1}
    assert "PULocationID" in report["months"][0]["error"]
    assert report["months"][0]["written"] == []


def test_unconvertible_value_is_quarantined_not_fatal(
    bronze: Callable[..., DataFrame], put_bronze: Any, spark: SparkSession, paths: LakePaths
) -> None:
    put_bronze(JANUARY, bronze({}).withColumn("PULocationID", F.lit(1e12)))

    entry = process_month(spark, JANUARY, paths, RUN_ID)

    assert entry["status"] == "processed"
    assert entry["rejected_by_rule"]["location_invalid"] == 1


def test_failure_between_writes_reports_what_was_written(
    spark: SparkSession,
    bronze: Callable[..., DataFrame],
    put_bronze: Any,
    paths: LakePaths,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    put_bronze(JANUARY, january_sample(bronze))
    original = bronze_to_silver.write_partition

    def fail_on_quarantine(spark: SparkSession, df: DataFrame, table: str, *args: Any) -> None:
        if "_quarantine" in table:
            raise OSError("Schreibfehler")
        original(spark, df, table, *args)

    monkeypatch.setattr(bronze_to_silver, "write_partition", fail_on_quarantine)

    entry = process_month(spark, JANUARY, paths, RUN_ID)

    assert entry["status"] == "failed"
    assert entry["written"] == ["silver"]
    assert "Schreibfehler" in entry["error"]


@pytest.mark.parametrize(
    ("counts", "expected"),
    [
        ({"processed": 3}, 0),
        ({"processed": 2, "missing": 1}, 0),
        ({"missing": 3}, 3),
        ({}, 3),
        ({"processed": 2, "failed": 1}, 1),
        ({"failed": 1}, 1),
    ],
)
def test_exit_code(counts: dict[str, int], expected: int) -> None:
    assert exit_code({"counts": counts}) == expected
