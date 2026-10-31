import json
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pipeline_common.periods import Month
from spark_jobs import lake
from spark_jobs.bronze_to_silver import files_for, process_month, run
from spark_jobs.lake import LakePaths

JANUARY = Month(2024, 1)
FEBRUARY = Month(2024, 2)
RUN_ID = "20240401T000000Z"


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
    assert {(r["year"], r["month"]) for r in rows} == {("2024", "01")}
    assert {r["_run_id"] for r in rows} == {RUN_ID}
    assert all("/bronze/yellow_tripdata/year=2024/month=01/" in r["_source_file"] for r in rows)

    quarantine = spark.read.parquet(paths.quarantine_table()).collect()
    assert sorted(r["reject_reason"] for r in quarantine) == ["amount_invalid", "location_invalid"]

    for table in (paths.silver_table(), paths.quarantine_table()):
        partition = Path(table.removeprefix("file://")) / "year=2024" / "month=01"
        assert len(list(partition.glob("*.parquet"))) == 1


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
    assert counts == {"01": 2, "02": 1}


def test_same_business_values_different_vendor_are_kept(
    spark: SparkSession, bronze: Callable[..., DataFrame], put_bronze: Any, paths: LakePaths
) -> None:
    put_bronze(JANUARY, bronze({"VendorID": 1}, {"VendorID": 2}))

    entry = process_month(spark, JANUARY, paths, RUN_ID)

    assert entry["rows_valid"] == 2
    assert entry["duplicates"] == 0


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
    stored = json.loads(Path(paths.silver_run_report(RUN_ID).removeprefix("file://")).read_text())
    assert stored == report


def test_schema_error_marks_month_failed(
    spark: SparkSession, bronze: Callable[..., DataFrame], put_bronze: Any, paths: LakePaths
) -> None:
    put_bronze(JANUARY, bronze({}).drop("PULocationID"))

    report = run(spark, [JANUARY], paths, RUN_ID)

    assert report["counts"] == {"failed": 1}
    assert "PULocationID" in report["months"][0]["error"]


def test_unconvertible_value_is_quarantined_not_fatal(
    bronze: Callable[..., DataFrame], put_bronze: Any, spark: SparkSession, paths: LakePaths
) -> None:
    # Eine gültige Fahrt dazu, sonst wäre der Monat ganz abgelehnt und damit failed.
    unconvertible = F.when(F.col("VendorID") == 1, F.lit(1e12)).otherwise(F.col("PULocationID"))
    put_bronze(JANUARY, bronze({}, {"VendorID": 1}).withColumn("PULocationID", unconvertible))

    entry = process_month(spark, JANUARY, paths, RUN_ID)

    assert entry["status"] == "processed"
    assert entry["rows_valid"] == 1
    assert entry["rejected_by_rule"]["location_invalid"] == 1


def test_all_rows_rejected_marks_month_failed(
    spark: SparkSession, bronze: Callable[..., DataFrame], put_bronze: Any, paths: LakePaths
) -> None:
    put_bronze(JANUARY, january_sample(bronze))
    process_month(spark, JANUARY, paths, RUN_ID)

    put_bronze(JANUARY, bronze({"fare_amount": -3.0}, {"PULocationID": 999}))
    entry = process_month(spark, JANUARY, paths, "20240402T000000Z")

    assert entry["status"] == "failed"
    assert entry["rows_valid"] == 0
    assert entry["rows_rejected"] == 2
    assert {r["_run_id"] for r in silver(spark, paths).collect()} == {RUN_ID}
    assert spark.read.parquet(paths.quarantine_table()).count() == 2


def test_empty_bronze_marks_month_failed(
    spark: SparkSession, bronze: Callable[..., DataFrame], put_bronze: Any, paths: LakePaths
) -> None:
    put_bronze(JANUARY, bronze())

    report = run(spark, [JANUARY], paths, RUN_ID)

    assert report["counts"] == {"failed": 1}
    assert report["months"][0]["rows_in"] == 0
    assert not Path(paths.silver_table().removeprefix("file://")).exists()


def test_failure_between_writes_discards_month(
    spark: SparkSession,
    bronze: Callable[..., DataFrame],
    put_bronze: Any,
    paths: LakePaths,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    put_bronze(JANUARY, january_sample(bronze))
    process_month(spark, JANUARY, paths, RUN_ID)
    original = lake.write_partition

    def fail_on_quarantine(spark: SparkSession, df: DataFrame, table: str, *args: Any) -> None:
        if "_quarantine" in table:
            raise OSError("Schreibfehler")
        original(spark, df, table, *args)

    monkeypatch.setattr(lake, "write_partition", fail_on_quarantine)

    entry = process_month(spark, JANUARY, paths, "20240402T000000Z")

    assert entry["status"] == "failed"
    assert "Schreibfehler" in entry["error"]
    for table in (paths.silver_table(), paths.quarantine_table()):
        assert not (Path(table.removeprefix("file://")) / "year=2024" / "month=01").exists()


@pytest.mark.parametrize(
    ("rows", "files"), [(0, 1), (1, 1), (1_000_000, 1), (1_000_001, 2), (3_523_771, 4)]
)
def test_files_for(rows: int, files: int) -> None:
    assert files_for(rows) == files
