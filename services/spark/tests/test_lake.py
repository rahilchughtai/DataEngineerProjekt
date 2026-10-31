from datetime import datetime
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from pipeline_common.periods import Month
from spark_jobs import lake

MARCH = Month(2024, 3)


def test_with_lineage_adds_run_and_partition_columns(spark: SparkSession) -> None:
    processed_at = datetime(2024, 4, 1, 12, 0)
    df = lake.with_lineage(spark.range(1), MARCH, "run", processed_at)
    row = df.first()
    assert (row["_run_id"], row["year"], row["month"]) == ("run", "2024", "03")
    assert df.schema["year"].dataType.simpleString() == "string"


def test_removed_on_failure_deletes_month_in_all_tables(
    spark: SparkSession, tmp_path: Path
) -> None:
    tables = [f"file://{tmp_path}/silver", f"file://{tmp_path}/quarantine"]
    other = tmp_path / "silver" / Month(2024, 2).partition
    for table in ("silver", "quarantine"):
        (tmp_path / table / MARCH.partition).mkdir(parents=True)
    other.mkdir(parents=True)

    with pytest.raises(RuntimeError), lake.removed_on_failure(spark, MARCH, tables):
        raise RuntimeError("Schreiben fehlgeschlagen")

    assert not (tmp_path / "silver" / MARCH.partition).exists()
    assert not (tmp_path / "quarantine" / MARCH.partition).exists()
    assert other.exists()


def test_removed_on_failure_keeps_month_on_success(spark: SparkSession, tmp_path: Path) -> None:
    (tmp_path / "silver" / MARCH.partition).mkdir(parents=True)
    with lake.removed_on_failure(spark, MARCH, [f"file://{tmp_path}/silver"]):
        pass
    assert (tmp_path / "silver" / MARCH.partition).exists()
