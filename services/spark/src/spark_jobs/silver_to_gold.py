"""Spark-Job 2: Silver → Gold.

Aggregiert pro Monat die bereinigten Fahrten aus Silver zur stündlichen Nachfrage je Pickup-Zone.
Das Raster ist dicht: Jede Zone und jede Stunde des Monats steht in Gold, Stunden ohne Fahrten mit
trip_count = 0, weil auch ausbleibende Nachfrage ein Trainingssignal ist. Gold ist wie Silver nach
year/month partitioniert und wird pro Monat überschrieben (idempotent). Bevor geschrieben wird,
muss die Summe von trip_count genau der Zahl der Silver-Zeilen entsprechen und Silver aus genau
einem Lauf von Job 1 stammen; sonst bleibt der bisherige Gold-Monat unverändert.
"""

import logging
import sys
import time
from collections.abc import Iterable
from datetime import datetime
from functools import partial
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pipeline_common.periods import Month
from pipeline_common.reports import PROCESSED
from pipeline_common.timing import elapsed, now_utc
from spark_jobs import job, lake
from spark_jobs.lake import LakePaths
from spark_jobs.schema import LOCATION_ID_RANGE, duration_seconds, month_bounds
from spark_jobs.session import build_session

JOB = "silver_to_gold"
log = logging.getLogger(JOB)

ZONES = range(LOCATION_ID_RANGE[0], LOCATION_ID_RANGE[1] + 1)

GOLD_COLUMNS = [
    "pickup_hour",
    "pu_location_id",
    "trip_count",
    "avg_trip_distance",
    "avg_duration_minutes",
    "avg_fare_amount",
    "avg_total_amount",
    "hour_of_day",
    "day_of_week",
    "is_weekend",
    *lake.LINEAGE_COLUMNS,
]


class ReconciliationError(RuntimeError):
    """Gold enthält nicht genau die Fahrten aus Silver."""


def hour_grid(spark: SparkSession, month: Month, zones: Iterable[int] = ZONES) -> DataFrame:
    """Jede Kombination aus Zone und Stunde des Monats."""
    start, end = month_bounds(month)
    one_hour = F.expr("INTERVAL 1 HOUR")
    hours = spark.range(1).select(
        F.explode(F.sequence(start, end - one_hour, one_hour)).alias("pickup_hour")
    )
    zone_ids = spark.createDataFrame([(z,) for z in zones], "pu_location_id int")
    return hours.crossJoin(zone_ids)


def aggregate(silver: DataFrame, grid: DataFrame) -> DataFrame:
    hourly = (
        silver.withColumn("pickup_hour", F.date_trunc("hour", "pickup_datetime"))
        .groupBy("pickup_hour", "pu_location_id")
        .agg(
            F.count("*").alias("trip_count"),
            F.avg("trip_distance").alias("avg_trip_distance"),
            F.avg(duration_seconds() / 60).alias("avg_duration_minutes"),
            F.avg("fare_amount").alias("avg_fare_amount"),
            F.avg("total_amount").alias("avg_total_amount"),
        )
    )
    # Left Join vom Raster: Zone-Stunden ohne Fahrten bleiben mit 0 erhalten, die Mittelwerte null.
    return grid.join(hourly, ["pickup_hour", "pu_location_id"], "left").fillna(
        0, subset=["trip_count"]
    )


def add_calendar_features(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("hour_of_day", F.hour("pickup_hour"))
        # weekday zählt ab 0 = Montag, dayofweek dagegen ab 1 = Sonntag.
        .withColumn("day_of_week", F.weekday("pickup_hour") + 1)
        .withColumn("is_weekend", F.col("day_of_week") >= 6)
    )


def build_gold(
    silver: DataFrame, grid: DataFrame, month: Month, run_id: str, processed_at: datetime
) -> DataFrame:
    features = add_calendar_features(aggregate(silver, grid))
    return lake.with_lineage(features, month, run_id, processed_at).select(*GOLD_COLUMNS)


def process_month(
    spark: SparkSession,
    month: Month,
    paths: LakePaths,
    run_id: str,
    zones: Iterable[int] = ZONES,
) -> dict[str, Any]:
    process = partial(_process_month, spark, month, paths, run_id, zones)
    return job.guarded_month(spark, month, paths.silver_month(month), log, process)


def check_month(stats: dict[str, Any], silver_run_ids: list[str]) -> None:
    """Prüft vor dem Schreiben; schlägt die Prüfung fehl, bleibt der bisherige Gold-Monat stehen."""
    # Job 1 ersetzt einen Monat immer ganz. Mehrere Läufe in einer Partition deuten auf einen
    # abgebrochenen Commit oder parallele Läufe hin.
    if len(silver_run_ids) > 1:
        raise ReconciliationError(
            f"Silver enthält Daten aus mehreren Läufen von Job 1: {', '.join(silver_run_ids)}"
        )
    if stats["trips_in_gold"] != stats["rows_silver"]:
        raise ReconciliationError(
            f"Gold enthält {stats['trips_in_gold']} Fahrten, Silver {stats['rows_silver']}"
        )


def write_month(
    spark: SparkSession, gold: DataFrame, paths: LakePaths, month: Month, stats: dict[str, Any]
) -> None:
    """Überschreibt den Monat in Gold; scheitert das Schreiben, fehlt er ganz statt halb ersetzt."""
    table = paths.gold_table()
    with lake.removed_on_failure(spark, month, (table,)):
        # Ein Monat Gold sind wenige MB; ohne coalesce entstünden 200 Dateien (shuffle.partitions).
        lake.write_partition(spark, gold.coalesce(1), table, month, stats["rows_gold"] > 0)


def _process_month(
    spark: SparkSession,
    month: Month,
    paths: LakePaths,
    run_id: str,
    zones: Iterable[int],
    source: str,
) -> dict[str, Any]:
    """Felder des Monats im Report: Status, Zähler, Lineage und Laufzeit."""
    started = time.monotonic()
    processed_at = now_utc()
    with job.cached(spark.read.parquet(source)) as silver:
        # Lineage: aus welchen Läufen von Job 1 die Silver-Daten des Monats stammen.
        by_run = {
            row["_run_id"]: row["count"] for row in silver.groupBy("_run_id").count().collect()
        }
        silver_run_ids = sorted(by_run)
        grid = hour_grid(spark, month, zones)
        with job.cached(build_gold(silver, grid, month, run_id, processed_at)) as gold:
            totals = gold.agg(
                F.count("*").alias("rows_gold"),
                F.sum("trip_count").alias("trips_in_gold"),
                F.sum(F.when(F.col("trip_count") == 0, 1).otherwise(0)).alias("rows_without_trips"),
            ).first()
            stats = {
                "rows_silver": sum(by_run.values()),
                "rows_gold": totals["rows_gold"],
                "trips_in_gold": totals["trips_in_gold"] or 0,
                "rows_without_trips": totals["rows_without_trips"] or 0,
            }
            check_month(stats, silver_run_ids)
            write_month(spark, gold, paths, month, stats)

    seconds = elapsed(started)
    log.info(
        "month=%s status=processed rows_silver=%d rows_gold=%d without_trips=%d seconds=%.1f",
        month,
        stats["rows_silver"],
        stats["rows_gold"],
        stats["rows_without_trips"],
        seconds,
    )
    return {"status": PROCESSED} | stats | {"silver_run_ids": silver_run_ids, "seconds": seconds}


def run(
    spark: SparkSession,
    months: list[Month],
    paths: LakePaths,
    run_id: str,
    zones: Iterable[int] = ZONES,
) -> dict[str, Any]:
    started = time.monotonic()
    results = [process_month(spark, month, paths, run_id, zones) for month in months]
    return job.write_report(
        spark,
        started,
        results,
        run_id=run_id,
        dataset=paths.dataset,
        job=JOB,
        report_path=paths.gold_run_report(run_id),
        log=log,
    )


def main() -> int:
    return job.main(JOB, run, build_session, log)


if __name__ == "__main__":
    sys.exit(main())
