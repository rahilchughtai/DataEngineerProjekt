"""Spark-Job 2: Silver → Gold.

Aggregiert pro Monat die bereinigten Fahrten aus Silver zur stündlichen Nachfrage je Pickup-Zone.
Das Raster ist dicht: Jede Zone und jede Stunde des Monats steht in Gold, Stunden ohne Fahrten mit
trip_count = 0, weil auch ausbleibende Nachfrage ein Trainingssignal ist. Gold ist wie Silver nach
year/month partitioniert und wird pro Monat überschrieben (idempotent). Bevor geschrieben wird,
muss die Summe von trip_count genau der Zahl der Silver-Zeilen entsprechen und Silver aus genau
einem Lauf von Job 1 stammen; sonst bleibt der bisherige Gold-Monat unverändert.
"""

import json
import logging
import sys
import time
from collections.abc import Iterable
from datetime import datetime, timezone
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pipeline_common.logs import setup_logging
from pipeline_common.periods import Month, months_from_env
from pipeline_common.reports import run_report
from pipeline_common.run_id import read_run_id
from spark_jobs import lake
from spark_jobs.lake import LakePaths
from spark_jobs.quality import LOCATION_ID_RANGE, month_bounds
from spark_jobs.runtime import exit_code
from spark_jobs.session import build_session

log = logging.getLogger("silver_to_gold")

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
    "_run_id",
    "_processed_at",
    "year",
    "month",
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
    duration_minutes = F.timestamp_diff(
        "SECOND", F.col("pickup_datetime"), F.col("dropoff_datetime")
    )
    hourly = (
        silver.withColumn("pickup_hour", F.date_trunc("hour", "pickup_datetime"))
        .groupBy("pickup_hour", "pu_location_id")
        .agg(
            F.count("*").alias("trip_count"),
            F.avg("trip_distance").alias("avg_trip_distance"),
            F.avg(duration_minutes / 60).alias("avg_duration_minutes"),
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
    return (
        add_calendar_features(aggregate(silver, grid))
        .withColumn("_run_id", F.lit(run_id))
        .withColumn("_processed_at", F.lit(processed_at))
        .withColumn("year", F.lit(f"{month.year:04d}"))
        .withColumn("month", F.lit(f"{month.month:02d}"))
        .select(*GOLD_COLUMNS)
    )


def process_month(
    spark: SparkSession,
    month: Month,
    paths: LakePaths,
    run_id: str,
    zones: Iterable[int] = ZONES,
) -> dict[str, Any]:
    source = paths.silver_month(month)
    entry: dict[str, Any] = {"month": str(month), "source": source}
    try:
        return _process_month(spark, month, paths, run_id, zones, entry)
    except Exception as err:
        log.exception("month=%s status=failed", month)
        return entry | {"status": "failed", "error": str(err)}


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
    try:
        # Ein Monat Gold sind wenige MB; ohne coalesce entstünden 200 Dateien (shuffle.partitions).
        lake.write_partition(
            spark, gold.coalesce(1), paths.gold_table(), month, stats["rows_gold"] > 0
        )
    except Exception:
        target = f"{paths.gold_table()}/{lake.partition(month)}"
        try:
            lake.delete(spark, target)
        except Exception:
            log.exception("month=%s Aufräumen fehlgeschlagen: %s", month, target)
        raise


def _process_month(
    spark: SparkSession,
    month: Month,
    paths: LakePaths,
    run_id: str,
    zones: Iterable[int],
    entry: dict[str, Any],
) -> dict[str, Any]:
    source = entry["source"]
    started = time.monotonic()
    if not lake.exists(spark, source):
        log.warning("month=%s status=missing source=%s", month, source)
        return entry | {"status": "missing"}

    processed_at = datetime.now(timezone.utc).replace(microsecond=0)
    silver = spark.read.parquet(source).cache()
    try:
        # Lineage: aus welchen Läufen von Job 1 die Silver-Daten des Monats stammen.
        by_run = {
            row["_run_id"]: row["count"] for row in silver.groupBy("_run_id").count().collect()
        }
        gold = build_gold(silver, hour_grid(spark, month, zones), month, run_id, processed_at)
        gold = gold.cache()
        try:
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
            check_month(stats, sorted(by_run))
            write_month(spark, gold, paths, month, stats)
        finally:
            gold.unpersist()
    finally:
        silver.unpersist()

    seconds = round(time.monotonic() - started, 2)
    log.info(
        "month=%s status=processed rows_silver=%d rows_gold=%d without_trips=%d seconds=%.1f",
        month,
        stats["rows_silver"],
        stats["rows_gold"],
        stats["rows_without_trips"],
        seconds,
    )
    return (
        entry
        | {"status": "processed"}
        | stats
        | {"silver_run_ids": sorted(by_run), "seconds": seconds}
    )


def run(
    spark: SparkSession,
    months: list[Month],
    paths: LakePaths,
    run_id: str,
    zones: Iterable[int] = ZONES,
) -> dict[str, Any]:
    results: list[dict[str, Any]] = []
    started = time.monotonic()
    for month in months:
        results.append(process_month(spark, month, paths, run_id, zones))

    report = run_report(run_id, paths.dataset, started, results, job="silver_to_gold")
    counts = report["counts"]
    report_path = paths.gold_run_report(run_id)
    lake.write_text(spark, report_path, json.dumps(report, indent=2))
    log.info("run=%s finished counts=%s report=%s", run_id, counts, report_path)
    return report


def main() -> int:
    setup_logging(quiet=("py4j",))
    try:
        months = months_from_env()
        paths = LakePaths.from_env()
        run_id = read_run_id()
        spark = build_session("silver_to_gold")
    except ValueError as err:
        log.error("%s", err)
        return 2

    log.info("run=%s months=%s", run_id, ",".join(map(str, months)))
    try:
        report = run(spark, months, paths, run_id)
    finally:
        spark.stop()
    return exit_code(report, "Silver")


if __name__ == "__main__":
    sys.exit(main())
