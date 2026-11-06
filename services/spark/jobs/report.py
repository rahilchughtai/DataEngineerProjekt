"""Run report: after gold, counts the rows of each month in all layers and records when.

The report is derived from the data itself, not from what the individual steps report, and is
stored as JSON under gold/_reports/<INGEST_PERIOD>/ and as report.json and report.md in
runs/<INGEST_PERIOD>/ on the host. It must hold that
bronze_rows = silver_rows + quarantined_rows + duplicates and gold_trips = silver_rows.
The timestamps bronze_loaded_at ≤ silver_processed_at ≤ gold_processed_at show the latency of
each stage: from storing the source file to finished features; bronze_to_silver_s and
silver_to_gold_s give these gaps in seconds. SHA-256 and Last-Modified of the source file come
from the ingestion manifest and show which source version the run processed.
"""

import json
import logging
import os
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from common import (
    BRONZE,
    GOLD,
    MANIFESTS,
    QUARANTINE,
    REPORTS,
    SILVER,
    month_path,
    period_months,
    start,
)

log = logging.getLogger("report")

# Bind mount of ./runs in docker-compose.yml.
LOCAL_RUNS = Path("/opt/runs")

SCHEMA = (
    "month string, bronze_rows long, silver_rows long, quarantined_rows long, duplicates long, "
    "rejected_by_reason map<string, long>, gold_rows long, gold_trips long, "
    "bronze_loaded_at timestamp, silver_processed_at timestamp, gold_processed_at timestamp, "
    "bronze_to_silver_s double, silver_to_gold_s double, "
    "source_sha256 string, source_last_modified string"
)
MANIFEST_SCHEMA = "sha256 string, last_modified string"


def seconds(start: datetime | None, end: datetime | None) -> float | None:
    return (end - start).total_seconds() if start and end else None


def month_report(spark: SparkSession, year: int, month: int) -> dict:
    def read(table: str):
        return spark.read.parquet(month_path(table, year, month))

    rows, latest = F.count("*").alias("rows"), F.max("_processed_at").alias("at")
    # When ingestion stored the file: modification time of the object in bronze.
    bronze = (
        read(BRONZE)
        .agg(F.count("*").alias("rows"), F.max("_metadata.file_modification_time").alias("at"))
        .first()
    )
    silver = read(SILVER).agg(rows, latest).first()
    by_reason = read(QUARANTINE).groupBy("reject_reason").count().collect()
    rejected = {row["reject_reason"]: row["count"] for row in by_reason}
    gold = read(GOLD).agg(rows, latest, F.sum("trip_count").alias("trips")).first()
    manifest = spark.read.json(
        f"{month_path(MANIFESTS, year, month)}/manifest.json", MANIFEST_SCHEMA, multiLine=True
    ).first()
    quarantined_rows = sum(rejected.values())
    return {
        "month": f"{year:04d}-{month:02d}",
        "bronze_rows": bronze["rows"],
        "silver_rows": silver["rows"],
        "quarantined_rows": quarantined_rows,
        "duplicates": bronze["rows"] - silver["rows"] - quarantined_rows,
        "rejected_by_reason": rejected,
        "gold_rows": gold["rows"],
        "gold_trips": gold["trips"] or 0,
        "bronze_loaded_at": bronze["at"],
        "silver_processed_at": silver["at"],
        "gold_processed_at": gold["at"],
        "bronze_to_silver_s": seconds(bronze["at"], silver["at"]),
        "silver_to_gold_s": seconds(silver["at"], gold["at"]),
        "source_sha256": manifest["sha256"],
        "source_last_modified": manifest["last_modified"],
    }


def pipeline_seconds(rows: list[dict]) -> float | None:
    """From the first source file stored in bronze to the last gold partition of the period."""
    return seconds(
        min(row["bronze_loaded_at"] for row in rows), max(row["gold_processed_at"] for row in rows)
    )


def markdown(period: str, rows: list[dict], pipeline_s: float | None) -> str:
    def cell(value: float | None) -> str:
        return "n/a" if value is None else f"{value:.1f}"

    rejected: dict[str, int] = {}
    for row in rows:
        for reason, count in row["rejected_by_reason"].items():
            rejected[reason] = rejected.get(reason, 0) + count
    lines = [
        f"# Run report {period}",
        "",
        f"Pipeline run time (first source file in bronze to last gold partition): "
        f"{cell(pipeline_s)} s",
        "",
        "| Month | Bronze | Silver | Quarantined | Duplicates | Gold rows | Gold trips "
        "| Bronze → silver (s) | Silver → gold (s) |",
        "|---|--:|--:|--:|--:|--:|--:|--:|--:|",
        *(
            f"| {r['month']} | {r['bronze_rows']} | {r['silver_rows']} | {r['quarantined_rows']} "
            f"| {r['duplicates']} | {r['gold_rows']} | {r['gold_trips']} "
            f"| {cell(r['bronze_to_silver_s'])} | {cell(r['silver_to_gold_s'])} |"
            for r in rows
        ),
        "",
        "| Reject reason | Rows |",
        "|---|--:|",
        *(f"| {reason} | {count} |" for reason, count in sorted(rejected.items())),
        "",
        "| Month | Source last modified | Source SHA-256 |",
        "|---|---|---|",
        *(f"| {r['month']} | {r['source_last_modified']} | `{r['source_sha256']}` |" for r in rows),
    ]
    return "\n".join(lines) + "\n"


def write_local(directory: Path, period: str, rows: list[dict]) -> None:
    pipeline_s = pipeline_seconds(rows)
    directory.mkdir(parents=True, exist_ok=True)
    report = {"period": period, "pipeline_s": pipeline_s, "months": rows}
    (directory / "report.json").write_text(
        json.dumps(report, indent=2, default=str) + "\n", encoding="utf-8"
    )
    (directory / "report.md").write_text(markdown(period, rows, pipeline_s), encoding="utf-8")
    log.info("local report: runs/%s/report.json and report.md", period)


def main() -> None:
    spark = start("report")
    rows = [month_report(spark, year, month) for year, month in period_months()]
    for row in rows:
        log.info("%s", row)
    period = os.environ["INGEST_PERIOD"].strip()
    spark.createDataFrame(rows, SCHEMA).coalesce(1).write.mode("overwrite").json(
        f"{REPORTS}/{period}"
    )
    write_local(LOCAL_RUNS / period, period, rows)
    spark.stop()


if __name__ == "__main__":
    main()
