"""Run report: after gold, counts the rows of each month in all layers and records when.

The report is derived from the data itself, not from what the individual steps report, and is
stored as JSON under gold/_reports/<INGEST_PERIOD>/. It must hold that
bronze_rows = silver_rows + quarantined_rows + duplicates and gold_trips = silver_rows.
The timestamps bronze_loaded_at ≤ silver_processed_at ≤ gold_processed_at show the latency of
each stage: from storing the source file to finished features.
"""

import logging
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from common import BRONZE, GOLD, QUARANTINE, REPORTS, SILVER, month_path, period_months, start

log = logging.getLogger("report")

SCHEMA = (
    "month string, bronze_rows long, silver_rows long, quarantined_rows long, duplicates long, "
    "rejected_by_reason map<string, long>, gold_rows long, gold_trips long, "
    "bronze_loaded_at timestamp, silver_processed_at timestamp, gold_processed_at timestamp"
)


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
    }


def main() -> None:
    spark = start("report")
    rows = [month_report(spark, year, month) for year, month in period_months()]
    for row in rows:
        log.info("%s", row)
    period = os.environ["INGEST_PERIOD"].strip()
    spark.createDataFrame(rows, SCHEMA).coalesce(1).write.mode("overwrite").json(
        f"{REPORTS}/{period}"
    )
    spark.stop()


if __name__ == "__main__":
    main()
