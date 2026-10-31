"""Laufbericht: zählt nach Gold je Monat die Zeilen in allen Schichten.

Der Bericht entsteht aus den Daten selbst, nicht aus Angaben der einzelnen Schritte, und liegt als
JSON unter gold/_reports/<INGEST_PERIOD>/. Es muss gelten:
bronze_rows = silver_rows + quarantined_rows + duplicates und gold_trips = silver_rows.
"""

import logging
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from common import BRONZE, GOLD, QUARANTINE, REPORTS, SILVER, month_path, period_months, start

log = logging.getLogger("report")

SCHEMA = (
    "month string, bronze_rows long, silver_rows long, quarantined_rows long, duplicates long, "
    "rejected_by_reason map<string, long>, gold_rows long, gold_trips long"
)


def month_report(spark: SparkSession, year: int, month: int) -> dict:
    def read(table: str):
        return spark.read.parquet(month_path(table, year, month))

    bronze_rows = read(BRONZE).count()
    silver_rows = read(SILVER).count()
    by_reason = read(QUARANTINE).groupBy("reject_reason").count().collect()
    rejected = {row["reject_reason"]: row["count"] for row in by_reason}
    gold = read(GOLD).agg(F.count("*").alias("rows"), F.sum("trip_count").alias("trips")).first()
    quarantined_rows = sum(rejected.values())
    return {
        "month": f"{year:04d}-{month:02d}",
        "bronze_rows": bronze_rows,
        "silver_rows": silver_rows,
        "quarantined_rows": quarantined_rows,
        "duplicates": bronze_rows - silver_rows - quarantined_rows,
        "rejected_by_reason": rejected,
        "gold_rows": gold["rows"],
        "gold_trips": gold["trips"] or 0,
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
