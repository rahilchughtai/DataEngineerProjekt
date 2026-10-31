"""Job 2, silver → gold: hourly demand per pickup zone as features for the ML application."""

import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from common import GOLD, SILVER, month_path, period_months, start, write_month

log = logging.getLogger("silver_to_gold")


def hourly_demand(silver: DataFrame) -> DataFrame:
    """One row per pickup zone and hour with trips: count, averages, calendar features."""
    duration_minutes = F.timestamp_diff("SECOND", "pickup_datetime", "dropoff_datetime") / 60
    return (
        silver.groupBy(
            F.date_trunc("hour", "pickup_datetime").alias("pickup_hour"), "pu_location_id"
        )
        .agg(
            F.count("*").alias("trip_count"),
            F.avg("trip_distance").alias("avg_trip_distance"),
            F.avg(duration_minutes).alias("avg_duration_minutes"),
            F.avg("fare_amount").alias("avg_fare_amount"),
            F.avg("total_amount").alias("avg_total_amount"),
        )
        .withColumn("hour_of_day", F.hour("pickup_hour"))
        # weekday counts from 0 = Monday; so day_of_week is 1 = Monday to 7 = Sunday.
        .withColumn("day_of_week", F.weekday("pickup_hour") + 1)
        .withColumn("is_weekend", F.col("day_of_week") >= 6)
        .withColumn("_processed_at", F.current_timestamp())
    )


def main() -> None:
    spark = start("silver_to_gold")
    for year, month in period_months():
        silver = spark.read.parquet(month_path(SILVER, year, month))
        write_month(hourly_demand(silver), GOLD, year, month)
        log.info("%04d-%02d: gold written", year, month)
    spark.stop()


if __name__ == "__main__":
    main()
