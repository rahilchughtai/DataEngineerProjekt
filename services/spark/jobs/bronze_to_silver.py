"""Job 1, bronze → silver: harmonize the schema, remove duplicates, check data quality.

Valid trips go to silver/yellow_tripdata, rejected ones with a reason (reject_reason) to the
quarantine silver/_quarantine/yellow_tripdata. Each month is read separately because the TLC
changes its schema over the years (e.g. passenger_count as double or int64).
"""

import logging

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from common import BRONZE, QUARANTINE, SILVER, month_path, period_months, start, write_month

log = logging.getLogger("bronze_to_silver")

# Silver column → (column in bronze, target type). Data minimization: only what demand features
# need; VendorID, store_and_fwd_flag, RatecodeID and individual fees are dropped.
# Timestamps are local NYC time without a zone (TLC convention), hence timestamp_ntz.
SILVER_COLUMNS = {
    "pickup_datetime": ("tpep_pickup_datetime", "timestamp_ntz"),
    "dropoff_datetime": ("tpep_dropoff_datetime", "timestamp_ntz"),
    "pu_location_id": ("PULocationID", "int"),
    "do_location_id": ("DOLocationID", "int"),
    "passenger_count": ("passenger_count", "int"),
    "trip_distance": ("trip_distance", "double"),
    "fare_amount": ("fare_amount", "double"),
    "total_amount": ("total_amount", "double"),
    "payment_type": ("payment_type", "int"),
}


def harmonize(raw: DataFrame) -> DataFrame:
    """Selects the silver columns and casts them to fixed types.

    try_cast turns unconvertible values into null (the rules then reject the row) instead of
    failing the whole month.
    """
    columns = [
        F.col(src).try_cast(dtype).alias(name) for name, (src, dtype) in SILVER_COLUMNS.items()
    ]
    return raw.select(*columns, F.col("_source_file"))


def out_of_range(name: str, low: float, high: float) -> Column:
    column = F.col(name)
    return column.isNull() | F.isnan(column) | (column < low) | (column > high)


def first_instant(year: int, month: int) -> Column:
    return F.lit(f"{year:04d}-{month:02d}-01 00:00:00").cast("timestamp_ntz")


def reject_reason(year: int, month: int) -> Column:
    """Name of the first violated rule, or null for valid trips (order = priority)."""
    start = first_instant(year, month)
    end = first_instant(year + 1, 1) if month == 12 else first_instant(year, month + 1)
    pickup, dropoff = F.col("pickup_datetime"), F.col("dropoff_datetime")
    duration = F.timestamp_diff("SECOND", pickup, dropoff)
    passengers = F.col("passenger_count")
    rules = {
        "pickup_null": pickup.isNull(),
        "dropoff_null": dropoff.isNull(),
        # TLC files occasionally contain trips from other months or years.
        "pickup_outside_month": (pickup < start) | (pickup >= end),
        "negative_duration": duration < 0,
        "duration_over_24h": duration > 24 * 3600,
        "distance_invalid": out_of_range("trip_distance", 0, 200),
        # Negative amounts are cancellations or corrections, not trips.
        "amount_invalid": out_of_range("fare_amount", 0, 1e12)
        | out_of_range("total_amount", 0, 1e12),
        # TLC zones have IDs 1 to 263; 264/265 stand for "unknown" / "outside NYC".
        "location_invalid": out_of_range("pu_location_id", 1, 265)
        | out_of_range("do_location_id", 1, 265),
        # passenger_count is often missing (entered at the meter) and may stay null.
        "passenger_count_invalid": (passengers < 0) | (passengers > 9),
    }
    return F.coalesce(*(F.when(rule, F.lit(name)) for name, rule in rules.items()))


def to_silver(raw: DataFrame, year: int, month: int) -> DataFrame:
    """Removes exact duplicates, harmonizes the schema, adds reject_reason and lineage."""
    return (
        harmonize(raw.dropDuplicates())
        .withColumn("reject_reason", reject_reason(year, month))
        .withColumn("_processed_at", F.current_timestamp())
    )


def main() -> None:
    spark = start("bronze_to_silver")
    for year, month in period_months():
        # Lineage: which bronze file each row comes from.
        raw = spark.read.parquet(month_path(BRONZE, year, month)).withColumn(
            "_source_file", F.col("_metadata.file_path")
        )
        silver = to_silver(raw, year, month)
        rejected = F.col("reject_reason").isNotNull()
        write_month(silver.filter(~rejected).drop("reject_reason"), SILVER, year, month)
        write_month(silver.filter(rejected), QUARANTINE, year, month)
        log.info("%04d-%02d: silver and quarantine written", year, month)
    spark.stop()


if __name__ == "__main__":
    main()
