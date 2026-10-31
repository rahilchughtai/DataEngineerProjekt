"""Job 1, Bronze → Silver: Schema vereinheitlichen, Duplikate entfernen, Datenqualität prüfen.

Gültige Fahrten landen in silver/yellow_tripdata, abgelehnte mit Grund (reject_reason) in der
Quarantäne silver/_quarantine/yellow_tripdata. Jeder Monat wird einzeln gelesen, weil die TLC ihr
Schema über die Jahre ändert (z. B. passenger_count als double oder int64).
"""

import logging

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from common import BRONZE, QUARANTINE, SILVER, month_path, period_months, start, write_month

log = logging.getLogger("bronze_to_silver")

# Silver-Spalte → (Spalte in Bronze, Zieltyp). Datenminimierung: nur, was für Nachfrage-Features
# gebraucht wird; VendorID, store_and_fwd_flag, RatecodeID und Einzelgebühren entfallen.
# Zeitstempel sind lokale NYC-Zeit ohne Zone (TLC-Konvention), daher timestamp_ntz.
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
    """Wählt die Silver-Spalten und castet sie auf feste Typen.

    try_cast macht aus nicht konvertierbaren Werten null (die Regeln lehnen die Zeile dann ab),
    statt den ganzen Monat abzubrechen.
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
    """Name der ersten verletzten Regel oder null für gültige Fahrten (Reihenfolge = Priorität)."""
    start = first_instant(year, month)
    end = first_instant(year + 1, 1) if month == 12 else first_instant(year, month + 1)
    pickup, dropoff = F.col("pickup_datetime"), F.col("dropoff_datetime")
    duration = F.timestamp_diff("SECOND", pickup, dropoff)
    passengers = F.col("passenger_count")
    rules = {
        "pickup_null": pickup.isNull(),
        "dropoff_null": dropoff.isNull(),
        # TLC-Dateien enthalten vereinzelt Fahrten aus anderen Monaten oder Jahren.
        "pickup_outside_month": (pickup < start) | (pickup >= end),
        "negative_duration": duration < 0,
        "duration_over_24h": duration > 24 * 3600,
        "distance_invalid": out_of_range("trip_distance", 0, 200),
        # Negative Beträge sind Stornos oder Korrekturbuchungen, keine Fahrten.
        "amount_invalid": out_of_range("fare_amount", 0, 1e12)
        | out_of_range("total_amount", 0, 1e12),
        # Die TLC-Zonen haben die IDs 1 bis 263, 264/265 stehen für „unbekannt“ / „außerhalb NYC“.
        "location_invalid": out_of_range("pu_location_id", 1, 265)
        | out_of_range("do_location_id", 1, 265),
        # passenger_count fehlt häufig (Eingabe am Taxameter) und darf null bleiben.
        "passenger_count_invalid": (passengers < 0) | (passengers > 9),
    }
    return F.coalesce(*(F.when(rule, F.lit(name)) for name, rule in rules.items()))


def to_silver(raw: DataFrame, year: int, month: int) -> DataFrame:
    """Entfernt exakte Duplikate, vereinheitlicht das Schema, ergänzt reject_reason und Lineage."""
    return (
        harmonize(raw.dropDuplicates())
        .withColumn("reject_reason", reject_reason(year, month))
        .withColumn("_processed_at", F.current_timestamp())
    )


def main() -> None:
    spark = start("bronze_to_silver")
    for year, month in period_months():
        # Lineage: aus welcher Bronze-Datei jede Zeile stammt.
        raw = spark.read.parquet(month_path(BRONZE, year, month)).withColumn(
            "_source_file", F.col("_metadata.file_path")
        )
        silver = to_silver(raw, year, month)
        rejected = F.col("reject_reason").isNotNull()
        write_month(silver.filter(~rejected).drop("reject_reason"), SILVER, year, month)
        write_month(silver.filter(rejected), QUARANTINE, year, month)
        log.info("%04d-%02d: Silver und Quarantäne geschrieben", year, month)
    spark.stop()


if __name__ == "__main__":
    main()
