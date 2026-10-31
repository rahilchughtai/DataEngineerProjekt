"""Zielschema der Silver-Schicht und Harmonisierung der Bronze-Dateien.

Die TLC-Dateien ändern ihr Schema über die Jahre (z. B. passenger_count als double oder int64,
Airport_fee/airport_fee). Jeder Monat wird deshalb einzeln gelesen und auf ein festes Schema
gecastet. Datenminimierung: Es werden nur die Spalten übernommen, die für Nachfrage-Features
gebraucht werden. VendorID, store_and_fwd_flag, RatecodeID und die Einzelgebühren entfallen.
"""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DataType, DoubleType, IntegerType, TimestampNTZType

from pipeline_common.periods import Month

# Silver-Spalte → (Spalte in Bronze, Zieltyp).
# Spark löst Spaltennamen ohne Unterscheidung von Groß- und Kleinschreibung auf.
# Zeitstempel sind lokale NYC-Zeit ohne Zone (TLC-Konvention) und bleiben deshalb TIMESTAMP_NTZ.
SILVER_COLUMNS: dict[str, tuple[str, DataType]] = {
    "pickup_datetime": ("tpep_pickup_datetime", TimestampNTZType()),
    "dropoff_datetime": ("tpep_dropoff_datetime", TimestampNTZType()),
    "pu_location_id": ("PULocationID", IntegerType()),
    "do_location_id": ("DOLocationID", IntegerType()),
    "passenger_count": ("passenger_count", IntegerType()),
    "trip_distance": ("trip_distance", DoubleType()),
    "fare_amount": ("fare_amount", DoubleType()),
    "total_amount": ("total_amount", DoubleType()),
    "payment_type": ("payment_type", IntegerType()),
}
BUSINESS_COLUMNS = list(SILVER_COLUMNS)

# Die TLC-Zonenkarte hat die IDs 1 bis 263; 264 und 265 stehen für „unbekannt“ bzw. „außerhalb NYC“.
LOCATION_ID_RANGE = (1, 265)


class SchemaError(ValueError):
    """Eine Bronze-Datei enthält nicht alle benötigten Spalten."""


def harmonize(raw: DataFrame) -> DataFrame:
    available = {name.lower() for name in raw.columns}
    missing = [src for src, _ in SILVER_COLUMNS.values() if src.lower() not in available]
    if missing:
        raise SchemaError(f"Bronze-Datei ohne Spalten {', '.join(missing)}")
    # try_cast: unter ANSI (Standard in Spark 4) bricht ein cast bei einem Wert den ganzen Monat ab.
    columns: list[Column] = [
        F.col(src).try_cast(dtype).alias(name) for name, (src, dtype) in SILVER_COLUMNS.items()
    ]
    # Lineage: aus welcher Bronze-Datei die Zeile stammt (von der Lesestelle gesetzt).
    source_file = F.col("_source_file") if "_source_file" in raw.columns else F.lit(None)
    columns.append(source_file.cast("string").alias("_source_file"))
    return raw.select(*columns)


def month_bounds(month: Month) -> tuple[Column, Column]:
    """Erster Zeitpunkt des Monats und des Folgemonats, als TIMESTAMP_NTZ wie in Silver."""

    def first_instant(m: Month) -> Column:
        return F.lit(f"{m}-01 00:00:00").cast("timestamp_ntz")

    return first_instant(month), first_instant(month.next())


def duration_seconds() -> Column:
    """Fahrtdauer einer Silver-Zeile in Sekunden."""
    return F.timestamp_diff("SECOND", F.col("pickup_datetime"), F.col("dropoff_datetime"))
