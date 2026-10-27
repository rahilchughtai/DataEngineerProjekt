"""Datenqualitätsprüfungen für Bronze → Silver.

Jede Regel beschreibt, wann eine Zeile *abgelehnt* wird. Die erste verletzte Regel wird als
reject_reason gespeichert; abgelehnte Zeilen landen in der Quarantäne, nicht im Papierkorb.
Die Reihenfolge ist zugleich die Priorität. Die Tabelle steht auch in services/spark/README.md.
"""

from collections.abc import Callable

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from spark_jobs.periods import Month

MAX_DURATION_SECONDS = 24 * 3600
MAX_TRIP_DISTANCE_MILES = 200.0
MAX_PASSENGERS = 9
# Die TLC-Zonenkarte hat die IDs 1 bis 263; 264 und 265 stehen für „unbekannt“ bzw. „außerhalb NYC“.
LOCATION_ID_RANGE = (1, 265)


def _month_bounds(month: Month) -> tuple[Column, Column]:
    start = f"{month.year:04d}-{month.month:02d}-01 00:00:00"
    nxt = (
        f"{month.year + 1:04d}-01-01 00:00:00"
        if month.month == 12
        else f"{month.year:04d}-{month.month + 1:02d}-01 00:00:00"
    )
    return F.lit(start).cast("timestamp_ntz"), F.lit(nxt).cast("timestamp_ntz")


def _duration_seconds() -> Column:
    return F.timestamp_diff("SECOND", F.col("pickup_datetime"), F.col("dropoff_datetime"))


def _out_of_range(name: str, low: float, high: float) -> Column:
    column = F.col(name)
    return column.isNull() | (column < low) | (column > high)


def _pickup_outside_month(month: Month) -> Column:
    start, end = _month_bounds(month)
    pickup = F.col("pickup_datetime")
    return (pickup < start) | (pickup >= end)


def _location_invalid(_: Month) -> Column:
    low_id, high_id = LOCATION_ID_RANGE
    return _out_of_range("pu_location_id", low_id, high_id) | _out_of_range(
        "do_location_id", low_id, high_id
    )


# Regelname → Bedingung für den Monat. Spalten entstehen erst beim Aufruf, nicht beim Import.
RULES: dict[str, Callable[[Month], Column]] = {
    "pickup_null": lambda _: F.col("pickup_datetime").isNull(),
    "dropoff_null": lambda _: F.col("dropoff_datetime").isNull(),
    # TLC-Dateien enthalten vereinzelt Fahrten aus anderen Monaten oder Jahren.
    "pickup_outside_month": _pickup_outside_month,
    "negative_duration": lambda _: _duration_seconds() < 0,
    "duration_over_24h": lambda _: _duration_seconds() > MAX_DURATION_SECONDS,
    "distance_invalid": lambda _: _out_of_range("trip_distance", 0, MAX_TRIP_DISTANCE_MILES),
    # Negative Beträge sind Stornos oder Korrekturbuchungen, keine Fahrten.
    # Spark ordnet NaN über jede Zahl, `< 0` greift also nicht.
    "amount_invalid": lambda _: (
        F.col("fare_amount").isNull()
        | F.col("total_amount").isNull()
        | F.isnan("fare_amount")
        | F.isnan("total_amount")
        | (F.col("fare_amount") < 0)
        | (F.col("total_amount") < 0)
    ),
    "location_invalid": _location_invalid,
    # passenger_count fehlt in der Quelle häufig (Eingabe am Taxameter) und bleibt dann null.
    "passenger_count_invalid": lambda _: (
        (F.col("passenger_count") < 0) | (F.col("passenger_count") > MAX_PASSENGERS)
    ),
}
RULE_NAMES = tuple(RULES)


def classify(df: DataFrame, month: Month) -> DataFrame:
    """Ergänzt reject_reason: Name der ersten verletzten Regel oder null für gültige Zeilen."""
    reason = F.coalesce(*(F.when(violated(month), F.lit(name)) for name, violated in RULES.items()))
    return df.withColumn("reject_reason", reason)
