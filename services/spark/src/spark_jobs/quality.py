"""Datenqualitätsprüfungen für Bronze → Silver.

Jede Regel beschreibt, wann eine Zeile *abgelehnt* wird. Die erste verletzte Regel wird als
reject_reason gespeichert; abgelehnte Zeilen landen in der Quarantäne, nicht im Papierkorb.
Die Reihenfolge ist zugleich die Priorität. Die Tabelle steht auch in services/spark/README.md.
"""

from collections.abc import Callable

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from pipeline_common.periods import Month
from spark_jobs.schema import LOCATION_ID_RANGE, duration_seconds

MAX_DURATION_SECONDS = 24 * 3600
MAX_TRIP_DISTANCE_MILES = 200.0
MAX_PASSENGERS = 9


def month_bounds(month: Month) -> tuple[Column, Column]:
    def first_instant(m: Month) -> Column:
        return F.lit(f"{m}-01 00:00:00").cast("timestamp_ntz")

    return first_instant(month), first_instant(month.next())


def _out_of_range(name: str, low: float, high: float) -> Column:
    column = F.col(name)
    return column.isNull() | (column < low) | (column > high)


def _pickup_outside_month(month: Month) -> Column:
    start, end = month_bounds(month)
    pickup = F.col("pickup_datetime")
    return (pickup < start) | (pickup >= end)


def _invalid_amount(name: str) -> Column:
    column = F.col(name)
    return column.isNull() | F.isnan(name) | (column < 0) | (column == F.lit(float("inf")))


def _amount_invalid(_: Month) -> Column:
    return _invalid_amount("fare_amount") | _invalid_amount("total_amount")


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
    "negative_duration": lambda _: duration_seconds() < 0,
    "duration_over_24h": lambda _: duration_seconds() > MAX_DURATION_SECONDS,
    "distance_invalid": lambda _: (
        _out_of_range("trip_distance", 0, MAX_TRIP_DISTANCE_MILES) | F.isnan("trip_distance")
    ),
    # Negative Beträge sind Stornos oder Korrekturbuchungen, keine Fahrten.
    "amount_invalid": _amount_invalid,
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
