"""Datenqualitätsprüfungen für Bronze → Silver.

Jede Regel beschreibt, wann eine Zeile *abgelehnt* wird. Die erste verletzte Regel wird als
reject_reason gespeichert; abgelehnte Zeilen landen in der Quarantäne, nicht im Papierkorb.
Die Reihenfolge ist zugleich die Priorität. Die Tabelle steht auch in services/spark/README.md.
"""

from dataclasses import dataclass

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from spark_jobs.periods import Month

MAX_DURATION_SECONDS = 24 * 3600
MAX_TRIP_DISTANCE_MILES = 200.0
MAX_PASSENGERS = 9
# Die TLC-Zonenkarte hat die IDs 1 bis 263; 264 und 265 stehen für „unbekannt“ bzw. „außerhalb NYC“.
LOCATION_ID_RANGE = (1, 265)


@dataclass(frozen=True)
class Rule:
    name: str
    violated: Column


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


def rules(month: Month) -> list[Rule]:
    start, end = _month_bounds(month)
    pickup = F.col("pickup_datetime")
    low_id, high_id = LOCATION_ID_RANGE
    return [
        Rule("pickup_null", pickup.isNull()),
        Rule("dropoff_null", F.col("dropoff_datetime").isNull()),
        # TLC-Dateien enthalten vereinzelt Fahrten aus anderen Monaten oder Jahren.
        Rule("pickup_outside_month", (pickup < start) | (pickup >= end)),
        Rule("negative_duration", _duration_seconds() < 0),
        Rule("duration_over_24h", _duration_seconds() > MAX_DURATION_SECONDS),
        Rule("distance_invalid", _out_of_range("trip_distance", 0, MAX_TRIP_DISTANCE_MILES)),
        # Negative Beträge sind Stornos oder Korrekturbuchungen, keine Fahrten.
        Rule(
            "amount_invalid",
            F.col("fare_amount").isNull()
            | F.col("total_amount").isNull()
            | (F.col("fare_amount") < 0)
            | (F.col("total_amount") < 0),
        ),
        Rule(
            "location_invalid",
            _out_of_range("pu_location_id", low_id, high_id)
            | _out_of_range("do_location_id", low_id, high_id),
        ),
        # passenger_count fehlt in der Quelle häufig (Eingabe am Taxameter) und bleibt dann null.
        Rule("passenger_count_invalid", F.col("passenger_count") > MAX_PASSENGERS),
    ]


# Feste Liste für Reports ohne aktive SparkSession; ein Test hält sie synchron zu rules().
RULE_NAMES = (
    "pickup_null",
    "dropoff_null",
    "pickup_outside_month",
    "negative_duration",
    "duration_over_24h",
    "distance_invalid",
    "amount_invalid",
    "location_invalid",
    "passenger_count_invalid",
)


def classify(df: DataFrame, month: Month) -> DataFrame:
    """Ergänzt reject_reason: Name der ersten verletzten Regel oder null für gültige Zeilen."""
    reason = F.coalesce(*(F.when(rule.violated, F.lit(rule.name)) for rule in rules(month)))
    return df.withColumn("reject_reason", reason)
