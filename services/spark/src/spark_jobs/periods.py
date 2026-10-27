"""Auflösung der zu verarbeitenden Monate (einzeln oder Quartal).

Gleiche Formate wie im Ingestion-Service (services/ingestion/src/ingestion/months.py).
Bewusst kopiert statt geteilt: jeder Service ist ein eigenständiges Projekt und Image.
"""

import argparse
import os
import re
from dataclasses import dataclass

_MONTH_RE = re.compile(r"^(\d{4})-(\d{2})$")
_QUARTER_RE = re.compile(r"^(\d{4})-?Q([1-4])$", re.IGNORECASE)


@dataclass(frozen=True, order=True)
class Month:
    year: int
    month: int

    def __post_init__(self) -> None:
        if not 1 <= self.month <= 12:
            raise ValueError(f"Ungültiger Monat: {self.month}")

    def __str__(self) -> str:
        return f"{self.year:04d}-{self.month:02d}"


def parse_month(value: str) -> Month:
    match = _MONTH_RE.match(value.strip())
    if not match:
        raise ValueError(f"Monat muss im Format YYYY-MM angegeben werden, nicht {value!r}")
    return Month(int(match[1]), int(match[2]))


def quarter_months(value: str) -> list[Month]:
    match = _QUARTER_RE.match(value.strip())
    if not match:
        raise ValueError(f"Quartal muss im Format YYYYQn angegeben werden, nicht {value!r}")
    year, quarter = int(match[1]), int(match[2])
    first = 3 * (quarter - 1) + 1
    return [Month(year, m) for m in range(first, first + 3)]


def add_period_args(parser: argparse.ArgumentParser) -> None:
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--quarter", metavar="YYYYQn", help="alle Monate eines Quartals")
    group.add_argument("--month", metavar="YYYY-MM", help="ein einzelner Monat")


def resolve_months(args: argparse.Namespace) -> list[Month]:
    """CLI-Argumente haben Vorrang, sonst INGEST_QUARTER bzw. INGEST_MONTH aus der Umgebung."""
    if args.quarter:
        return quarter_months(args.quarter)
    if args.month:
        return [parse_month(args.month)]
    quarter = os.environ.get("INGEST_QUARTER", "").strip()
    month = os.environ.get("INGEST_MONTH", "").strip()
    if quarter and month:
        raise ValueError("INGEST_QUARTER und INGEST_MONTH sind beide gesetzt, bitte nur eine")
    if quarter:
        return quarter_months(quarter)
    if month:
        return [parse_month(month)]
    raise ValueError("Kein Zeitraum angegeben (--quarter/--month oder INGEST_QUARTER/INGEST_MONTH)")
