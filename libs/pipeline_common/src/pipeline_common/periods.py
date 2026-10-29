"""Auflösung der zu verarbeitenden Monate (einzeln oder Quartal) aus INGEST_PERIOD."""

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


def parse_period(value: str) -> list[Month]:
    """Ein Quartal (YYYYQn) oder ein einzelner Monat (YYYY-MM)."""
    if _QUARTER_RE.match(value.strip()):
        return quarter_months(value)
    if _MONTH_RE.match(value.strip()):
        return [parse_month(value)]
    raise ValueError(f"Zeitraum muss YYYYQn oder YYYY-MM sein, nicht {value!r}")


def period_from_env() -> str:
    """Zeitraum des Laufs aus INGEST_PERIOD, wie ihn Compose aus .env setzt."""
    period = os.environ.get("INGEST_PERIOD", "").strip()
    if not period:
        raise ValueError("Kein Zeitraum angegeben (INGEST_PERIOD, siehe .env.example)")
    return period


def months_from_env() -> list[Month]:
    return parse_period(period_from_env())
