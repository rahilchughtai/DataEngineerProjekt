"""Auflösung der zu verarbeitenden Monate (einzeln oder Quartal) aus INGEST_PERIOD."""

import re
from dataclasses import dataclass

from pipeline_common.env import required

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

    @property
    def partition_values(self) -> dict[str, str]:
        """Werte der Partitionsspalten year und month, zweistellig wie in den Pfaden."""
        return {"year": f"{self.year:04d}", "month": f"{self.month:02d}"}

    @property
    def partition(self) -> str:
        """Hive-Partition des Monats, wie sie alle Schichten im Lake verwenden."""
        return "/".join(f"{name}={value}" for name, value in self.partition_values.items())

    def next(self) -> "Month":
        if self.month == 12:
            return Month(self.year + 1, 1)
        return Month(self.year, self.month + 1)


def parse_period(value: str) -> list[Month]:
    """Ein Quartal (YYYYQn) oder ein einzelner Monat (YYYY-MM)."""
    if match := _QUARTER_RE.match(value.strip()):
        first = 3 * (int(match[2]) - 1) + 1
        return [Month(int(match[1]), m) for m in range(first, first + 3)]
    if match := _MONTH_RE.match(value.strip()):
        return [Month(int(match[1]), int(match[2]))]
    raise ValueError(f"Zeitraum muss YYYYQn oder YYYY-MM sein, nicht {value!r}")


def period_from_env() -> str:
    """Zeitraum des Laufs aus INGEST_PERIOD, wie ihn Compose aus .env setzt."""
    return required("INGEST_PERIOD")


def months_from_env() -> list[Month]:
    return parse_period(period_from_env())
