"""Auflösung der zu ladenden Monate (einzeln, Bereich oder Quartal)."""

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

    def next(self) -> "Month":
        if self.month == 12:
            return Month(self.year + 1, 1)
        return Month(self.year, self.month + 1)


def parse_month(value: str) -> Month:
    match = _MONTH_RE.match(value.strip())
    if not match:
        raise ValueError(f"Monat muss im Format YYYY-MM angegeben werden, nicht {value!r}")
    return Month(int(match[1]), int(match[2]))


def month_range(start: Month, end: Month) -> list[Month]:
    if end < start:
        raise ValueError(f"Ende {end} liegt vor Start {start}")
    months = [start]
    while months[-1] < end:
        months.append(months[-1].next())
    return months


def quarter_months(value: str) -> list[Month]:
    match = _QUARTER_RE.match(value.strip())
    if not match:
        raise ValueError(f"Quartal muss im Format YYYYQn angegeben werden, nicht {value!r}")
    year, quarter = int(match[1]), int(match[2])
    first = Month(year, 3 * (quarter - 1) + 1)
    return month_range(first, Month(year, first.month + 2))
