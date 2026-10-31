"""Zeitraum eines Laufs und Monatspartitionen: das Einzige, was Ingestion und Spark teilen.

INGEST_PERIOD ist ein Quartal (2024Q1) für den regulären Lauf oder ein Monat (2024-03), um nur
diesen neu zu verarbeiten. Bronze, Silver und Gold liegen alle unter year=YYYY/month=MM.
"""

import re

_PERIOD = re.compile(r"^(\d{4})(?:-(\d{2})|-?Q([1-4]))$", re.IGNORECASE)


def months(period: str) -> list[tuple[int, int]]:
    """'2024Q1' → [(2024, 1), (2024, 2), (2024, 3)], '2024-03' → [(2024, 3)]."""
    match = _PERIOD.match(period.strip())
    if not match or (match[2] and not 1 <= int(match[2]) <= 12):
        raise ValueError(f"INGEST_PERIOD muss YYYYQn oder YYYY-MM sein, nicht {period!r}")
    year = int(match[1])
    if match[2]:
        return [(year, int(match[2]))]
    first = 3 * int(match[3]) - 2
    return [(year, month) for month in range(first, first + 3)]


def partition(year: int, month: int) -> str:
    """Partition eines Monats, z. B. year=2024/month=03."""
    return f"year={year:04d}/month={month:02d}"
