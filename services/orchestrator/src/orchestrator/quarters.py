"""Quartale im Format YYYYQn: Prüfung der Eingabe und neuestes veröffentlichtes Quartal."""

import re
from datetime import date

# Die TLC veröffentlicht einen Monat etwa zwei Monate nach seinem Ende.
PUBLICATION_LAG_MONTHS = 2

# Gleiche Schreibweisen wie in Ingestion und Spark (z. B. 2024Q1, 2024q1, 2024-Q1).
_QUARTER_RE = re.compile(r"^(\d{4})-?Q([1-4])$", re.IGNORECASE)


def parse_quarter(value: str) -> str:
    """Prüft ein Quartal und normalisiert es auf YYYYQn."""
    match = _QUARTER_RE.match(value.strip())
    if not match:
        raise ValueError(f"Quartal muss im Format YYYYQn angegeben werden, nicht {value!r}")
    return f"{match[1]}Q{match[2]}"


def latest_published_quarter(today: date, lag_months: int = PUBLICATION_LAG_MONTHS) -> str:
    """Das letzte Quartal, dessen drei Monate nach der Veröffentlichungsverzögerung vorliegen.

    Ein Monat gilt als veröffentlicht, wenn seit seinem Ende lag_months volle Monate
    vergangen sind; der laufende Monat ist noch nicht vorbei und zählt nicht mit.
    """
    year, month_index = divmod(today.year * 12 + today.month - 1 - (lag_months + 1), 12)
    complete = (month_index + 1) // 3
    if complete == 0:
        return f"{year - 1}Q4"
    return f"{year}Q{complete}"
