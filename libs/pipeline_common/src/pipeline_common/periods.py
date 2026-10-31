"""Period of a run and month partitions, used the same way by ingestion and Spark.

INGEST_PERIOD is a quarter (2024Q1) for the regular run or a month (2024-03) to reprocess only
that month. Bronze, silver and gold all live under year=YYYY/month=MM.
"""

import re

_PERIOD = re.compile(r"^(\d{4})(?:-(\d{2})|-?Q([1-4]))$", re.IGNORECASE)


def months(period: str) -> list[tuple[int, int]]:
    """'2024Q1' → [(2024, 1), (2024, 2), (2024, 3)], '2024-03' → [(2024, 3)]."""
    match = _PERIOD.match(period.strip())
    if not match or (match[2] and not 1 <= int(match[2]) <= 12):
        raise ValueError(f"INGEST_PERIOD must be YYYYQn or YYYY-MM, not {period!r}")
    year = int(match[1])
    if match[2]:
        return [(year, int(match[2]))]
    first = 3 * int(match[3]) - 2
    return [(year, month) for month in range(first, first + 3)]


def partition(year: int, month: int) -> str:
    """Partition of a month, e.g. year=2024/month=03."""
    return f"year={year:04d}/month={month:02d}"
