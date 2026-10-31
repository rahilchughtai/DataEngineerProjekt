"""Zeitstempel und Laufzeiten in der Form, die alle Reports verwenden."""

import time
from datetime import datetime, timezone


def now_utc() -> datetime:
    """Aktuelle Zeit in UTC auf ganze Sekunden, wie sie in Lineage-Spalten und Reports steht."""
    return datetime.now(timezone.utc).replace(microsecond=0)


def elapsed(started: float) -> float:
    """Sekunden seit started, einem time.monotonic()-Wert, auf zwei Stellen gerundet."""
    return round(time.monotonic() - started, 2)
