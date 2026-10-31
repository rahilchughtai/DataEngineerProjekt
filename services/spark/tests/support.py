"""Gemeinsame Konstanten und Helfer der Spark-Tests, die keine Fixtures sind."""

from pathlib import Path

from pipeline_common.periods import Month

JANUARY = Month(2024, 1)
FEBRUARY = Month(2024, 2)
# Run-IDs im Format, das run-init erzeugt (Startzeit UTC plus 6 Hex-Zeichen).
RUN_ID = "20240401T000000Z-abc123"
RERUN_ID = "20240402T000000Z-def456"


def local(uri: str) -> Path:
    """Lokaler Pfad zu einer file://-URI aus LakePaths."""
    return Path(uri.removeprefix("file://"))
