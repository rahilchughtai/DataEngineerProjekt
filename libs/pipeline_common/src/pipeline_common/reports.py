"""Aufbau der Run-Reports und Exit-Codes, die alle Schritte der Kette gleich verwenden."""

from collections import Counter
from typing import Any

from pipeline_common.timing import elapsed

EXIT_OK = 0
EXIT_FAILED = 1
EXIT_CONFIG = 2
EXIT_INCOMPLETE = 3

# Status eines Monats im Report. Die Status für fehlende Quelldaten legt jeder Schritt selbst fest.
FAILED = "failed"
PROCESSED = "processed"

# Fehlertexte von Spark enthalten den ganzen Java-Stacktrace; der Report bekommt nur den Anfang.
MAX_ERROR_LENGTH = 1000


def failed_entry(entry: dict[str, Any], err: BaseException) -> dict[str, Any]:
    """Eintrag eines fehlgeschlagenen Monats, mit Fehlertyp, auch wenn die Meldung leer ist."""
    error = f"{type(err).__name__}: {err}"[:MAX_ERROR_LENGTH]
    return entry | {"status": FAILED, "error": error}


def run_report(
    run_id: str,
    dataset: str,
    started: float,
    results: list[dict[str, Any]],
    job: str | None = None,
) -> dict[str, Any]:
    """Report eines Schritts; started ist ein time.monotonic()-Wert vom Beginn des Schritts."""
    report: dict[str, Any] = {"run_id": run_id}
    if job:
        report["job"] = job
    return report | {
        "dataset": dataset,
        "seconds": elapsed(started),
        "counts": dict(Counter(r["status"] for r in results)),
        "months": results,
    }


def exit_code(counts: dict[str, int], incomplete_status: str) -> int:
    """1 bei fehlgeschlagenen Monaten, 3 wenn Quelldaten für einen Monat fehlen, sonst 0."""
    if counts.get(FAILED):
        return EXIT_FAILED
    if counts.get(incomplete_status):
        return EXIT_INCOMPLETE
    return EXIT_OK
