"""Aufbau der Run-Reports und Exit-Codes, die alle Schritte der Kette gleich verwenden."""

from collections import Counter
from collections.abc import Iterable
from typing import Any

from pipeline_common.timing import elapsed

EXIT_OK = 0
EXIT_FAILED = 1
EXIT_CONFIG = 2
EXIT_INCOMPLETE = 3


def runs_key(dataset: str, run_id: str) -> str:
    """Key des Reports relativ zum Bucket; jeder Schritt legt ihn in seiner Schicht ab."""
    return f"_runs/{dataset}/{run_id}.json"


def count_statuses(results: Iterable[dict[str, Any]]) -> dict[str, int]:
    return dict(Counter(r["status"] for r in results))


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
        "counts": count_statuses(results),
        "months": results,
    }


def exit_code(counts: dict[str, int], incomplete_status: str) -> int:
    """1 bei fehlgeschlagenen Monaten, 3 wenn Quelldaten für einen Monat fehlen, sonst 0."""
    if counts.get("failed"):
        return EXIT_FAILED
    if counts.get(incomplete_status):
        return EXIT_INCOMPLETE
    return EXIT_OK
