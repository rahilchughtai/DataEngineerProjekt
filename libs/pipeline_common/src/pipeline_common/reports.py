"""Aufbau der Run-Reports und Exit-Codes, die alle Schritte der Kette gleich verwenden."""

import time
from collections import Counter
from collections.abc import Iterable
from typing import Any


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
        "seconds": round(time.monotonic() - started, 2),
        "counts": count_statuses(results),
        "months": results,
    }


def exit_code(counts: dict[str, int], incomplete_status: str) -> int:
    """1 bei fehlgeschlagenen Monaten, 3 wenn Quelldaten für einen Monat fehlen, sonst 0."""
    if counts.get("failed"):
        return 1
    if counts.get(incomplete_status):
        return 3
    return 0
