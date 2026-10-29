"""Gemeinsamer Rahmen der Spark-Jobs: Run-ID, Logging und Exit-Code aus dem Run-Report."""

import logging
import os
import re
import sys
from pathlib import Path
from typing import Any

log = logging.getLogger("runtime")


RUN_ID_FILE_ENV = "PIPELINE_RUN_ID_FILE"
RUN_ID_PATTERN = re.compile(r"\d{8}T\d{6}Z-[0-9a-f]{6}")


def read_run_id() -> str:
    """Run-ID des Laufs aus der Datei, die run-init schreibt (PIPELINE_RUN_ID_FILE)."""
    path = os.environ.get(RUN_ID_FILE_ENV, "").strip()
    if not path:
        raise ValueError(f"Umgebungsvariable {RUN_ID_FILE_ENV} fehlt (Run-ID von run-init)")
    try:
        run_id = Path(path).read_text(encoding="utf-8").strip()
    except OSError as err:
        raise ValueError(f"{RUN_ID_FILE_ENV}: {path} nicht lesbar ({err})") from err
    if not RUN_ID_PATTERN.fullmatch(run_id):
        raise ValueError(
            f"Ungültige Run-ID {run_id!r} in {path}, erwartet YYYYMMDDTHHMMSSZ-xxxxxx (hex)"
        )
    return run_id


def setup_logging() -> None:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
        stream=sys.stdout,
    )
    logging.getLogger("py4j").setLevel(logging.WARNING)


def exit_code(report: dict[str, Any], source_layer: str) -> int:
    """1 bei fehlgeschlagenen Monaten, 3 wenn Quelldaten für einen Monat fehlen, sonst 0."""
    counts = report["counts"]
    if counts.get("failed"):
        return 1
    if counts.get("missing") or not counts.get("processed"):
        log.error("%s-Daten fehlen, Zeitraum unvollständig: counts=%s", source_layer, counts)
        return 3
    return 0
