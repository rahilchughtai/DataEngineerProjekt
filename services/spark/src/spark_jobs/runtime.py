"""Gemeinsamer Rahmen der Spark-Jobs: Run-ID, Logging und Exit-Code aus dem Run-Report."""

import logging
import os
import re
import secrets
import sys
from datetime import datetime, timezone
from typing import Any

log = logging.getLogger("runtime")


RUN_ID_ENV = "PIPELINE_RUN_ID"
RUN_ID_PATTERN = re.compile(r"\d{8}T\d{6}Z-[0-9a-f]{6}")


def new_run_id() -> str:
    """Nach Startzeit sortierbar; der Zufallsteil trennt Läufe, die in derselben Sekunde starten."""
    return f"{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}-{secrets.token_hex(3)}"


def resolve_run_id() -> str:
    """Run-ID des Orchestrators aus PIPELINE_RUN_ID, bei Einzelaufruf eine neue."""
    run_id = os.environ.get(RUN_ID_ENV, "").strip()
    if not run_id:
        return new_run_id()
    if not RUN_ID_PATTERN.fullmatch(run_id):
        raise ValueError(
            f"Ungültige {RUN_ID_ENV} {run_id!r}, erwartet YYYYMMDDTHHMMSSZ-xxxxxx (hex)"
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
