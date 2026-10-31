"""Run-ID des Laufs, die run-init in eine Datei im Volume pipeline-run schreibt."""

import re
from datetime import datetime, timezone
from pathlib import Path

from pipeline_common.env import required

RUN_ID_FILE_ENV = "PIPELINE_RUN_ID_FILE"
RUN_ID_PATTERN = re.compile(r"\d{8}T\d{6}Z-[0-9a-f]{6}")
_STARTED_AT_FORMAT = "%Y%m%dT%H%M%SZ"


def started_at(run_id: str) -> datetime:
    """Startzeit des Laufs (UTC), die vorne in der Run-ID steht."""
    timestamp = run_id.split("-", 1)[0]
    return datetime.strptime(timestamp, _STARTED_AT_FORMAT).replace(tzinfo=timezone.utc)


def read_run_id() -> str:
    """Run-ID des Laufs aus der Datei, die run-init schreibt (PIPELINE_RUN_ID_FILE)."""
    path = required(RUN_ID_FILE_ENV)
    try:
        run_id = Path(path).read_text(encoding="utf-8").strip()
    except OSError as err:
        raise ValueError(f"{RUN_ID_FILE_ENV}: {path} nicht lesbar ({err})") from err
    if not RUN_ID_PATTERN.fullmatch(run_id):
        raise ValueError(
            f"Ungültige Run-ID {run_id!r} in {path}, erwartet YYYYMMDDTHHMMSSZ-xxxxxx (hex)"
        )
    return run_id
