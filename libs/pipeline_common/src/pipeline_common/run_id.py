"""Run-ID des Laufs, die run-init in eine Datei im Volume pipeline-run schreibt."""

import os
import re
from pathlib import Path

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
