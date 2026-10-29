"""Exit-Code der Spark-Jobs aus dem Run-Report."""

import logging
from typing import Any

from pipeline_common.reports import exit_code as common_exit_code

log = logging.getLogger("runtime")


def exit_code(report: dict[str, Any], source_layer: str) -> int:
    """1 bei fehlgeschlagenen Monaten, 3 wenn Quelldaten für einen Monat fehlen, sonst 0."""
    counts = report["counts"]
    code = common_exit_code(counts, "missing")
    if code == 0 and not counts.get("processed"):
        code = 3
    if code == 3:
        log.error("%s-Daten fehlen, Zeitraum unvollständig: counts=%s", source_layer, counts)
    return code
