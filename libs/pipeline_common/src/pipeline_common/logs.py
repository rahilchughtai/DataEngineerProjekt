"""Logging-Format aller Services."""

import logging
import os
import sys
from collections.abc import Iterable

_LEVELS = ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")


def setup_logging(quiet: Iterable[str] = ()) -> None:
    """Loggt nach stdout; die Logger in quiet erst ab WARNING.

    Ein unbekanntes LOG_LEVEL fällt auf INFO zurück, statt den Start mit Traceback abzubrechen.
    """
    requested = os.environ.get("LOG_LEVEL", "INFO").strip().upper()
    level = requested if requested in _LEVELS else "INFO"
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
        stream=sys.stdout,
    )
    for name in quiet:
        logging.getLogger(name).setLevel(logging.WARNING)
    if level != requested:
        logging.getLogger(__name__).warning(
            "LOG_LEVEL=%r unbekannt, verwende INFO (erlaubt: %s)", requested, ", ".join(_LEVELS)
        )
