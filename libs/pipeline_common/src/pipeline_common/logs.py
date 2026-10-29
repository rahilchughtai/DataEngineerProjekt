"""Logging-Format aller Services."""

import logging
import os
import sys
from collections.abc import Iterable


def setup_logging(quiet: Iterable[str] = ()) -> None:
    """Loggt nach stdout; die Logger in quiet erst ab WARNING."""
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
        stream=sys.stdout,
    )
    for name in quiet:
        logging.getLogger(name).setLevel(logging.WARNING)
