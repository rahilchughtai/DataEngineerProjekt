"""Shared log format for all services, so the logs of the chain look the same."""

import logging


def setup_logging(*quiet: str) -> None:
    """Logs from INFO; the loggers in quiet (e.g. libraries) only from WARNING."""
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    for name in quiet:
        logging.getLogger(name).setLevel(logging.WARNING)
