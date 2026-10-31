"""Einheitliches Log-Format aller Services, damit die Logs der Kette gleich aussehen."""

import logging


def setup_logging(*quiet: str) -> None:
    """Loggt ab INFO; die Logger in quiet (z. B. Bibliotheken) erst ab WARNING."""
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    for name in quiet:
        logging.getLogger(name).setLevel(logging.WARNING)
