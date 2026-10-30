import logging

from pipeline_common.logs import setup_logging


def test_quiet_loggers_only_warn() -> None:
    setup_logging(quiet=("noisy-lib",))
    assert logging.getLogger("noisy-lib").level == logging.WARNING
