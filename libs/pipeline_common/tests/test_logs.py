import logging

import pytest

from pipeline_common.logs import setup_logging


def test_quiet_loggers_only_warn() -> None:
    setup_logging(quiet=("noisy-lib",))
    assert logging.getLogger("noisy-lib").level == logging.WARNING


def test_unknown_log_level_falls_back_to_info(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LOG_LEVEL", "VERBOSE")
    calls: list[dict[str, object]] = []
    monkeypatch.setattr(logging, "basicConfig", lambda **kwargs: calls.append(kwargs))
    setup_logging()
    assert calls[0]["level"] == "INFO"
