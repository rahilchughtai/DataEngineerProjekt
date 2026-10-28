import argparse

import pytest

from ingestion.cli import exit_code, resolve_months
from ingestion.months import Month


def _args(quarter: str | None = None, month: str | None = None) -> argparse.Namespace:
    return argparse.Namespace(quarter=quarter, month=month, force=False)


@pytest.fixture(autouse=True)
def clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("INGEST_PERIOD", raising=False)


def test_cli_argument_beats_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("INGEST_PERIOD", "2023Q1")
    assert resolve_months(_args(month="2024-05")) == [Month(2024, 5)]


def test_quarter_from_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("INGEST_PERIOD", "2024Q2")
    assert resolve_months(_args()) == [Month(2024, 4), Month(2024, 5), Month(2024, 6)]


def test_month_from_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("INGEST_PERIOD", "2024-01")
    assert resolve_months(_args()) == [Month(2024, 1)]


def test_invalid_period_from_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("INGEST_PERIOD", "2024/01")
    with pytest.raises(ValueError, match="YYYYQn oder YYYY-MM"):
        resolve_months(_args())


def test_no_period_given() -> None:
    with pytest.raises(ValueError, match="Kein Zeitraum"):
        resolve_months(_args())


@pytest.mark.parametrize(
    ("counts", "expected"),
    [
        ({"loaded": 3}, 0),
        ({"loaded": 1, "skipped": 2}, 0),
        ({"loaded": 2, "not_published": 1}, 3),
        ({"loaded": 2, "failed": 1}, 1),
        ({"failed": 1, "not_published": 1}, 1),
    ],
)
def test_exit_code(counts: dict[str, int], expected: int) -> None:
    assert exit_code(counts) == expected
