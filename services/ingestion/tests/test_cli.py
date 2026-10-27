import argparse

import pytest

from ingestion.cli import resolve_months
from ingestion.months import Month


def _args(quarter: str | None = None, month: str | None = None) -> argparse.Namespace:
    return argparse.Namespace(quarter=quarter, month=month, force=False)


@pytest.fixture(autouse=True)
def clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("INGEST_QUARTER", raising=False)
    monkeypatch.delenv("INGEST_MONTH", raising=False)


def test_cli_argument_beats_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("INGEST_QUARTER", "2023Q1")
    assert resolve_months(_args(month="2024-05")) == [Month(2024, 5)]


def test_quarter_from_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("INGEST_QUARTER", "2024Q2")
    assert resolve_months(_args()) == [Month(2024, 4), Month(2024, 5), Month(2024, 6)]


def test_month_from_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("INGEST_MONTH", "2024-01")
    assert resolve_months(_args()) == [Month(2024, 1)]


def test_both_environment_variables_conflict(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("INGEST_QUARTER", "2024Q1")
    monkeypatch.setenv("INGEST_MONTH", "2024-01")
    with pytest.raises(ValueError, match="beide"):
        resolve_months(_args())


def test_no_period_given() -> None:
    with pytest.raises(ValueError, match="Kein Zeitraum"):
        resolve_months(_args())
