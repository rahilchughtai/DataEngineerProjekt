"""Hält periods.py im Gleichschritt mit der Kopie im Ingestion-Service."""

import importlib.util
from collections.abc import Callable
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

from spark_jobs import periods

INGESTION_MONTHS = Path(__file__).parents[2] / "ingestion" / "src" / "ingestion" / "months.py"


@pytest.fixture(scope="module")
def ingestion_months() -> ModuleType:
    if not INGESTION_MONTHS.exists():
        pytest.skip("Ingestion-Service nicht im Checkout (z. B. Testlauf im Spark-Image)")
    spec = importlib.util.spec_from_file_location("ingestion_months", INGESTION_MONTHS)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def outcome(parse: Callable[[str], Any], value: str) -> str:
    try:
        result = parse(value)
    except ValueError:
        return "ValueError"
    return ",".join(map(str, result)) if isinstance(result, list) else str(result)


@pytest.mark.parametrize(
    "value", ["2024-01", "2024-12", " 2024-03 ", "2024-13", "2024-00", "24-01", "2024Q1"]
)
def test_parse_month_matches_ingestion(ingestion_months: ModuleType, value: str) -> None:
    assert outcome(periods.parse_month, value) == outcome(ingestion_months.parse_month, value)


@pytest.mark.parametrize(
    "value", ["2024Q1", "2024q4", "2024-Q2", "2024Q5", "2024Q0", "24Q1", "2024-01"]
)
def test_quarter_months_matches_ingestion(ingestion_months: ModuleType, value: str) -> None:
    assert outcome(periods.quarter_months, value) == outcome(ingestion_months.quarter_months, value)


@pytest.mark.parametrize(
    "value", ["2024Q1", "2024-Q2", " 2024-03 ", "2024Q5", "2024-13", "2024/01", ""]
)
def test_parse_period_matches_ingestion(ingestion_months: ModuleType, value: str) -> None:
    assert outcome(periods.parse_period, value) == outcome(ingestion_months.parse_period, value)


@pytest.mark.parametrize(
    ("period", "expected"),
    [("2024Q1", "2024-01,2024-02,2024-03"), ("2024-03", "2024-03")],
)
def test_period_from_environment(
    monkeypatch: pytest.MonkeyPatch, period: str, expected: str
) -> None:
    monkeypatch.setenv("INGEST_PERIOD", period)
    assert ",".join(map(str, periods.months_from_env())) == expected


@pytest.mark.parametrize("value", ["2024Q1", "2024-03", "2024/01", ""])
def test_months_from_env_matches_ingestion(
    monkeypatch: pytest.MonkeyPatch, ingestion_months: ModuleType, value: str
) -> None:
    monkeypatch.setenv("INGEST_PERIOD", value)
    expected = outcome(lambda _: ingestion_months.months_from_env(), value)
    assert outcome(lambda _: periods.months_from_env(), value) == expected
