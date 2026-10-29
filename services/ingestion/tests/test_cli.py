import argparse
import re

import pytest

from ingestion import cli
from ingestion.cli import exit_code, main, new_run_id, resolve_months, resolve_run_id
from ingestion.months import Month

RUN_ID = "20240401T000000Z-abc123"


def _args(quarter: str | None = None, month: str | None = None) -> argparse.Namespace:
    return argparse.Namespace(quarter=quarter, month=month, force=False)


def test_new_run_id_is_unique_and_sortable() -> None:
    ids = [new_run_id() for _ in range(50)]
    assert len(set(ids)) == len(ids)
    assert all(re.fullmatch(r"\d{8}T\d{6}Z-[0-9a-f]{6}", run_id) for run_id in ids)


@pytest.fixture(autouse=True)
def clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("INGEST_PERIOD", raising=False)
    monkeypatch.delenv("PIPELINE_RUN_ID", raising=False)


def test_run_id_from_orchestrator(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PIPELINE_RUN_ID", f" {RUN_ID} ")
    assert resolve_run_id() == RUN_ID


@pytest.mark.parametrize("value", [None, "", "   "])
def test_run_id_falls_back_to_new_id(monkeypatch: pytest.MonkeyPatch, value: str | None) -> None:
    if value is not None:
        monkeypatch.setenv("PIPELINE_RUN_ID", value)
    assert re.fullmatch(r"\d{8}T\d{6}Z-[0-9a-f]{6}", resolve_run_id())


@pytest.mark.parametrize(
    "value", ["abc", "20240401T000000Z-ABC123", "20240401T000000Z-abc1234", "../x"]
)
def test_invalid_run_id_is_rejected(monkeypatch: pytest.MonkeyPatch, value: str) -> None:
    monkeypatch.setenv("PIPELINE_RUN_ID", value)
    with pytest.raises(ValueError, match="PIPELINE_RUN_ID"):
        resolve_run_id()


def test_main_rejects_invalid_run_id_as_config_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("S3_ENDPOINT", "http://s3.test:8333")
    monkeypatch.setenv("S3_INGESTION_ACCESS_KEY", "key")
    monkeypatch.setenv("S3_INGESTION_SECRET_KEY", "secret")
    monkeypatch.setenv("PIPELINE_RUN_ID", "kaputt")

    def no_client(*_: object) -> None:
        raise AssertionError("S3-Client darf bei Konfigurationsfehler nicht entstehen")

    monkeypatch.setattr(cli.storage, "make_client", no_client)
    assert main(["--month", "2024-01"]) == 2


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
