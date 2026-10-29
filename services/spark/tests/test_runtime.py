import importlib
import re

import pytest

from spark_jobs.runtime import exit_code, new_run_id, resolve_run_id

RUN_ID = "20240401T000000Z-abc123"


@pytest.fixture(autouse=True)
def clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PIPELINE_RUN_ID", raising=False)


def test_new_run_id_is_unique_and_sortable() -> None:
    ids = [new_run_id() for _ in range(50)]
    assert len(set(ids)) == len(ids)
    assert all(re.fullmatch(r"\d{8}T\d{6}Z-[0-9a-f]{6}", run_id) for run_id in ids)
    assert sorted(ids, key=lambda run_id: run_id[:16]) == ids


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


@pytest.mark.parametrize("job", ["bronze_to_silver", "silver_to_gold"])
def test_main_rejects_invalid_run_id_before_spark_starts(
    monkeypatch: pytest.MonkeyPatch, job: str
) -> None:
    module = importlib.import_module(f"spark_jobs.{job}")
    monkeypatch.setenv("PIPELINE_RUN_ID", "kaputt")

    def no_session(_: str) -> None:
        raise AssertionError("Spark-Session darf bei Konfigurationsfehler nicht starten")

    monkeypatch.setattr(module, "build_session", no_session)
    assert module.main(["--month", "2024-01"]) == 2


@pytest.mark.parametrize(
    ("counts", "expected"),
    [
        ({"processed": 3}, 0),
        ({"processed": 2, "missing": 1}, 3),
        ({"missing": 3}, 3),
        ({}, 3),
        ({"processed": 2, "failed": 1}, 1),
        ({"failed": 1}, 1),
    ],
)
def test_exit_code(counts: dict[str, int], expected: int) -> None:
    assert exit_code({"counts": counts}, "Bronze") == expected
