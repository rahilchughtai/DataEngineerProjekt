import importlib
from pathlib import Path

import pytest

from spark_jobs.runtime import exit_code, read_run_id

RUN_ID = "20240401T000000Z-abc123"


@pytest.fixture(autouse=True)
def clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PIPELINE_RUN_ID_FILE", raising=False)


def write_run_id(monkeypatch: pytest.MonkeyPatch, path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")
    monkeypatch.setenv("PIPELINE_RUN_ID_FILE", str(path))


def test_run_id_from_run_init(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    write_run_id(monkeypatch, tmp_path / "run-id", f"{RUN_ID}\n")
    assert read_run_id() == RUN_ID


@pytest.mark.parametrize("value", [None, "", "   "])
def test_run_id_file_is_required(monkeypatch: pytest.MonkeyPatch, value: str | None) -> None:
    if value is not None:
        monkeypatch.setenv("PIPELINE_RUN_ID_FILE", value)
    with pytest.raises(ValueError, match="PIPELINE_RUN_ID_FILE fehlt"):
        read_run_id()


@pytest.mark.parametrize(
    "value", ["", "abc", "20240401T000000Z-ABC123", "20240401T000000Z-abc1234", "../x"]
)
def test_invalid_run_id_is_rejected(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, value: str
) -> None:
    write_run_id(monkeypatch, tmp_path / "run-id", value)
    with pytest.raises(ValueError, match="Ungültige Run-ID"):
        read_run_id()


def test_missing_run_id_file_is_rejected(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("PIPELINE_RUN_ID_FILE", str(tmp_path / "fehlt"))
    with pytest.raises(ValueError, match="PIPELINE_RUN_ID_FILE"):
        read_run_id()


@pytest.mark.parametrize("job", ["bronze_to_silver", "silver_to_gold"])
def test_main_rejects_invalid_run_id_before_spark_starts(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, job: str
) -> None:
    module = importlib.import_module(f"spark_jobs.{job}")
    monkeypatch.setenv("INGEST_PERIOD", "2024-01")
    write_run_id(monkeypatch, tmp_path / "run-id", "kaputt")

    def no_session(_: str) -> None:
        raise AssertionError("Spark-Session darf bei Konfigurationsfehler nicht starten")

    monkeypatch.setattr(module, "build_session", no_session)
    assert module.main() == 2


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
