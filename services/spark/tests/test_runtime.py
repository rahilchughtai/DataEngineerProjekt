import importlib
from pathlib import Path

import pytest

from spark_jobs.runtime import exit_code


@pytest.fixture(autouse=True)
def clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PIPELINE_RUN_ID_FILE", raising=False)


@pytest.mark.parametrize("job", ["bronze_to_silver", "silver_to_gold"])
def test_main_rejects_invalid_run_id_before_spark_starts(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, job: str
) -> None:
    module = importlib.import_module(f"spark_jobs.{job}")
    monkeypatch.setenv("INGEST_PERIOD", "2024-01")
    (tmp_path / "run-id").write_text("kaputt", encoding="utf-8")
    monkeypatch.setenv("PIPELINE_RUN_ID_FILE", str(tmp_path / "run-id"))

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
