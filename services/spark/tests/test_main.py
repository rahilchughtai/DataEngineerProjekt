import importlib
from pathlib import Path

import pytest

LAKE_ENV = {
    "BRONZE_BUCKET": "bronze",
    "SILVER_BUCKET": "silver",
    "GOLD_BUCKET": "gold",
    "TLC_DATASET": "yellow_tripdata",
}


@pytest.fixture(autouse=True)
def clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PIPELINE_RUN_ID_FILE", raising=False)


@pytest.mark.parametrize("job", ["bronze_to_silver", "silver_to_gold"])
def test_main_rejects_invalid_run_id_before_spark_starts(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, job: str
) -> None:
    module = importlib.import_module(f"spark_jobs.{job}")
    monkeypatch.setenv("INGEST_PERIOD", "2024-01")
    for name, value in LAKE_ENV.items():
        monkeypatch.setenv(name, value)
    (tmp_path / "run-id").write_text("kaputt", encoding="utf-8")
    monkeypatch.setenv("PIPELINE_RUN_ID_FILE", str(tmp_path / "run-id"))

    def no_session(_: str) -> None:
        raise AssertionError("Spark-Session darf bei Konfigurationsfehler nicht starten")

    monkeypatch.setattr(module, "build_session", no_session)
    assert module.main() == 2
