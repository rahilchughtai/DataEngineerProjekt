from pathlib import Path

import pytest

from ingestion import job
from ingestion.job import main

RUN_ID_FILE = "PIPELINE_RUN_ID_FILE"


@pytest.fixture(autouse=True)
def clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("INGEST_PERIOD", raising=False)
    monkeypatch.delenv(RUN_ID_FILE, raising=False)


def test_main_rejects_invalid_run_id_as_config_error(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("S3_ENDPOINT", "http://s3.test:8333")
    monkeypatch.setenv("S3_INGESTION_ACCESS_KEY", "key")
    monkeypatch.setenv("S3_INGESTION_SECRET_KEY", "secret")
    monkeypatch.setenv("INGEST_PERIOD", "2024-01")
    (tmp_path / "run-id").write_text("kaputt", encoding="utf-8")
    monkeypatch.setenv(RUN_ID_FILE, str(tmp_path / "run-id"))

    def no_client(*_: object) -> None:
        raise AssertionError("S3-Client darf bei Konfigurationsfehler nicht entstehen")

    monkeypatch.setattr(job.storage, "make_client", no_client)
    assert main() == 2
