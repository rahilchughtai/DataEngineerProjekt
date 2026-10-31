from pathlib import Path
from typing import Any

import pytest

from ingestion import job
from ingestion.config import Config
from ingestion.download import DownloadResult, NotPublishedError, SourceInfo
from ingestion.job import ingest_month, main
from pipeline_common.periods import Month

RUN_ID_FILE = "PIPELINE_RUN_ID_FILE"


@pytest.fixture(autouse=True)
def clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("INGEST_PERIOD", raising=False)
    monkeypatch.delenv(RUN_ID_FILE, raising=False)


def set_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, period: str, run_id: str) -> None:
    monkeypatch.setenv("S3_ENDPOINT", "http://s3.test:8333")
    monkeypatch.setenv("S3_INGESTION_ACCESS_KEY", "key")
    monkeypatch.setenv("S3_INGESTION_SECRET_KEY", "secret")
    monkeypatch.setenv("BRONZE_BUCKET", "bronze")
    monkeypatch.setenv("TLC_BASE_URL", "https://tlc.test/trip-data")
    monkeypatch.setenv("TLC_DATASET", "yellow_tripdata")
    monkeypatch.setenv("INGEST_PERIOD", period)
    (tmp_path / "run-id").write_text(run_id, encoding="utf-8")
    monkeypatch.setenv(RUN_ID_FILE, str(tmp_path / "run-id"))


def test_main_rejects_invalid_run_id_as_config_error(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    set_env(monkeypatch, tmp_path, "2024-01", "kaputt")

    def no_client(*_: object) -> None:
        raise AssertionError("S3-Client darf bei Konfigurationsfehler nicht entstehen")

    monkeypatch.setattr(job.storage, "make_client", no_client)
    assert main() == 2


@pytest.mark.parametrize(("stored_etag", "status"), [("abc", "skipped"), ("old", "loaded")])
def test_ingest_month_reloads_only_changed_source(
    monkeypatch: pytest.MonkeyPatch, cfg: Config, parquet_file: Path, stored_etag: str, status: str
) -> None:
    source = SourceInfo(url="u", etag="abc", last_modified="")
    uploads: list[str] = []
    monkeypatch.setattr(job, "fetch_source_info", lambda *_: source)
    monkeypatch.setattr(job.storage, "stored_metadata", lambda *_: {"source-etag": stored_etag})
    monkeypatch.setattr(job, "download", lambda *_: DownloadResult(parquet_file, 40, "f00", 0.1))
    monkeypatch.setattr(job.storage, "upload", lambda _s3, _bucket, key, *_: uploads.append(key))

    entry = ingest_month(Month(2024, 1), cfg, None, None)

    assert entry["status"] == status
    # Immer derselbe Key: eine Wiederholung überschreibt das Objekt statt ein zweites anzulegen.
    assert uploads == ([] if status == "skipped" else [entry["key"]])


def test_main_reports_failed_and_unpublished_months(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    set_env(monkeypatch, tmp_path, "2024Q1", "20240401T000000Z-abc123")
    reports: list[dict[str, Any]] = []

    def ingest(month: Month, *_: object) -> dict[str, Any]:
        if month.month == 2:
            raise NotPublishedError("fehlt")
        if month.month == 3:
            raise TimeoutError()
        return {"month": str(month), "status": "loaded"}

    monkeypatch.setattr(job.storage, "make_client", lambda _cfg: None)
    monkeypatch.setattr(job, "ingest_month", ingest)
    monkeypatch.setattr(
        job.storage, "write_run_report", lambda _s3, _b, _d, report: reports.append(report) or "k"
    )

    assert main() == 1
    (report,) = reports
    assert report["counts"] == {"loaded": 1, "not_published": 1, "failed": 1}
    failed = report["months"][2]
    assert failed["error"] == "TimeoutError: "
    # Quelle und Ziel bleiben auch bei einem Fehler im Report nachvollziehbar.
    assert failed["key"] == "yellow_tripdata/year=2024/month=03/yellow_tripdata_2024-03.parquet"
    assert report["months"][1]["source_url"].endswith("/yellow_tripdata_2024-02.parquet")
