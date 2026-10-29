from pathlib import Path

import pytest

from ingestion import cli
from ingestion.cli import exit_code, main, read_run_id

RUN_ID = "20240401T000000Z-abc123"


@pytest.fixture(autouse=True)
def clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("INGEST_PERIOD", raising=False)
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


def test_main_rejects_invalid_run_id_as_config_error(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("S3_ENDPOINT", "http://s3.test:8333")
    monkeypatch.setenv("S3_INGESTION_ACCESS_KEY", "key")
    monkeypatch.setenv("S3_INGESTION_SECRET_KEY", "secret")
    monkeypatch.setenv("INGEST_PERIOD", "2024-01")
    write_run_id(monkeypatch, tmp_path / "run-id", "kaputt")

    def no_client(*_: object) -> None:
        raise AssertionError("S3-Client darf bei Konfigurationsfehler nicht entstehen")

    monkeypatch.setattr(cli.storage, "make_client", no_client)
    assert main() == 2


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
