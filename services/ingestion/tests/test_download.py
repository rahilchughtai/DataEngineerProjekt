import hashlib
from pathlib import Path

import pytest
import requests
import responses

from ingestion.config import Config
from ingestion.download import (
    InvalidFileError,
    NotPublishedError,
    TransientDownloadError,
    download,
    fetch_source_info,
    validate_parquet,
)

URL = "https://example.test/trip-data/yellow_tripdata_2024-01.parquet"


def test_validate_parquet_accepts_magic_bytes(parquet_file: Path) -> None:
    validate_parquet(parquet_file)


def test_validate_parquet_rejects_other_files(tmp_path: Path) -> None:
    path = tmp_path / "error.parquet"
    path.write_bytes(b"<html>Access Denied</html>")
    with pytest.raises(InvalidFileError):
        validate_parquet(path)


@responses.activate
def test_download_writes_file_and_hash(tmp_path: Path, cfg: Config, parquet_bytes: bytes) -> None:
    responses.get(URL, body=parquet_bytes)
    dest = tmp_path / "out.parquet"

    with requests.Session() as session:
        result = download(session, URL, dest, cfg)

    assert dest.read_bytes() == parquet_bytes
    assert result.size == len(parquet_bytes)
    assert result.sha256 == hashlib.sha256(parquet_bytes).hexdigest()


@responses.activate
@pytest.mark.parametrize("status", [403, 404])
def test_download_missing_file_is_not_retried(tmp_path: Path, cfg: Config, status: int) -> None:
    responses.get(URL, status=status)

    with requests.Session() as session, pytest.raises(NotPublishedError):
        download(session, URL, tmp_path / "out.parquet", cfg)

    assert len(responses.calls) == 1


@responses.activate
def test_download_retries_server_errors(tmp_path: Path, cfg: Config, parquet_bytes: bytes) -> None:
    responses.get(URL, status=503)
    responses.get(URL, body=parquet_bytes)

    with requests.Session() as session:
        result = download(session, URL, tmp_path / "out.parquet", cfg)

    assert len(responses.calls) == 2
    assert result.size == len(parquet_bytes)


@responses.activate
def test_download_gives_up_after_max_attempts(tmp_path: Path, cfg: Config) -> None:
    responses.get(URL, status=429)

    with requests.Session() as session, pytest.raises(TransientDownloadError):
        download(session, URL, tmp_path / "out.parquet", cfg)

    assert len(responses.calls) == cfg.max_attempts


@responses.activate
def test_download_retries_truncated_transfer(
    tmp_path: Path, cfg: Config, parquet_bytes: bytes
) -> None:
    # Abgebrochene Übertragung: weniger Bytes als im Content-Length angekündigt.
    responses.get(URL, body=parquet_bytes[:10], headers={"Content-Length": str(len(parquet_bytes))})
    responses.get(URL, body=parquet_bytes)

    with requests.Session() as session:
        result = download(session, URL, tmp_path / "out.parquet", cfg)

    assert len(responses.calls) == 2
    assert result.size == len(parquet_bytes)


@responses.activate
def test_download_rejects_invalid_content(tmp_path: Path, cfg: Config) -> None:
    responses.get(URL, body=b"not parquet at all")

    with requests.Session() as session, pytest.raises(InvalidFileError):
        download(session, URL, tmp_path / "out.parquet", cfg)


@responses.activate
def test_fetch_source_info_parses_headers(cfg: Config) -> None:
    responses.head(
        URL,
        headers={
            "ETag": '"abc123"',
            "Last-Modified": "Wed, 20 Mar 2024 10:00:00 GMT",
            "Content-Length": "4096",
        },
    )

    with requests.Session() as session:
        info = fetch_source_info(session, URL, cfg)

    assert info.etag == "abc123"
    assert info.last_modified == "Wed, 20 Mar 2024 10:00:00 GMT"
    assert info.content_length == 4096
