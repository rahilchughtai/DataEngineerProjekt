import json
from pathlib import Path
from typing import Any

import pytest
from botocore.exceptions import ClientError

from ingestion import storage
from ingestion.download import DownloadResult, SourceInfo
from pipeline_common.periods import Month

SOURCE = SourceInfo(
    url="https://example.test/yellow_tripdata_2024-01.parquet",
    etag="abc123",
    last_modified="Wed, 20 Mar 2024 10:00:00 GMT",
    content_length=40,
)


class FakeS3:
    """Minimaler Ersatz für den boto3-Client; zeichnet Aufrufe auf."""

    def __init__(self, head_error: str | None = None, metadata: dict[str, str] | None = None):
        self.head_error = head_error
        self.metadata = metadata or {}
        self.uploads: list[dict[str, Any]] = []
        self.objects: dict[str, dict[str, Any]] = {}

    def head_object(self, Bucket: str, Key: str) -> dict[str, Any]:  # noqa: N803
        if self.head_error:
            raise ClientError({"Error": {"Code": self.head_error}}, "HeadObject")
        return {"Metadata": self.metadata}

    def upload_file(self, filename: str, bucket: str, key: str, ExtraArgs: dict) -> None:  # noqa: N803
        self.uploads.append({"file": filename, "bucket": bucket, "key": key, **ExtraArgs})

    def put_object(self, **kwargs: Any) -> None:
        self.objects[kwargs["Key"]] = kwargs


def test_object_key_is_partitioned_by_time() -> None:
    key = storage.object_key("yellow_tripdata", Month(2024, 3))
    assert key == "yellow_tripdata/year=2024/month=03/yellow_tripdata_2024-03.parquet"


def test_object_key_is_deterministic() -> None:
    # Idempotenz: derselbe Monat landet immer auf demselben Key.
    assert storage.object_key("d", Month(2024, 3)) == storage.object_key("d", Month(2024, 3))


@pytest.mark.parametrize(
    ("metadata", "etag", "expected"),
    [
        (None, "abc123", False),
        ({}, "abc123", False),
        ({"source-etag": "abc123"}, "abc123", True),
        ({"source-etag": "old"}, "abc123", False),
        ({"source-etag": ""}, "", False),
    ],
)
def test_is_current(metadata: dict[str, str] | None, etag: str, expected: bool) -> None:
    source = SourceInfo(url="u", etag=etag, last_modified="", content_length=None)
    assert storage.is_current(metadata, source) is expected


@pytest.mark.parametrize("code", ["404", "NoSuchKey", "NotFound"])
def test_stored_metadata_missing_object(code: str) -> None:
    assert storage.stored_metadata(FakeS3(head_error=code), "bronze", "k") is None


def test_stored_metadata_reraises_other_errors() -> None:
    with pytest.raises(ClientError):
        storage.stored_metadata(FakeS3(head_error="AccessDenied"), "bronze", "k")


def test_stored_metadata_returns_metadata() -> None:
    client = FakeS3(metadata={"source-etag": "abc123"})
    assert storage.stored_metadata(client, "bronze", "k") == {"source-etag": "abc123"}


def test_upload_sets_lineage_metadata(parquet_file: Path) -> None:
    client = FakeS3()
    result = DownloadResult(path=parquet_file, size=40, sha256="f00", seconds=1.0)

    storage.upload(client, "bronze", "some/key.parquet", result, SOURCE)

    [call] = client.uploads
    assert call["bucket"] == "bronze"
    assert call["key"] == "some/key.parquet"
    assert call["ContentType"] == storage.PARQUET_CONTENT_TYPE
    metadata = call["Metadata"]
    assert metadata["source-url"] == SOURCE.url
    assert metadata["source-etag"] == "abc123"
    assert metadata["sha256"] == "f00"
    assert metadata["size-bytes"] == "40"
    assert "ingested-at" in metadata


def test_write_run_report() -> None:
    client = FakeS3()
    report = {"run_id": "20240101T000000Z", "counts": {"loaded": 3}}

    key = storage.write_run_report(client, "bronze", "yellow_tripdata", report)

    assert key == "_runs/yellow_tripdata/20240101T000000Z.json"
    stored = client.objects[key]
    assert stored["Bucket"] == "bronze"
    assert stored["ContentType"] == "application/json"
    assert json.loads(stored["Body"]) == report
