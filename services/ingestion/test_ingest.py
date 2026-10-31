import io

import pytest
import requests
import responses
from botocore.exceptions import ClientError

import ingest

URL = "https://tlc.test/trip-data"
FILE_URL = f"{URL}/yellow_tripdata_2024-03.parquet"


class FakeS3:
    """Minimal stand-in for the boto3 client."""

    def __init__(self, existing: set[str] | None = None) -> None:
        self.objects = dict.fromkeys(existing or set(), b"")

    def head_object(self, Bucket: str, Key: str) -> None:  # noqa: N803
        if Key not in self.objects:
            raise ClientError({"Error": {"Code": "404"}}, "HeadObject")

    def upload_fileobj(self, fileobj: io.RawIOBase, bucket: str, key: str) -> None:
        self.objects[key] = fileobj.read()


def test_object_key_is_partitioned_by_month() -> None:
    key = ingest.object_key(2024, 3)
    assert key == "yellow_tripdata/year=2024/month=03/yellow_tripdata_2024-03.parquet"


@responses.activate
def test_downloads_file_unchanged_into_bronze() -> None:
    responses.get(FILE_URL, body=b"PAR1 data PAR1")
    s3 = FakeS3()

    ingest.ingest_month(s3, ingest.http_session(), URL, 2024, 3)

    assert s3.objects[ingest.object_key(2024, 3)] == b"PAR1 data PAR1"
    assert responses.calls[0].request.headers["Accept-Encoding"] == "identity"


@responses.activate
def test_existing_month_is_skipped() -> None:
    s3 = FakeS3(existing={ingest.object_key(2024, 3)})

    ingest.ingest_month(s3, ingest.http_session(), URL, 2024, 3)

    assert len(responses.calls) == 0


@responses.activate
def test_unpublished_month_fails_without_upload() -> None:
    # CloudFront answers 403 for missing files.
    responses.get(FILE_URL, status=403)
    s3 = FakeS3()

    with pytest.raises(requests.HTTPError):
        ingest.ingest_month(s3, ingest.http_session(), URL, 2024, 3)
    assert s3.objects == {}


@pytest.fixture
def no_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(ingest.time, "sleep", lambda _: None)


@responses.activate
def test_retries_after_connection_error(no_sleep: None) -> None:
    responses.get(FILE_URL, body=requests.ConnectionError("reset"))
    responses.get(FILE_URL, body=b"PAR1 data PAR1")
    s3 = FakeS3()

    ingest.ingest_month(s3, ingest.http_session(), URL, 2024, 3)

    assert s3.objects[ingest.object_key(2024, 3)] == b"PAR1 data PAR1"
    assert len(responses.calls) == 2


@responses.activate
def test_gives_up_after_all_attempts(no_sleep: None) -> None:
    responses.get(FILE_URL, body=requests.ConnectionError("reset"))
    s3 = FakeS3()

    with pytest.raises(requests.ConnectionError):
        ingest.ingest_month(s3, ingest.http_session(), URL, 2024, 3)
    assert len(responses.calls) == ingest.ATTEMPTS
    assert s3.objects == {}
