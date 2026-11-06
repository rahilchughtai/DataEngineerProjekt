import hashlib
import io
import json

import pytest
import requests
import responses
from botocore.exceptions import ClientError

import ingest

URL = "https://tlc.test/trip-data"
FILE_URL = f"{URL}/yellow_tripdata_2024-03.parquet"
BODY = b"PAR1 data PAR1"
VERSION = {"ETag": '"v1"', "Last-Modified": "Wed, 01 May 2024 00:00:00 GMT"}


class FakeS3:
    """Minimal stand-in for the boto3 client."""

    def __init__(self, existing: set[str] | None = None) -> None:
        self.objects = dict.fromkeys(existing or set(), b"")

    def head_object(self, Bucket: str, Key: str) -> None:  # noqa: N803
        if Key not in self.objects:
            raise ClientError({"Error": {"Code": "404"}}, "HeadObject")

    def get_object(self, Bucket: str, Key: str) -> dict:  # noqa: N803
        self.head_object(Bucket, Key)
        return {"Body": io.BytesIO(self.objects[Key])}

    def put_object(self, Bucket: str, Key: str, Body: bytes, ContentType: str) -> None:  # noqa: N803
        self.objects[Key] = Body

    def upload_fileobj(self, fileobj: io.RawIOBase, bucket: str, key: str) -> None:
        self.objects[key] = fileobj.read()


def manifest(s3: FakeS3) -> dict:
    return json.loads(s3.objects[ingest.manifest_key(2024, 3)])


def loaded(etag: str = '"v1"') -> FakeS3:
    """Bronze after an earlier run that loaded the source version etag."""
    s3 = FakeS3(existing={ingest.object_key(2024, 3)})
    s3.objects[ingest.manifest_key(2024, 3)] = json.dumps(
        {"etag": etag, "last_modified": VERSION["Last-Modified"]}
    ).encode()
    return s3


def test_object_key_is_partitioned_by_month() -> None:
    key = ingest.object_key(2024, 3)
    assert key == "yellow_tripdata/year=2024/month=03/yellow_tripdata_2024-03.parquet"


@responses.activate
def test_downloads_file_unchanged_into_bronze() -> None:
    responses.get(FILE_URL, body=BODY, headers=VERSION)
    s3 = FakeS3()

    ingest.ingest_month(s3, ingest.http_session(), URL, 2024, 3)

    assert s3.objects[ingest.object_key(2024, 3)] == BODY
    assert responses.calls[0].request.headers["Accept-Encoding"] == "identity"


@responses.activate
def test_manifest_records_checksum_and_source_version() -> None:
    responses.get(FILE_URL, body=BODY, headers=VERSION)
    s3 = FakeS3()

    ingest.ingest_month(s3, ingest.http_session(), URL, 2024, 3)

    assert manifest(s3) | {"downloaded_at": None} == {
        "source_url": FILE_URL,
        "etag": '"v1"',
        "last_modified": VERSION["Last-Modified"],
        "sha256": hashlib.sha256(BODY).hexdigest(),
        "size_bytes": len(BODY),
        "downloaded_at": None,
    }


@responses.activate
def test_unchanged_source_is_skipped() -> None:
    responses.head(FILE_URL, headers=VERSION)
    s3 = loaded()

    ingest.ingest_month(s3, ingest.http_session(), URL, 2024, 3)

    assert [call.request.method for call in responses.calls] == ["HEAD"]


@responses.activate
def test_changed_source_is_loaded_again() -> None:
    responses.head(FILE_URL, headers=VERSION)
    responses.get(FILE_URL, body=BODY, headers=VERSION)
    s3 = loaded(etag='"v0"')

    ingest.ingest_month(s3, ingest.http_session(), URL, 2024, 3)

    assert s3.objects[ingest.object_key(2024, 3)] == BODY
    assert manifest(s3)["etag"] == '"v1"'


@responses.activate
def test_month_without_manifest_is_loaded_again() -> None:
    responses.get(FILE_URL, body=BODY, headers=VERSION)
    s3 = FakeS3(existing={ingest.object_key(2024, 3)})

    ingest.ingest_month(s3, ingest.http_session(), URL, 2024, 3)

    assert s3.objects[ingest.object_key(2024, 3)] == BODY
    assert manifest(s3)["sha256"] == hashlib.sha256(BODY).hexdigest()


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
    responses.get(FILE_URL, body=BODY)
    s3 = FakeS3()

    ingest.ingest_month(s3, ingest.http_session(), URL, 2024, 3)

    assert s3.objects[ingest.object_key(2024, 3)] == BODY
    assert len(responses.calls) == 2


@responses.activate
def test_gives_up_after_all_attempts(no_sleep: None) -> None:
    responses.get(FILE_URL, body=requests.ConnectionError("reset"))
    s3 = FakeS3()

    with pytest.raises(requests.ConnectionError):
        ingest.ingest_month(s3, ingest.http_session(), URL, 2024, 3)
    assert len(responses.calls) == ingest.ATTEMPTS
    assert s3.objects == {}
