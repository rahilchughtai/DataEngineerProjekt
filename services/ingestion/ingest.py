"""Ingestion: loads the TLC monthly files of the period unchanged into the bronze layer.

Per month: the file is streamed from the TLC to bronze/yellow_tripdata/year=YYYY/month=MM/,
and a manifest with SHA-256, size and source version (ETag, Last-Modified) goes to
bronze/_manifests/. A rerun skips the month if the file is in bronze and the source version still
matches the manifest (idempotent); a changed source file is loaded again. A failure aborts the
run; docker compose then does not start the Spark jobs.
"""

import hashlib
import json
import logging
import os
import time
from datetime import UTC, datetime

import boto3
import requests
import urllib3
from botocore.client import BaseClient
from botocore.config import Config
from botocore.exceptions import ClientError
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from pipeline_common.logs import setup_logging
from pipeline_common.periods import months, partition

log = logging.getLogger("ingestion")

BUCKET = "bronze"
DATASET = "yellow_tripdata"
ATTEMPTS = 3


def file_name(year: int, month: int) -> str:
    return f"{DATASET}_{year:04d}-{month:02d}.parquet"


def object_key(year: int, month: int) -> str:
    return f"{DATASET}/{partition(year, month)}/{file_name(year, month)}"


def manifest_key(year: int, month: int) -> str:
    # Outside the month folder: Spark would read a manifest there as data.
    return f"_manifests/{DATASET}/{partition(year, month)}/manifest.json"


def http_session() -> requests.Session:
    session = requests.Session()
    # Up to 5 retries with growing backoff on connection errors, 429 and 5xx.
    # 403/404 means the month is not published (yet), which fails immediately.
    retry = Retry(total=5, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    # http:// only for the E2E mock (tlc-mock); the real TLC URL is https
    for prefix in ("http://", "https://"):
        session.mount(prefix, adapter)
    # Download uncompressed so bronze is byte-identical to the source.
    session.headers["Accept-Encoding"] = "identity"
    return session


def object_exists(s3: BaseClient, key: str) -> bool:
    try:
        s3.head_object(Bucket=BUCKET, Key=key)
    except ClientError as err:
        if err.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
            return False
        raise
    return True


def read_manifest(s3: BaseClient, key: str) -> dict | None:
    try:
        body = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
    except ClientError as err:
        if err.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
            return None
        raise
    return json.loads(body)


def source_version(response: requests.Response) -> dict:
    return {
        "etag": response.headers.get("ETag"),
        "last_modified": response.headers.get("Last-Modified"),
    }


def unchanged(s3: BaseClient, session: requests.Session, url: str, year: int, month: int) -> bool:
    """True if bronze holds the month and the source still has the version of the manifest."""
    manifest = read_manifest(s3, manifest_key(year, month))
    if manifest is None or not object_exists(s3, object_key(year, month)):
        return False
    response = session.head(url, timeout=60)
    response.raise_for_status()
    return source_version(response) == {k: manifest[k] for k in ("etag", "last_modified")}


class HashingReader:
    """Passes the download stream to the upload and computes SHA-256 and size on the way."""

    def __init__(self, raw) -> None:
        self.raw = raw
        self.sha256 = hashlib.sha256()
        self.size = 0

    def read(self, size: int = -1) -> bytes:
        chunk = self.raw.read(size)
        self.sha256.update(chunk)
        self.size += len(chunk)
        return chunk


def download_to_bronze(s3: BaseClient, session: requests.Session, url: str, key: str) -> dict:
    """Streams the file into bronze and returns its manifest."""
    with session.get(url, stream=True, timeout=60) as response:
        response.raise_for_status()
        reader = HashingReader(response.raw)
        # An upload creates the object only at the end: an abort leaves no half month behind.
        s3.upload_fileobj(reader, BUCKET, key)
    return {
        "source_url": url,
        **source_version(response),
        "sha256": reader.sha256.hexdigest(),
        "size_bytes": reader.size,
        "downloaded_at": datetime.now(UTC).isoformat(timespec="seconds"),
    }


def ingest_month(
    s3: BaseClient, session: requests.Session, base_url: str, year: int, month: int
) -> None:
    key = object_key(year, month)
    url = f"{base_url}/{file_name(year, month)}"
    if unchanged(s3, session, url, year, month):
        log.info("s3://%s/%s matches the source, skipped", BUCKET, key)
        return
    for attempt in range(1, ATTEMPTS + 1):
        try:
            manifest = download_to_bronze(s3, session, url, key)
            break
        # Not RequestException: that would also retry HTTPError (403/404 = month not published).
        # Errors while reading response.raw come from urllib3, not from requests.
        except (requests.ConnectionError, requests.Timeout, urllib3.exceptions.HTTPError) as err:
            if attempt == ATTEMPTS:
                raise
            log.warning("%s: attempt %d/%d failed (%s)", url, attempt, ATTEMPTS, err)
            time.sleep(2**attempt)
    # Written after the file: without a manifest, the next run loads the month again.
    s3.put_object(
        Bucket=BUCKET,
        Key=manifest_key(year, month),
        Body=json.dumps(manifest, indent=2).encode(),
        ContentType="application/json",
    )
    log.info("%s → s3://%s/%s (sha256 %s)", url, BUCKET, key, manifest["sha256"])


def main() -> None:
    setup_logging()
    # Own identity with write access to bronze only (least privilege, see seaweedfs/).
    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT"],
        aws_access_key_id=os.environ["S3_INGESTION_ACCESS_KEY"],
        aws_secret_access_key=os.environ["S3_INGESTION_SECRET_KEY"],
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}),
    )
    session = http_session()
    base_url = os.environ["TLC_BASE_URL"].rstrip("/")
    try:
        for year, month in months(os.environ["INGEST_PERIOD"]):
            ingest_month(s3, session, base_url, year, month)
    except Exception:
        log.exception("ingestion failed")
        raise


if __name__ == "__main__":
    main()
