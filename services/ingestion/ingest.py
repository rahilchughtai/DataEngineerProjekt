"""Ingestion: loads the TLC monthly files of the period unchanged into the bronze layer.

Per month: if the file already exists in bronze, it is skipped (idempotent). Otherwise it is
streamed from the TLC and stored under bronze/yellow_tripdata/year=YYYY/month=MM/. A failure
aborts the run; docker compose then does not start the Spark jobs.
"""

import logging
import os
import time

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


def download_to_bronze(s3: BaseClient, session: requests.Session, url: str, key: str) -> None:
    with session.get(url, stream=True, timeout=60) as response:
        response.raise_for_status()
        # An upload creates the object only at the end: an abort leaves no half month behind.
        s3.upload_fileobj(response.raw, BUCKET, key)


def ingest_month(
    s3: BaseClient, session: requests.Session, base_url: str, year: int, month: int
) -> None:
    key = object_key(year, month)
    if object_exists(s3, key):
        log.info("s3://%s/%s already exists, skipped", BUCKET, key)
        return
    url = f"{base_url}/{file_name(year, month)}"
    for attempt in range(1, ATTEMPTS + 1):
        try:
            download_to_bronze(s3, session, url, key)
            break
        # Not RequestException: that would also retry HTTPError (403/404 = month not published).
        # Errors while reading response.raw come from urllib3, not from requests.
        except (requests.ConnectionError, requests.Timeout, urllib3.exceptions.HTTPError) as err:
            if attempt == ATTEMPTS:
                raise
            log.warning("%s: attempt %d/%d failed (%s)", url, attempt, ATTEMPTS, err)
            time.sleep(2**attempt)
    log.info("%s → s3://%s/%s", url, BUCKET, key)


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
