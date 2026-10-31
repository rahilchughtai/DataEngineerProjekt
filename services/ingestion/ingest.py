"""Ingestion: loads the TLC monthly files of the period unchanged into the bronze layer.

Per month: if the file already exists in bronze, it is skipped (idempotent). Otherwise it is
streamed from the TLC and stored under bronze/yellow_tripdata/year=YYYY/month=MM/. A failure
aborts the run; docker compose then does not start the Spark jobs.
"""

import logging
import os

import boto3
import requests
from botocore.config import Config
from botocore.exceptions import ClientError
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from pipeline_common.logs import setup_logging
from pipeline_common.periods import months, partition

log = logging.getLogger("ingestion")

BUCKET = "bronze"
DATASET = "yellow_tripdata"


def file_name(year: int, month: int) -> str:
    return f"{DATASET}_{year:04d}-{month:02d}.parquet"


def object_key(year: int, month: int) -> str:
    return f"{DATASET}/{partition(year, month)}/{file_name(year, month)}"


def http_session() -> requests.Session:
    session = requests.Session()
    # Up to 5 retries with growing backoff on connection errors, 429 and 5xx.
    # 403/404 means the month is not published (yet), which fails immediately.
    retry = Retry(total=5, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504])
    session.mount("http://", HTTPAdapter(max_retries=retry))
    session.mount("https://", HTTPAdapter(max_retries=retry))
    # Download uncompressed so bronze is byte-identical to the source.
    session.headers["Accept-Encoding"] = "identity"
    return session


def exists(s3, key: str) -> bool:
    try:
        s3.head_object(Bucket=BUCKET, Key=key)
    except ClientError as err:
        if err.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
            return False
        raise
    return True


def ingest_month(s3, http: requests.Session, base_url: str, year: int, month: int) -> None:
    key = object_key(year, month)
    if exists(s3, key):
        log.info("s3://%s/%s existiert bereits, übersprungen", BUCKET, key)
        return
    url = f"{base_url}/{file_name(year, month)}"
    with http.get(url, stream=True, timeout=60) as response:
        response.raise_for_status()
        # An upload creates the object only at the end: an abort leaves no half month behind.
        s3.upload_fileobj(response.raw, BUCKET, key)
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
    http = http_session()
    base_url = os.environ["TLC_BASE_URL"].rstrip("/")
    for year, month in months(os.environ["INGEST_PERIOD"]):
        ingest_month(s3, http, base_url, year, month)


if __name__ == "__main__":
    main()
