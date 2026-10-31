"""Ingestion: lädt die TLC-Monatsdateien des Zeitraums unverändert in die Bronze-Schicht.

Je Monat: Existiert die Datei schon in Bronze, wird sie übersprungen (idempotent). Sonst wird sie
von der TLC gestreamt und unter bronze/yellow_tripdata/year=YYYY/month=MM/ abgelegt. Ein Fehler
bricht den Lauf ab; docker compose startet die Spark-Jobs dann nicht.
"""

import logging
import os

import boto3
import requests
from botocore.config import Config
from botocore.exceptions import ClientError
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

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
    # Bis zu 5 Wiederholungen mit wachsender Pause bei Verbindungsfehlern, 429 und 5xx.
    # 403/404 heißt: Monat (noch) nicht veröffentlicht, das ist sofort ein Fehler.
    retry = Retry(total=5, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504])
    session.mount("http://", HTTPAdapter(max_retries=retry))
    session.mount("https://", HTTPAdapter(max_retries=retry))
    # Unkomprimiert laden, damit Bronze byte-gleich zur Quelle ist.
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
        # Ein Upload legt das Objekt erst am Ende an: bei einem Abbruch bleibt kein halber Monat.
        s3.upload_fileobj(response.raw, BUCKET, key)
    log.info("%s → s3://%s/%s", url, BUCKET, key)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    # Eigene Identität mit Schreibrecht nur auf bronze (Least Privilege, siehe seaweedfs/).
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
