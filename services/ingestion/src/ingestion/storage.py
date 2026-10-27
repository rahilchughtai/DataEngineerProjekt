"""Zugriff auf die Bronze-Schicht im SeaweedFS-S3."""

import json
import logging
from datetime import UTC, datetime
from typing import Any

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError
from tenacity import (
    Retrying,
    before_sleep_log,
    retry_if_exception_type,
    stop_after_delay,
    wait_fixed,
)

from ingestion.config import Config
from ingestion.download import DownloadResult, SourceInfo
from ingestion.months import Month

log = logging.getLogger(__name__)

PARQUET_CONTENT_TYPE = "application/vnd.apache.parquet"


def make_client(cfg: Config) -> Any:
    return boto3.client(
        "s3",
        endpoint_url=cfg.s3_endpoint,
        aws_access_key_id=cfg.s3_access_key,
        aws_secret_access_key=cfg.s3_secret_key,
        region_name=cfg.s3_region,
        config=BotoConfig(
            s3={"addressing_style": "path"},
            retries={"max_attempts": 5, "mode": "standard"},
        ),
    )


def object_key(dataset: str, month: Month) -> str:
    """Deterministischer, nach Zeit partitionierter Key. Gleicher Monat → gleicher Key."""
    return f"{dataset}/year={month.year:04d}/month={month.month:02d}/{dataset}_{month}.parquet"


def wait_for_bucket(client: Any, bucket: str, timeout_seconds: float = 120) -> None:
    """Wartet, bis der Bucket erreichbar ist (falls der Compose-Provider depends_on ignoriert)."""
    retrying = Retrying(
        stop=stop_after_delay(timeout_seconds),
        wait=wait_fixed(3),
        retry=retry_if_exception_type(Exception),
        before_sleep=before_sleep_log(log, logging.WARNING),
        reraise=True,
    )
    retrying(client.head_bucket, Bucket=bucket)


def stored_metadata(client: Any, bucket: str, key: str) -> dict[str, str] | None:
    try:
        response = client.head_object(Bucket=bucket, Key=key)
    except ClientError as err:
        if err.response.get("Error", {}).get("Code") in ("404", "NoSuchKey", "NotFound"):
            return None
        raise
    return response.get("Metadata", {})


def is_current(metadata: dict[str, str] | None, source: SourceInfo) -> bool:
    """Bronze ist aktuell, wenn die Quelle seit dem letzten Laden unverändert ist (ETag)."""
    return bool(metadata and source.etag and metadata.get("source-etag") == source.etag)


def upload(
    client: Any,
    bucket: str,
    key: str,
    result: DownloadResult,
    source: SourceInfo,
) -> None:
    # Ein PUT auf denselben Key ersetzt das Objekt atomar: kein Duplikat, kein halber Stand.
    metadata = {
        "source-url": source.url,
        "source-etag": source.etag,
        "source-last-modified": source.last_modified,
        "sha256": result.sha256,
        "size-bytes": str(result.size),
        "ingested-at": datetime.now(UTC).isoformat(timespec="seconds"),
        "ingested-by": "ingestion-service",
    }
    client.upload_file(
        str(result.path),
        bucket,
        key,
        ExtraArgs={"Metadata": metadata, "ContentType": PARQUET_CONTENT_TYPE},
    )


def write_run_report(client: Any, bucket: str, dataset: str, report: dict[str, Any]) -> str:
    """Legt pro Lauf ein JSON-Protokoll ab (Lineage und Latenzen)."""
    key = f"_runs/{dataset}/{report['run_id']}.json"
    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(report, indent=2).encode(),
        ContentType="application/json",
    )
    return key
