"""Fixtures des End-to-End-Tests: S3-Zugriff je Identität, Testdaten und Laufbericht.

Der Test läuft als Container e2e-tests am Ende der Kette aus docker-compose.test.yml und liest
danach nur noch den Data Lake.
"""

import json
import os
from pathlib import Path
from typing import Any

import boto3
import pytest
from botocore.config import Config
from pyarrow import fs

HERE = Path(__file__).resolve().parent
FIXTURES = HERE / "fixtures" / "trip-data"
PERIOD = "2024Q1"
BUCKETS = ("bronze", "silver", "gold")
# Identitäten und Rechte aus seaweedfs/s3.template.json (kopiert der Dockerfile neben die Tests).
IDENTITIES: list[dict[str, Any]] = json.loads((HERE / "s3.template.json").read_text())["identities"]
ADMIN = next(identity for identity in IDENTITIES if "Admin" in identity["actions"])


def credentials(identity: dict[str, Any]) -> tuple[str, str]:
    # Platzhalter wie __S3_ADMIN_ACCESS_KEY__ ersetzt der SeaweedFS-Entrypoint durch diese Variable.
    (credential,) = identity["credentials"]
    return os.environ[credential["accessKey"].strip("_")], os.environ[
        credential["secretKey"].strip("_")
    ]


def s3_client(identity: dict[str, Any]) -> Any:
    access_key, secret_key = credentials(identity)
    return boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT"],
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 2}),
    )


def keys(s3: Any, bucket: str, prefix: str = "") -> list[str]:
    pages = s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix)
    return sorted(item["Key"] for page in pages for item in page.get("Contents", []))


@pytest.fixture(scope="session")
def s3() -> Any:
    return s3_client(ADMIN)


@pytest.fixture(scope="session")
def lake_fs() -> fs.S3FileSystem:
    access_key, secret_key = credentials(ADMIN)
    return fs.S3FileSystem(
        access_key=access_key,
        secret_key=secret_key,
        endpoint_override=os.environ["S3_ENDPOINT"],
        region="us-east-1",
    )


@pytest.fixture(scope="session")
def report(s3: Any) -> dict[str, dict[str, Any]]:
    """Laufbericht des Quartals: Monat → Zähler (JSON Lines, eine Zeile je Monat)."""
    (key,) = [k for k in keys(s3, "gold", f"_reports/{PERIOD}/") if k.endswith(".json")]
    lines = s3.get_object(Bucket="gold", Key=key)["Body"].read().decode().splitlines()
    return {row["month"]: row for row in map(json.loads, lines)}
