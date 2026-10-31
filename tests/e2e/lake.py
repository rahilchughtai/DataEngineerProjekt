"""Lesender Zugriff auf den Data Lake des Testprojekts.

Nichts ist hier fest eingetragen: Endpunkt, Buckets und Zugangsdaten setzt docker-compose.test.yml
in der Umgebung des Containers, die Identitäten stammen aus seaweedfs/s3.json.template. Tabellen
werden an ihren Hive-Partitionen (…/year=YYYY/month=MM/…) erkannt, Reports an der Run-ID unter
_runs/. Der Laufbericht von run-report liegt getrennt davon unter _runs/pipeline/ in Gold.
"""

import hashlib
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.config import Config
from botocore.exceptions import ClientError
from pyarrow import fs

# Fixtures und Template kopiert tests/e2e/Dockerfile neben die Tests.
HERE = Path(__file__).resolve().parent
FIXTURES = HERE / "fixtures" / "trip-data"
IDENTITIES: list[dict[str, Any]] = json.loads(
    (HERE / "s3.json.template").read_text(encoding="utf-8")
)["identities"]
ADMIN = next(identity["name"] for identity in IDENTITIES if "Admin" in identity["actions"])
PIPELINE_REPORTS = "_runs/pipeline/"


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


@dataclass(frozen=True)
class Settings:
    endpoint: str
    # Schicht (bronze, silver, gold) → Bucket, wie ihn der schreibende Service konfiguriert bekommt.
    buckets: dict[str, str]
    tlc_base_url: str
    period: str
    runs_dir: Path
    credentials: dict[str, tuple[str, str]]

    @classmethod
    def from_env(cls) -> "Settings":
        def secret(placeholder: str) -> str:
            # Platzhalter wie __S3_ADMIN_ACCESS_KEY__ ersetzt der Entrypoint durch diese Variable.
            return os.environ[placeholder.strip("_")]

        credentials = {
            identity["name"]: (secret(credential["accessKey"]), secret(credential["secretKey"]))
            for identity in IDENTITIES
            for credential in identity["credentials"]
        }
        return cls(
            endpoint=os.environ["S3_ENDPOINT"],
            buckets={
                layer: os.environ[f"{layer.upper()}_BUCKET"]
                for layer in ("bronze", "silver", "gold")
            },
            tlc_base_url=os.environ["TLC_BASE_URL"],
            period=os.environ["INGEST_PERIOD"],
            runs_dir=Path(os.environ["RUNS_DIR"]),
            credentials=credentials,
        )


class Lake:
    def __init__(self, settings: Settings, identity: str = ADMIN) -> None:
        access_key, secret_key = settings.credentials[identity]
        self.settings = settings
        self.client = boto3.client(
            "s3",
            endpoint_url=settings.endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name="us-east-1",
            config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 2}),
        )
        self.fs = fs.S3FileSystem(
            access_key=access_key,
            secret_key=secret_key,
            endpoint_override=settings.endpoint,
            region="us-east-1",
        )

    def keys(self, bucket: str, prefix: str = "") -> list[str]:
        keys: list[str] = []
        for page in self.client.get_paginator("list_objects_v2").paginate(
            Bucket=bucket, Prefix=prefix
        ):
            keys += [item["Key"] for item in page.get("Contents", [])]
        return sorted(keys)

    def read(self, bucket: str, key: str) -> bytes:
        return self.client.get_object(Bucket=bucket, Key=key)["Body"].read()

    def report_key(self, bucket: str, run_id: str) -> str:
        """Report des Service, der in den Bucket schreibt; ohne den Laufbericht von run-report."""
        (key,) = [
            k
            for k in self.keys(bucket, "_runs/")
            if k.endswith(f"/{run_id}.json") and not k.startswith(PIPELINE_REPORTS)
        ]
        return key

    def report(self, bucket: str, run_id: str) -> dict[str, Any]:
        return json.loads(self.read(bucket, self.report_key(bucket, run_id)))

    def pipeline_report(self, run_id: str) -> dict[str, Any]:
        gold = self.settings.buckets["gold"]
        return json.loads(self.read(gold, f"{PIPELINE_REPORTS}{run_id}.json"))

    def can(self, operation: str, bucket: str, existing_key: str) -> bool:
        """Führt List, Read oder Write aus; False, wenn SeaweedFS den Zugriff verweigert."""
        actions = {
            "List": lambda: self.client.list_objects_v2(Bucket=bucket, MaxKeys=1),
            "Read": lambda: self.read(bucket, existing_key),
            "Write": lambda: self.client.put_object(Bucket=bucket, Key="_e2e/probe", Body=b""),
        }
        try:
            actions[operation]()
        except ClientError as err:
            if err.response["Error"]["Code"] == "AccessDenied":
                return False
            raise
        return True

    def tables(self, bucket: str) -> dict[str, pa.Table]:
        """Tabelle (Pfad vor den Partitionen) → Zeilen, Partitionsspalten year und month als int."""
        names = {key.split("/year=")[0] for key in self.keys(bucket) if "/year=" in key}
        return {name: pq.read_table(f"{bucket}/{name}", filesystem=self.fs) for name in names}
