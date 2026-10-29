"""Lesender Zugriff auf den Data Lake des Testprojekts.

Nichts ist hier fest eingetragen: Endpunkt, Buckets und Zugangsdaten stammen aus der aufgelösten
Compose-Konfiguration, die Identitäten aus seaweedfs/s3.json.template. Tabellen werden an ihren
Hive-Partitionen (…/year=YYYY/month=MM/…) erkannt, Reports an der Run-ID unter _runs/.
"""

import hashlib
import io
import json
import posixpath
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.config import Config
from botocore.exceptions import ClientError

ROOT = Path(__file__).resolve().parents[2]
FIXTURES = Path(__file__).resolve().parent / "fixtures" / "trip-data"
IDENTITIES: list[dict[str, Any]] = json.loads(
    (ROOT / "seaweedfs" / "s3.json.template").read_text(encoding="utf-8")
)["identities"]
ADMIN = next(identity["name"] for identity in IDENTITIES if "Admin" in identity["actions"])


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


@dataclass(frozen=True)
class Settings:
    endpoint: str
    # Schicht (bronze, silver, gold) → Bucket, wie ihn der schreibende Service konfiguriert bekommt.
    buckets: dict[str, str]
    tlc_base_url: str
    credentials: dict[str, tuple[str, str]]
    host_ports: list[int]

    @classmethod
    def from_compose(cls, config: dict[str, Any]) -> "Settings":
        services = config["services"]
        lake = services["seaweedfs"]
        (s3_port,) = lake["ports"]

        def secret(placeholder: str) -> str:
            # Platzhalter wie __S3_ADMIN_ACCESS_KEY__ ersetzt der Entrypoint durch diese Variable.
            return lake["environment"][placeholder.strip("_")]

        credentials = {}
        for identity in IDENTITIES:
            (credential,) = identity["credentials"]
            credentials[identity["name"]] = (
                secret(credential["accessKey"]),
                secret(credential["secretKey"]),
            )
        return cls(
            endpoint=f"http://{s3_port['host_ip']}:{s3_port['published']}",
            buckets={
                "bronze": services["ingestion"]["environment"]["BRONZE_BUCKET"],
                "silver": services["silver-job"]["environment"]["SILVER_BUCKET"],
                "gold": services["gold-job"]["environment"]["GOLD_BUCKET"],
            },
            tlc_base_url=services["ingestion"]["environment"]["TLC_BASE_URL"],
            credentials=credentials,
            host_ports=[
                int(port["published"])
                for service in services.values()
                for port in service.get("ports", [])
            ],
        )


@dataclass(frozen=True)
class LakeState:
    """Inhalt aller Schichten zu einem Zeitpunkt; spätere Läufe überschreiben die Partitionen."""

    # Bucket → Tabelle (Pfad vor den Partitionen) → Zeilen mit Partitionsspalten als Strings.
    tables: dict[str, dict[str, pa.Table]]
    # Bucket → Partitionsverzeichnis → Zahl der Objekte darin.
    files: dict[str, Counter[str]]


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
        (key,) = [k for k in self.keys(bucket, "_runs/") if k.endswith(f"/{run_id}.json")]
        return key

    def report(self, bucket: str, run_id: str) -> dict[str, Any]:
        return json.loads(self.read(bucket, self.report_key(bucket, run_id)))

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
        parts: dict[str, list[pa.Table]] = {}
        for key in self.keys(bucket):
            directory, file_name = posixpath.split(key)
            segments = directory.split("/")
            partitions = [s.split("=", 1) for s in segments if "=" in s]
            if not file_name.endswith(".parquet") or not partitions:
                continue
            table = pq.read_table(io.BytesIO(self.read(bucket, key)))
            for column, value in partitions:
                table = table.append_column(column, pa.array([value] * table.num_rows))
            table_name = "/".join(s for s in segments if "=" not in s)
            parts.setdefault(table_name, []).append(table)
        return {
            name: pa.concat_tables(tables, promote_options="default")
            for name, tables in parts.items()
        }

    def state(self) -> LakeState:
        buckets = self.settings.buckets.values()
        return LakeState(
            tables={bucket: self.tables(bucket) for bucket in buckets},
            files={
                bucket: Counter(
                    posixpath.dirname(k) for k in self.keys(bucket) if "=" in posixpath.dirname(k)
                )
                for bucket in buckets
            },
        )


@dataclass(frozen=True)
class Run:
    """Ergebnis eines Orchestrator-Laufs: Exit-Code, Laufbericht und Stand des Lake danach."""

    exit_code: int
    report: dict[str, Any]
    state: LakeState

    @property
    def run_id(self) -> str:
        return self.report["run_id"]
