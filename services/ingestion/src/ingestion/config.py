"""Konfiguration aus Umgebungsvariablen."""

import os
from dataclasses import dataclass


class ConfigError(ValueError):
    pass


def _required(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise ConfigError(f"Umgebungsvariable {name} fehlt (siehe .env.example)")
    return value


@dataclass(frozen=True)
class Config:
    s3_endpoint: str
    s3_access_key: str
    s3_secret_key: str
    s3_region: str
    bronze_bucket: str
    base_url: str
    dataset: str
    max_attempts: int
    backoff_seconds: float
    timeout_seconds: float

    @classmethod
    def from_env(cls) -> "Config":
        return cls(
            s3_endpoint=_required("S3_ENDPOINT"),
            s3_access_key=_required("S3_INGESTION_ACCESS_KEY"),
            s3_secret_key=_required("S3_INGESTION_SECRET_KEY"),
            s3_region=os.environ.get("S3_REGION", "us-east-1"),
            bronze_bucket=os.environ.get("BRONZE_BUCKET", "bronze"),
            base_url=_required("TLC_BASE_URL").rstrip("/"),
            dataset=os.environ.get("TLC_DATASET", "yellow_tripdata"),
            max_attempts=int(os.environ.get("DOWNLOAD_MAX_ATTEMPTS", "5")),
            backoff_seconds=float(os.environ.get("DOWNLOAD_BACKOFF_SECONDS", "2")),
            timeout_seconds=float(os.environ.get("DOWNLOAD_TIMEOUT_SECONDS", "60")),
        )
