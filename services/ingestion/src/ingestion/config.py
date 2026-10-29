"""Konfiguration aus Umgebungsvariablen."""

from dataclasses import dataclass

from pipeline_common.env import optional, required


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
            s3_endpoint=required("S3_ENDPOINT"),
            s3_access_key=required("S3_INGESTION_ACCESS_KEY"),
            s3_secret_key=required("S3_INGESTION_SECRET_KEY"),
            s3_region=optional("S3_REGION", "us-east-1"),
            bronze_bucket=optional("BRONZE_BUCKET", "bronze"),
            base_url=required("TLC_BASE_URL").rstrip("/"),
            dataset=optional("TLC_DATASET", "yellow_tripdata"),
            max_attempts=int(optional("DOWNLOAD_MAX_ATTEMPTS", "5")),
            backoff_seconds=float(optional("DOWNLOAD_BACKOFF_SECONDS", "2")),
            timeout_seconds=float(optional("DOWNLOAD_TIMEOUT_SECONDS", "60")),
        )
