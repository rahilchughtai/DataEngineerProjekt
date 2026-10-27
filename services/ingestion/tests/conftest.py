"""Gemeinsame Fixtures der Ingestion-Tests."""

from pathlib import Path

import pytest

from ingestion.config import Config
from ingestion.download import PARQUET_MAGIC

BASE_URL = "https://example.test/trip-data"


@pytest.fixture
def cfg() -> Config:
    # Kein Backoff, damit Retry-Tests ohne Wartezeit laufen.
    return Config(
        s3_endpoint="http://s3.test:8333",
        s3_access_key="key",
        s3_secret_key="secret",
        s3_region="us-east-1",
        bronze_bucket="bronze",
        base_url=BASE_URL,
        dataset="yellow_tripdata",
        max_attempts=3,
        backoff_seconds=0,
        timeout_seconds=5,
    )


@pytest.fixture
def parquet_bytes() -> bytes:
    return PARQUET_MAGIC + b"\x00" * 32 + PARQUET_MAGIC


@pytest.fixture
def parquet_file(tmp_path: Path, parquet_bytes: bytes) -> Path:
    path = tmp_path / "sample.parquet"
    path.write_bytes(parquet_bytes)
    return path
