import pytest

from ingestion.config import Config
from pipeline_common.env import ConfigError

REQUIRED = {
    "S3_ENDPOINT": "http://seaweedfs:8333",
    "S3_INGESTION_ACCESS_KEY": "key",
    "S3_INGESTION_SECRET_KEY": "secret",
    "TLC_BASE_URL": "https://tlc.test/trip-data",
    "BRONZE_BUCKET": "bronze",
    "TLC_DATASET": "yellow_tripdata",
}
OPTIONAL = [
    "S3_REGION",
    "DOWNLOAD_MAX_ATTEMPTS",
    "DOWNLOAD_BACKOFF_SECONDS",
    "DOWNLOAD_TIMEOUT_SECONDS",
]


@pytest.fixture
def env(monkeypatch: pytest.MonkeyPatch) -> pytest.MonkeyPatch:
    for name in OPTIONAL:
        monkeypatch.delenv(name, raising=False)
    for name, value in REQUIRED.items():
        monkeypatch.setenv(name, value)
    return monkeypatch


def test_defaults(env: pytest.MonkeyPatch) -> None:
    cfg = Config.from_env()
    assert cfg.s3_endpoint == "http://seaweedfs:8333"
    assert cfg.bronze_bucket == "bronze"
    assert cfg.dataset == "yellow_tripdata"
    assert cfg.max_attempts == 5
    assert cfg.backoff_seconds == 2.0


@pytest.mark.parametrize("name", list(REQUIRED))
def test_missing_required_variable(env: pytest.MonkeyPatch, name: str) -> None:
    env.setenv(name, "  ")
    with pytest.raises(ConfigError, match=name):
        Config.from_env()


def test_overrides_and_trailing_slash(env: pytest.MonkeyPatch) -> None:
    env.setenv("TLC_BASE_URL", "https://mirror.test/data/")
    env.setenv("DOWNLOAD_MAX_ATTEMPTS", "7")
    cfg = Config.from_env()
    assert cfg.base_url == "https://mirror.test/data"
    assert cfg.max_attempts == 7
