"""HTTP-Download der TLC-Monatsdateien mit Retries und Integritätsprüfung."""

import hashlib
import logging
import time
from dataclasses import dataclass
from pathlib import Path

import requests
from tenacity import (
    Retrying,
    before_sleep_log,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from ingestion.config import Config

log = logging.getLogger(__name__)

PARQUET_MAGIC = b"PAR1"
CHUNK_SIZE = 1024 * 1024


class NotPublishedError(Exception):
    """Die Datei existiert (noch) nicht an der Quelle. Wird nicht wiederholt."""


class TransientDownloadError(Exception):
    """Vorübergehender Fehler (5xx, 429, abgebrochene Übertragung). Wird wiederholt."""


class InvalidFileError(Exception):
    """Die Datei ist kein gültiges Parquet. Wird nicht wiederholt."""


@dataclass(frozen=True)
class SourceInfo:
    url: str
    etag: str
    last_modified: str
    content_length: int | None


@dataclass(frozen=True)
class DownloadResult:
    path: Path
    size: int
    sha256: str
    seconds: float


_RETRYABLE = (requests.ConnectionError, requests.Timeout, TransientDownloadError)


def _retrying(cfg: Config) -> Retrying:
    return Retrying(
        stop=stop_after_attempt(cfg.max_attempts),
        wait=wait_exponential(multiplier=cfg.backoff_seconds, max=60),
        retry=retry_if_exception_type(_RETRYABLE),
        before_sleep=before_sleep_log(log, logging.WARNING),
        reraise=True,
    )


def _check_status(response: requests.Response, url: str) -> None:
    # CloudFront antwortet bei fehlenden Dateien mit 403 statt 404.
    if response.status_code in (403, 404):
        raise NotPublishedError(f"{url} ist nicht veröffentlicht (HTTP {response.status_code})")
    if response.status_code == 429 or response.status_code >= 500:
        raise TransientDownloadError(f"{url} lieferte HTTP {response.status_code}")
    response.raise_for_status()


def _head_once(session: requests.Session, url: str, cfg: Config) -> SourceInfo:
    response = session.head(url, timeout=cfg.timeout_seconds, allow_redirects=True)
    _check_status(response, url)
    length = response.headers.get("Content-Length")
    return SourceInfo(
        url=url,
        etag=response.headers.get("ETag", "").strip('"'),
        last_modified=response.headers.get("Last-Modified", ""),
        content_length=int(length) if length else None,
    )


def _download_once(session: requests.Session, url: str, dest: Path, cfg: Config) -> DownloadResult:
    started = time.monotonic()
    digest = hashlib.sha256()
    size = 0
    with session.get(url, stream=True, timeout=cfg.timeout_seconds) as response:
        _check_status(response, url)
        expected = response.headers.get("Content-Length")
        with dest.open("wb") as fh:
            for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                fh.write(chunk)
                digest.update(chunk)
                size += len(chunk)
    if expected is not None and size != int(expected):
        raise TransientDownloadError(f"{url}: {size} von {expected} Bytes empfangen")
    return DownloadResult(dest, size, digest.hexdigest(), time.monotonic() - started)


def validate_parquet(path: Path) -> None:
    """Prüft die Parquet-Magic-Bytes am Anfang und Ende der Datei."""
    with path.open("rb") as fh:
        head = fh.read(4)
        fh.seek(-4, 2)
        tail = fh.read(4)
    if head != PARQUET_MAGIC or tail != PARQUET_MAGIC:
        raise InvalidFileError(f"{path.name} ist keine gültige Parquet-Datei")


def fetch_source_info(session: requests.Session, url: str, cfg: Config) -> SourceInfo:
    return _retrying(cfg)(_head_once, session, url, cfg)


def download(session: requests.Session, url: str, dest: Path, cfg: Config) -> DownloadResult:
    result = _retrying(cfg)(_download_once, session, url, dest, cfg)
    validate_parquet(result.path)
    return result
