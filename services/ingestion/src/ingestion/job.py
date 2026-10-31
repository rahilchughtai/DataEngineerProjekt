"""Einstieg des Ingestion-Service: lädt die Monate aus INGEST_PERIOD nach Bronze."""

import logging
import tempfile
import time
from pathlib import Path
from typing import Any

import requests

from ingestion import storage
from ingestion.config import Config
from ingestion.download import NotPublishedError, download, fetch_source_info
from pipeline_common.logs import setup_logging
from pipeline_common.periods import Month, months_from_env
from pipeline_common.reports import ExitCode, exit_code, failed_entry, run_report
from pipeline_common.run_id import read_run_id
from pipeline_common.timing import elapsed

log = logging.getLogger("ingestion")

# Status eines Monats, den die TLC (noch) nicht veröffentlicht hat; führt zu Exit-Code 3.
NOT_PUBLISHED = "not_published"


def month_entry(month: Month, cfg: Config) -> dict[str, Any]:
    """Quelle und Ziel eines Monats; stehen auch bei einem Fehler im Report."""
    return {
        "month": str(month),
        "source_url": f"{cfg.base_url}/{storage.file_name(cfg.dataset, month)}",
        "key": storage.object_key(cfg.dataset, month),
    }


def ingest_month(month: Month, cfg: Config, session: requests.Session, s3: Any) -> dict[str, Any]:
    entry = month_entry(month, cfg)
    url, key = entry["source_url"], entry["key"]
    started = time.monotonic()

    source = fetch_source_info(session, url, cfg)
    metadata = storage.stored_metadata(s3, cfg.bronze_bucket, key)
    if storage.is_current(metadata, source):
        log.info("month=%s status=skipped reason=unchanged key=%s", month, key)
        return entry | {"status": "skipped", "seconds": elapsed(started)}

    with tempfile.TemporaryDirectory(prefix="ingest-") as tmp:
        result = download(session, url, Path(tmp) / storage.file_name(cfg.dataset, month), cfg)
        log.info(
            "month=%s downloaded bytes=%d seconds=%.1f sha256=%s",
            month,
            result.size,
            result.seconds,
            result.sha256,
        )
        upload_started = time.monotonic()
        storage.upload(s3, cfg.bronze_bucket, key, result, source)
        upload_seconds = elapsed(upload_started)

    total = elapsed(started)
    log.info("month=%s status=loaded key=%s seconds=%.1f", month, key, total)
    return entry | {
        "status": "loaded",
        "bytes": result.size,
        "sha256": result.sha256,
        "download_seconds": result.seconds,
        "upload_seconds": upload_seconds,
        "seconds": total,
    }


def main() -> int:
    setup_logging(quiet=("botocore", "urllib3"))
    try:
        months = months_from_env()
        cfg = Config.from_env()
        run_id = read_run_id()
    except ValueError as err:
        log.error("%s", err)
        return ExitCode.CONFIG

    log.info("run=%s months=%s", run_id, ",".join(map(str, months)))

    s3 = storage.make_client(cfg)

    results: list[dict[str, Any]] = []
    started = time.monotonic()
    with requests.Session() as session:
        session.headers["User-Agent"] = "taxi-batch-pipeline-ingestion/0.1"
        # Unkomprimiert laden: Größenprüfung und sha256 beziehen sich auf die Bytes der Quelle.
        session.headers["Accept-Encoding"] = "identity"
        for month in months:
            entry = month_entry(month, cfg)
            try:
                results.append(ingest_month(month, cfg, session, s3))
            except NotPublishedError as err:
                log.warning("month=%s status=not_published %s", month, err)
                results.append(entry | {"status": NOT_PUBLISHED})
            except Exception as err:
                log.exception("month=%s status=failed", month)
                results.append(failed_entry(entry, err))

    report = run_report(run_id, cfg.dataset, started, results)
    counts = report["counts"]
    report_key = storage.write_run_report(s3, cfg.bronze_bucket, cfg.dataset, report)
    log.info(
        "run=%s finished counts=%s report=s3://%s/%s", run_id, counts, cfg.bronze_bucket, report_key
    )
    return exit_code(counts, NOT_PUBLISHED)
