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
from pipeline_common.reports import exit_code, run_report
from pipeline_common.run_id import read_run_id

log = logging.getLogger("ingestion")


def ingest_month(month: Month, cfg: Config, session: requests.Session, s3: Any) -> dict[str, Any]:
    file_name = f"{cfg.dataset}_{month}.parquet"
    url = f"{cfg.base_url}/{file_name}"
    key = storage.object_key(cfg.dataset, month)
    entry: dict[str, Any] = {"month": str(month), "source_url": url, "key": key}
    started = time.monotonic()

    source = fetch_source_info(session, url, cfg)
    metadata = storage.stored_metadata(s3, cfg.bronze_bucket, key)
    if storage.is_current(metadata, source):
        log.info("month=%s status=skipped reason=unchanged key=%s", month, key)
        return entry | {"status": "skipped", "seconds": round(time.monotonic() - started, 2)}

    with tempfile.TemporaryDirectory(prefix="ingest-") as tmp:
        result = download(session, url, Path(tmp) / file_name, cfg)
        log.info(
            "month=%s downloaded bytes=%d seconds=%.1f sha256=%s",
            month,
            result.size,
            result.seconds,
            result.sha256,
        )
        upload_started = time.monotonic()
        storage.upload(s3, cfg.bronze_bucket, key, result, source)
        upload_seconds = time.monotonic() - upload_started

    total = time.monotonic() - started
    log.info("month=%s status=loaded key=%s seconds=%.1f", month, key, total)
    return entry | {
        "status": "loaded",
        "bytes": result.size,
        "sha256": result.sha256,
        "download_seconds": round(result.seconds, 2),
        "upload_seconds": round(upload_seconds, 2),
        "seconds": round(total, 2),
    }


def main() -> int:
    setup_logging(quiet=("botocore", "urllib3"))
    try:
        months = months_from_env()
        cfg = Config.from_env()
        run_id = read_run_id()
    except ValueError as err:
        log.error("%s", err)
        return 2

    log.info("run=%s months=%s", run_id, ",".join(map(str, months)))

    s3 = storage.make_client(cfg)

    results: list[dict[str, Any]] = []
    started = time.monotonic()
    with requests.Session() as session:
        session.headers["User-Agent"] = "taxi-batch-pipeline-ingestion/0.1"
        for month in months:
            try:
                results.append(ingest_month(month, cfg, session, s3))
            except NotPublishedError as err:
                log.warning("month=%s status=not_published %s", month, err)
                results.append({"month": str(month), "status": "not_published"})
            except Exception as err:
                log.exception("month=%s status=failed", month)
                results.append({"month": str(month), "status": "failed", "error": str(err)})

    report = run_report(run_id, cfg.dataset, started, results)
    counts = report["counts"]
    report_key = storage.write_run_report(s3, cfg.bronze_bucket, cfg.dataset, report)
    log.info(
        "run=%s finished counts=%s report=s3://%s/%s", run_id, counts, cfg.bronze_bucket, report_key
    )
    return exit_code(counts, "not_published")
