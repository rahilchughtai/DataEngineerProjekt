"""Kommandozeile des Ingestion-Service."""

import argparse
import logging
import os
import sys
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import requests

from ingestion import storage
from ingestion.config import Config, ConfigError
from ingestion.download import InvalidFileError, NotPublishedError, download, fetch_source_info
from ingestion.months import Month, parse_month, parse_period, quarter_months

log = logging.getLogger("ingestion")


def _setup_logging() -> None:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
        stream=sys.stdout,
    )
    logging.getLogger("botocore").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="ingestion",
        description="Lädt TLC-Monatsdateien und legt sie unverändert in Bronze ab.",
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--quarter", metavar="YYYYQn", help="alle Monate eines Quartals")
    group.add_argument("--month", metavar="YYYY-MM", help="ein einzelner Monat")
    parser.add_argument(
        "--force", action="store_true", help="auch laden, wenn Bronze schon aktuell ist"
    )
    return parser.parse_args(argv)


def resolve_months(args: argparse.Namespace) -> list[Month]:
    """CLI-Argumente haben Vorrang, sonst INGEST_PERIOD aus der Umgebung."""
    if args.quarter:
        return quarter_months(args.quarter)
    if args.month:
        return [parse_month(args.month)]
    period = os.environ.get("INGEST_PERIOD", "").strip()
    if not period:
        raise ValueError("Kein Zeitraum angegeben (--quarter/--month oder INGEST_PERIOD)")
    return parse_period(period)


def ingest_month(
    month: Month, cfg: Config, session: requests.Session, s3: Any, force: bool
) -> dict[str, Any]:
    file_name = f"{cfg.dataset}_{month}.parquet"
    url = f"{cfg.base_url}/{file_name}"
    key = storage.object_key(cfg.dataset, month)
    entry: dict[str, Any] = {"month": str(month), "source_url": url, "key": key}
    started = time.monotonic()

    source = fetch_source_info(session, url, cfg)
    metadata = storage.stored_metadata(s3, cfg.bronze_bucket, key)
    if not force and storage.is_current(metadata, source):
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


def main(argv: list[str] | None = None) -> int:
    _setup_logging()
    try:
        args = _parse_args(argv)
        months = resolve_months(args)
        cfg = Config.from_env()
    except (ValueError, ConfigError) as err:
        log.error("%s", err)
        return 2

    run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    log.info("run=%s months=%s", run_id, ",".join(map(str, months)))

    s3 = storage.make_client(cfg)
    storage.wait_for_bucket(s3, cfg.bronze_bucket)

    results: list[dict[str, Any]] = []
    started = time.monotonic()
    with requests.Session() as session:
        session.headers["User-Agent"] = "taxi-batch-pipeline-ingestion/0.1"
        for month in months:
            try:
                results.append(ingest_month(month, cfg, session, s3, args.force))
            except NotPublishedError as err:
                log.warning("month=%s status=not_published %s", month, err)
                results.append({"month": str(month), "status": "not_published"})
            except (InvalidFileError, requests.RequestException, OSError) as err:
                log.error("month=%s status=failed %s", month, err)
                results.append({"month": str(month), "status": "failed", "error": str(err)})
            except Exception as err:
                log.exception("month=%s status=failed", month)
                results.append({"month": str(month), "status": "failed", "error": str(err)})

    counts: dict[str, int] = {}
    for r in results:
        counts[r["status"]] = counts.get(r["status"], 0) + 1
    report = {
        "run_id": run_id,
        "dataset": cfg.dataset,
        "seconds": round(time.monotonic() - started, 2),
        "counts": counts,
        "months": results,
    }
    report_key = storage.write_run_report(s3, cfg.bronze_bucket, cfg.dataset, report)
    log.info(
        "run=%s finished counts=%s report=s3://%s/%s", run_id, counts, cfg.bronze_bucket, report_key
    )
    return exit_code(counts)


def exit_code(counts: dict[str, int]) -> int:
    """1 bei fehlgeschlagenen Monaten, 3 wenn ein Monat noch nicht veröffentlicht ist, sonst 0."""
    if counts.get("failed"):
        return 1
    # Wie silver-job: ein unvollständiger Zeitraum hält die Pipeline schon hier an.
    if counts.get("not_published"):
        return 3
    return 0
