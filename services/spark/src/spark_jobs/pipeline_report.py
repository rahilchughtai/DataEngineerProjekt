"""Letzter Schritt von docker compose up: Laufbericht über Ingestion, Job 1 und Job 2.

Liest die Reports der drei Schritte unter der gemeinsamen Run-ID und schreibt sie zusammengefasst
nach gold/_runs/pipeline/<run_id>.json. Die Startzeit steckt in der Run-ID, die run-init vor allen
anderen Schritten erzeugt. Die Laufzeit umfasst deshalb auch Containerstarts und das Warten auf
den Spark-Cluster, also die End-to-End-Latenz des Laufs.
"""

import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any

from pyspark.sql import SparkSession

from pipeline_common.logs import setup_logging
from pipeline_common.periods import parse_period, period_from_env
from pipeline_common.run_id import read_run_id
from spark_jobs import lake
from spark_jobs.lake import LakePaths
from spark_jobs.session import build_session

log = logging.getLogger("pipeline_report")


class MissingReportError(RuntimeError):
    """Ein Schritt des Laufs hat keinen Report geschrieben."""


def step_reports(paths: LakePaths, run_id: str) -> dict[str, str]:
    """Compose-Service → Pfad seines Reports, in der Reihenfolge der Kette."""
    return {
        "ingestion": paths.bronze_run_report(run_id),
        "silver-job": paths.run_report(run_id),
        "gold-job": paths.gold_run_report(run_id),
    }


def run_started_at(run_id: str) -> datetime:
    return datetime.strptime(run_id[:16], "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)


def build_report(
    run_id: str, period: str, reports: dict[str, dict[str, Any]], finished_at: datetime
) -> dict[str, Any]:
    started_at = run_started_at(run_id)
    return {
        "run_id": run_id,
        "period": period,
        "months": [str(month) for month in parse_period(period)],
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "seconds": round((finished_at - started_at).total_seconds(), 2),
        "steps": [
            {"step": step, "counts": report["counts"], "seconds": report["seconds"]}
            for step, report in reports.items()
        ],
    }


def run(spark: SparkSession, paths: LakePaths, run_id: str, period: str) -> dict[str, Any]:
    reports = {}
    for step, path in step_reports(paths, run_id).items():
        if not lake.exists(spark, path):
            raise MissingReportError(f"Report von {step} fehlt: {path}")
        reports[step] = json.loads(lake.read_text(spark, path))

    finished_at = datetime.now(timezone.utc).replace(microsecond=0)
    report = build_report(run_id, period, reports, finished_at)
    report_path = paths.pipeline_report(run_id)
    lake.write_text(spark, report_path, json.dumps(report, indent=2))
    log.info(
        "run=%s period=%s seconds=%.0f report=%s", run_id, period, report["seconds"], report_path
    )
    return report


def main() -> int:
    setup_logging(quiet=("py4j",))
    try:
        period = period_from_env()
        parse_period(period)
        paths = LakePaths.from_env()
        run_id = read_run_id()
        spark = build_session("pipeline_report")
    except ValueError as err:
        log.error("%s", err)
        return 2

    try:
        run(spark, paths, run_id, period)
    except MissingReportError as err:
        log.error("%s", err)
        return 1
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
