"""Letzter Schritt von docker compose up: Laufbericht über Ingestion, Job 1 und Job 2.

Liest die Reports der drei Schritte unter der gemeinsamen Run-ID und schreibt sie zusammengefasst
nach gold/_runs/pipeline/<run_id>.json, mit RUNS_DIR zusätzlich als Kopie auf den Host. Die
Startzeit steckt in der Run-ID, die run-init vor allen anderen Schritten erzeugt. Die Laufzeit
umfasst deshalb auch Containerstarts und das Warten auf den Spark-Cluster, also die
End-to-End-Latenz des Laufs.
"""

import json
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession

from pipeline_common import env
from pipeline_common.periods import parse_period, period_from_env
from pipeline_common.reports import EXIT_CONFIG, EXIT_FAILED, EXIT_OK
from pipeline_common.run_id import read_run_id, started_at
from pipeline_common.timing import now_utc
from spark_jobs import job, lake
from spark_jobs.lake import LakePaths
from spark_jobs.session import build_session

log = logging.getLogger("pipeline_report")


class StepReportError(RuntimeError):
    """Ein Schritt des Laufs hat keinen verwendbaren Report geschrieben."""


def read_step_report(spark: SparkSession, step: str, path: str) -> dict[str, Any]:
    """counts und seconds aus dem Report eines Schritts."""
    if not lake.exists(spark, path):
        raise StepReportError(f"Report von {step} fehlt: {path}")
    try:
        report = json.loads(lake.read_text(spark, path))
        return {"counts": report["counts"], "seconds": report["seconds"]}
    except (ValueError, KeyError, TypeError) as err:
        raise StepReportError(f"Report von {step} unbrauchbar ({err!r}): {path}") from err


def step_reports(paths: LakePaths, run_id: str) -> dict[str, str]:
    """Compose-Service → Pfad seines Reports, in der Reihenfolge der Kette."""
    return {
        "ingestion": paths.bronze_run_report(run_id),
        "silver-job": paths.silver_run_report(run_id),
        "gold-job": paths.gold_run_report(run_id),
    }


def build_report(
    run_id: str, period: str, reports: dict[str, dict[str, Any]], finished_at: datetime
) -> dict[str, Any]:
    started = started_at(run_id)
    return {
        "run_id": run_id,
        "period": period,
        "months": [str(month) for month in parse_period(period)],
        "started_at": started.isoformat(),
        "finished_at": finished_at.isoformat(),
        "seconds": round((finished_at - started).total_seconds(), 2),
        "steps": [{"step": step} | report for step, report in reports.items()],
    }


def write_host_copy(runs_dir: str, report: dict[str, Any]) -> None:
    """Kopie nach runs/ auf dem Host; maßgeblich bleibt der Lake, daher nur eine Warnung."""
    if not runs_dir:
        return
    path = Path(runs_dir) / f"{report['run_id']}.json"
    try:
        path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    except OSError as err:
        log.warning("Laufbericht nicht nach %s geschrieben: %s", path, err)


def run(
    spark: SparkSession, paths: LakePaths, run_id: str, period: str, runs_dir: str = ""
) -> dict[str, Any]:
    reports = {
        step: read_step_report(spark, step, path)
        for step, path in step_reports(paths, run_id).items()
    }

    report = build_report(run_id, period, reports, now_utc())
    report_path = paths.pipeline_report(run_id)
    lake.write_text(spark, report_path, json.dumps(report, indent=2))
    write_host_copy(runs_dir, report)
    log.info(
        "run=%s period=%s seconds=%.0f report=%s", run_id, period, report["seconds"], report_path
    )
    return report


def load_config() -> tuple[str, LakePaths, str, str]:
    period = period_from_env()
    parse_period(period)
    return period, LakePaths.from_env(), read_run_id(), env.optional("RUNS_DIR", "")


def main() -> int:
    started = job.start("pipeline_report", build_session, log, load_config)
    if started is None:
        return EXIT_CONFIG
    (period, paths, run_id, runs_dir), spark = started

    try:
        run(spark, paths, run_id, period, runs_dir)
    except StepReportError as err:
        log.error("%s", err)
        return EXIT_FAILED
    finally:
        spark.stop()
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
