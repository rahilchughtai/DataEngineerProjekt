"""Gemeinsamer Ablauf der Monats-Jobs: Einstieg, Absicherung je Monat und Run-Report.

Die Jobs liefern nur noch die Verarbeitung eines Monats. Fehlende Quellen, Fehler einzelner
Monate, Report und Exit-Code behandeln beide gleich. Jeder Job übergibt dafür seinen Logger.
"""

import json
import logging
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Any

from pyspark.sql import DataFrame, SparkSession

from pipeline_common.logs import setup_logging
from pipeline_common.periods import Month, months_from_env
from pipeline_common.reports import EXIT_CONFIG, exit_code, run_report
from pipeline_common.run_id import read_run_id
from spark_jobs import lake
from spark_jobs.lake import LakePaths

# Status eines Monats, dessen Quelle (Bronze bzw. Silver) fehlt; führt zu Exit-Code 3.
MISSING = "missing"

Entry = dict[str, Any]
RunFn = Callable[[SparkSession, list[Month], LakePaths, str], Entry]


@contextmanager
def cached(df: DataFrame) -> Iterator[DataFrame]:
    cached_df = df.cache()
    try:
        yield cached_df
    finally:
        cached_df.unpersist()


def guarded_month(
    spark: SparkSession,
    month: Month,
    source: str,
    log: logging.Logger,
    process: Callable[..., Entry],
    *args: Any,
) -> Entry:
    """Ruft process(spark, month, entry, *args) auf, wenn die Quelle existiert.

    Ein Fehler landet als status=failed im Report, statt den Lauf abzubrechen.
    """
    entry: Entry = {"month": str(month), "source": source}
    try:
        if not lake.exists(spark, source):
            log.warning("month=%s status=missing source=%s", month, source)
            return entry | {"status": MISSING}
        return process(spark, month, entry, *args)
    except Exception as err:
        log.exception("month=%s status=failed", month)
        return entry | {"status": "failed", "error": str(err)}


def write_report(
    spark: SparkSession,
    started: float,
    results: list[Entry],
    *,
    run_id: str,
    dataset: str,
    job: str,
    report_path: str,
    log: logging.Logger,
) -> Entry:
    """Legt den Run-Report im Lake ab; started ist ein time.monotonic()-Wert vom Beginn."""
    report = run_report(run_id, dataset, started, results, job=job)
    lake.write_text(spark, report_path, json.dumps(report, indent=2))
    log.info("run=%s finished counts=%s report=%s", run_id, report["counts"], report_path)
    return report


def main(
    name: str, run: RunFn, build_session: Callable[[str], SparkSession], log: logging.Logger
) -> int:
    """Einstieg eines Monats-Jobs; Konfigurationsfehler fallen auf, bevor Spark startet."""
    setup_logging(quiet=("py4j",))
    try:
        months = months_from_env()
        paths = LakePaths.from_env()
        run_id = read_run_id()
        spark = build_session(name)
    except ValueError as err:
        log.error("%s", err)
        return EXIT_CONFIG

    log.info("run=%s months=%s", run_id, ",".join(map(str, months)))
    try:
        report = run(spark, months, paths, run_id)
    finally:
        spark.stop()
    return exit_code(report["counts"], MISSING)
