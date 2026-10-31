"""Gemeinsamer Ablauf der Spark-Jobs: Start, Absicherung je Monat und Run-Report.

Die Monats-Jobs liefern nur noch die Verarbeitung eines Monats. Fehlende Quellen, Fehler einzelner
Monate, Report und Exit-Code behandeln beide gleich. Jeder Job übergibt dafür seinen Logger.
"""

import json
import logging
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Any, TypeVar

from pyspark.sql import DataFrame, SparkSession

from pipeline_common.logs import setup_logging
from pipeline_common.periods import Month, months_from_env
from pipeline_common.reports import EXIT_CONFIG, exit_code, failed_entry, run_report
from pipeline_common.run_id import read_run_id
from spark_jobs import lake
from spark_jobs.lake import LakePaths

# Status eines Monats, dessen Quelle (Bronze bzw. Silver) fehlt; führt zu Exit-Code 3.
MISSING = "missing"

Entry = dict[str, Any]
RunFn = Callable[[SparkSession, list[Month], LakePaths, str], Entry]
T = TypeVar("T")


@contextmanager
def cached(df: DataFrame) -> Iterator[DataFrame]:
    cached_df = df.cache()
    try:
        yield cached_df
    finally:
        cached_df.unpersist()


def start(
    name: str,
    build_session: Callable[[str], SparkSession],
    log: logging.Logger,
    load: Callable[[], T],
) -> tuple[T, SparkSession] | None:
    """Liest die Konfiguration und startet erst danach Spark; None bei Konfigurationsfehlern.

    Der Aufrufer beendet sich dann mit EXIT_CONFIG, ohne dass ein Driver gestartet wurde.
    """
    setup_logging(quiet=("py4j",))
    try:
        config = load()
        return config, build_session(name)
    except ValueError as err:
        log.error("%s", err)
        return None


def guarded_month(
    spark: SparkSession,
    month: Month,
    source: str,
    log: logging.Logger,
    process: Callable[[str], Entry],
) -> Entry:
    """Verarbeitet einen Monat mit process(source), wenn die Quelle existiert.

    process liefert die Felder des Monats im Report. Ein Fehler landet als status=failed im
    Report, statt den Lauf abzubrechen.
    """
    entry: Entry = {"month": str(month), "source": source}
    try:
        if not lake.exists(spark, source):
            log.warning("month=%s status=missing source=%s", month, source)
            return entry | {"status": MISSING}
        return entry | process(source)
    except Exception as err:
        log.exception("month=%s status=failed", month)
        return failed_entry(entry, err)


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
    try:
        lake.write_text(spark, report_path, json.dumps(report, indent=2))
    except Exception:
        # Ohne Report gingen die Ergebnisse je Monat verloren; so stehen sie wenigstens im Log.
        log.error("run=%s Report nicht geschrieben: %s", run_id, json.dumps(report))
        raise
    log.info("run=%s finished counts=%s report=%s", run_id, report["counts"], report_path)
    return report


def main(
    name: str, run: RunFn, build_session: Callable[[str], SparkSession], log: logging.Logger
) -> int:
    """Einstieg eines Monats-Jobs; Konfigurationsfehler fallen auf, bevor Spark startet."""
    started = start(
        name, build_session, log, lambda: (months_from_env(), LakePaths.from_env(), read_run_id())
    )
    if started is None:
        return EXIT_CONFIG
    (months, paths, run_id), spark = started

    log.info("run=%s months=%s", run_id, ",".join(map(str, months)))
    try:
        report = run(spark, months, paths, run_id)
    finally:
        spark.stop()
    return exit_code(report["counts"], MISSING)
