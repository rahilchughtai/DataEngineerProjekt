"""Kommandozeile des Orchestrators: ein Quartal durch Ingestion, Job 1 und Job 2."""

import argparse
import json
import logging
import os
import secrets
import subprocess
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime
from functools import partial
from pathlib import Path
from typing import Any

from orchestrator.quarters import latest_published_quarter, parse_quarter

log = logging.getLogger("orchestrator")

# Exit-Code von ingestion, silver-job und gold-job, wenn Quelldaten für einen Monat fehlen.
INCOMPLETE = 3
# Wie GNU timeout: Der Schritt wurde nach Ablauf der Frist abgebrochen.
TIMEOUT = 124

DEFAULT_COMPOSE = "docker compose"
DEFAULT_STEP_TIMEOUT = 20 * 60
# So lange darf sich der Compose-Client nach SIGTERM beenden, bevor er SIGKILL bekommt.
TERMINATE_GRACE_SECONDS = 30
# Über diese Variable übernehmen die Jobs die Run-ID des Orchestrators.
RUN_ID_ENV = "PIPELINE_RUN_ID"

# Löst bei Überschreitung der Frist subprocess.TimeoutExpired aus.
Runner = Callable[[list[str]], int]


@dataclass(frozen=True)
class Step:
    name: str
    command: list[str]
    # Entfernt den Job-Container, falls er den abgebrochenen Compose-Client überlebt.
    cleanup: list[str] | None = None


def _setup_logging() -> None:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
        stream=sys.stdout,
    )


def _quarter_arg(value: str) -> str:
    try:
        return parse_quarter(value)
    except ValueError as err:
        raise argparse.ArgumentTypeError(str(err)) from err


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="orchestrator",
        description="Führt die Pipeline für ein Quartal aus: Ingestion, Bronze → Silver, "
        "Silver → Gold. Aufruf im Verzeichnis der docker-compose.yml.",
    )
    parser.add_argument(
        "--quarter",
        metavar="YYYYQn",
        type=_quarter_arg,
        help="Standard: neuestes veröffentlichtes Quartal",
    )
    parser.add_argument(
        "--compose",
        type=str.split,
        default=(os.environ.get("COMPOSE_CMD") or DEFAULT_COMPOSE).split(),
        help=f"Compose-Befehl (Standard: COMPOSE_CMD, sonst {DEFAULT_COMPOSE!r})",
    )
    parser.add_argument(
        "--report-dir",
        type=Path,
        default=Path("runs"),
        help="Verzeichnis für den Laufbericht (Standard: runs)",
    )
    parser.add_argument(
        "--step-timeout",
        type=_non_negative_seconds,
        default=DEFAULT_STEP_TIMEOUT,
        metavar="SECONDS",
        help=f"Frist je Schritt in Sekunden, 0 = keine (Standard: {DEFAULT_STEP_TIMEOUT})",
    )
    return parser.parse_args(argv)


def _non_negative_seconds(value: str) -> int:
    try:
        seconds = int(value)
    except ValueError:
        seconds = -1
    if seconds < 0:
        raise argparse.ArgumentTypeError(f"ganze Sekunden >= 0 erwartet, nicht {value!r}")
    return seconds


def new_run_id() -> str:
    """Nach Startzeit sortierbar; der Zufallsteil trennt Läufe, die in derselben Sekunde starten."""
    return f"{datetime.now(UTC):%Y%m%dT%H%M%SZ}-{secrets.token_hex(3)}"


def container_engine(compose: list[str]) -> str:
    return "podman" if "podman" in compose[0] else "docker"


def build_steps(compose: list[str], quarter: str, run_id: str) -> list[Step]:
    period = ["--quarter", quarter]
    engine = container_engine(compose)
    steps = [Step("build", [*compose, "--profile", "jobs", "build"])]
    for job in ("ingestion", "silver-job", "gold-job"):
        container = f"taxi-pipeline-{job}-{run_id}"
        steps.append(
            Step(
                job,
                [
                    *compose,
                    "run",
                    "--rm",
                    "--name",
                    container,
                    "-e",
                    f"{RUN_ID_ENV}={run_id}",
                    job,
                    *period,
                ],
                cleanup=[engine, "rm", "--force", container],
            )
        )
    return steps


def run_command(command: list[str], timeout: float | None = None) -> int:
    """Führt den Befehl aus; nach Ablauf der Frist wird er beendet und TimeoutExpired ausgelöst."""
    process = subprocess.Popen(command)
    try:
        return process.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        process.terminate()
        try:
            process.wait(timeout=TERMINATE_GRACE_SECONDS)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
        raise


def _cleanup(step: Step, runner: Runner) -> str:
    if step.cleanup is None:
        return "not_needed"
    log.info("step=%s cleanup=%s", step.name, " ".join(step.cleanup))
    try:
        code = runner(step.cleanup)
    except (OSError, subprocess.TimeoutExpired) as err:
        log.error("step=%s cleanup failed: %s", step.name, err)
        return "failed"
    # Ohne Container (schon mit --rm entfernt) melden docker und podman einen Fehler; das ist
    # hier das gewünschte Ergebnis und kein Problem.
    return "ok" if code == 0 else f"exit_code={code}"


def run_steps(steps: list[Step], runner: Runner) -> list[dict[str, Any]]:
    """Führt die Schritte nacheinander aus und bricht beim ersten Fehler ab."""
    results: list[dict[str, Any]] = []
    for step in steps:
        log.info("step=%s command=%s", step.name, " ".join(step.command))
        started_at = datetime.now(UTC).isoformat(timespec="seconds")
        started = time.monotonic()
        extra: dict[str, Any] = {}
        try:
            code = runner(step.command)
            status = "ok" if code == 0 else "failed"
        except OSError as err:
            log.error("step=%s %s", step.name, err)
            code, status = 127, "failed"
        except subprocess.TimeoutExpired as err:
            log.error(
                "step=%s Frist von %s s überschritten, Schritt abgebrochen", step.name, err.timeout
            )
            code, status = TIMEOUT, "timeout"
            extra["cleanup"] = _cleanup(step, runner)
        seconds = round(time.monotonic() - started, 2)
        level = logging.INFO if code == 0 else logging.ERROR
        log.log(
            level, "step=%s status=%s exit_code=%d seconds=%.1f", step.name, status, code, seconds
        )
        results.append(
            {
                "step": step.name,
                "status": status,
                "exit_code": code,
                "started_at": started_at,
                "seconds": seconds,
            }
            | extra
        )
        if code != 0:
            break
    return results


def write_report(report_dir: Path, report: dict[str, Any]) -> Path:
    report_dir.mkdir(parents=True, exist_ok=True)
    path = report_dir / f"{report['run_id']}.json"
    path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    return path


def main(argv: list[str] | None = None, runner: Runner | None = None) -> int:
    _setup_logging()
    args = _parse_args(argv)
    if runner is None:
        runner = partial(run_command, timeout=args.step_timeout or None)
    quarter = args.quarter or latest_published_quarter(date.today())
    compose = args.compose
    run_id = new_run_id()
    log.info(
        "run=%s quarter=%s compose=%s step_timeout=%s",
        run_id,
        quarter,
        " ".join(compose),
        args.step_timeout or "none",
    )

    started = time.monotonic()
    results = run_steps(build_steps(compose, quarter, run_id), runner)
    last = results[-1]
    code = last["exit_code"]
    report = {
        "run_id": run_id,
        "quarter": quarter,
        "compose": " ".join(compose),
        "step_timeout_seconds": args.step_timeout or None,
        "status": "ok" if code == 0 else "failed",
        "seconds": round(time.monotonic() - started, 2),
        "steps": results,
    }
    path = write_report(args.report_dir, report)

    if code == INCOMPLETE:
        log.warning(
            "quarter=%s step=%s Quelldaten unvollständig; bei ingestion heißt das, "
            "dass ein Monat noch nicht veröffentlicht ist",
            quarter,
            last["step"],
        )
    log.info(
        "run=%s finished status=%s seconds=%.1f report=%s",
        run_id,
        report["status"],
        report["seconds"],
        path,
    )
    return code
