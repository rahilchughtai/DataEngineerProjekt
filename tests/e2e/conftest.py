"""Fixtures des End-to-End-Tests.

Der Test ruft den echten Orchestrator auf (orchestrator.cli.main), so wie ihn die README
beschreibt; nur COMPOSE_FILE ergänzt den Override mit Mock-Quelle und eigenem Projekt. Die
teuren Läufe sind Session-Fixtures und laufen je einmal, in fester Reihenfolge:
first_run → second_run (Wiederholung) → unpublished_run.
"""

import json
import logging
import os
import socket
import subprocess
import time
from collections.abc import Iterator
from pathlib import Path

import pytest

from lake import ROOT, Lake, Run, Settings
from orchestrator import cli as orchestrator

# Eingaben des Tests: ein Quartal mit Fixtures und eines, für das der Mock nur 403 liefert.
QUARTER = "2024Q1"
UNPUBLISHED_QUARTER = "2024Q2"
STEP_TIMEOUT = "300"

E2E_DIR = Path(__file__).resolve().parent
COMPOSE_FILES = [ROOT / "docker-compose.yml", E2E_DIR / "compose.e2e.yml"]
# Die .env im Repo-Root, ergänzt um die Mock-Quelle; die spätere Datei gewinnt.
ENV_FILES = [ROOT / ".env", E2E_DIR / "e2e.env"]
# E2E_KEEP=1 lässt den Stack nach dem Test stehen (CI sammelt danach Logs und räumt selbst auf).
KEEP_STACK = os.environ.get("E2E_KEEP") == "1"

log = logging.getLogger("e2e")


@pytest.fixture(scope="session")
def compose() -> Iterator[list[str]]:
    """Compose-Befehl wie beim Orchestrator; alle Aufrufe im Repo-Root mit dem E2E-Override."""
    command = os.environ.get("COMPOSE_CMD", "").split()
    if not command:
        pytest.fail("COMPOSE_CMD fehlt: den Compose-Befehl der eigenen Container-Engine setzen")
    with pytest.MonkeyPatch.context() as patch:
        patch.chdir(ROOT)
        patch.setenv("COMPOSE_FILE", os.pathsep.join(map(str, COMPOSE_FILES)))
        patch.setenv("COMPOSE_ENV_FILES", ",".join(map(str, ENV_FILES)))
        yield command


def _compose(command: list[str], *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [*command, "--profile", "jobs", *args], capture_output=True, text=True, check=False
    )


@pytest.fixture(scope="session")
def settings(compose: list[str]) -> Settings:
    result = _compose(compose, "config", "--format", "json")
    if result.returncode != 0:
        pytest.fail(f"compose config fehlgeschlagen (cp .env.example .env?):\n{result.stderr}")
    return Settings.from_compose(json.loads(result.stdout))


@pytest.fixture(scope="session")
def lake(settings: Settings) -> Lake:
    return Lake(settings)


def _busy(ports: list[int], wait_seconds: float = 30) -> list[int]:
    # Podman gibt Port-Weiterleitungen nach compose down erst mit etwas Verzögerung frei.
    deadline = time.monotonic() + wait_seconds
    while True:
        busy = []
        for port in ports:
            with socket.socket() as sock:
                sock.settimeout(1)
                if sock.connect_ex(("127.0.0.1", port)) == 0:
                    busy.append(port)
        if not busy or time.monotonic() >= deadline:
            return busy
        time.sleep(1)


@pytest.fixture(scope="session")
def stack(compose: list[str], settings: Settings) -> Iterator[None]:
    """Leerer Data Lake zu Beginn; die Container startet der Orchestrator selbst."""
    _compose(compose, "down", "--volumes", "--remove-orphans").check_returncode()
    busy = _busy(settings.host_ports)
    if busy:
        pytest.fail(f"Ports {busy} belegt: die normale Umgebung vorher mit compose down stoppen")
    yield
    if KEEP_STACK:
        log.info("E2E_KEEP=1: Stack bleibt bestehen")
        return
    _compose(compose, "down", "--volumes", "--remove-orphans")


def _run(quarter: str, lake: Lake, factory: pytest.TempPathFactory) -> Run:
    report_dir = factory.mktemp(f"runs-{quarter}")
    code = orchestrator.main(
        ["--quarter", quarter, "--report-dir", str(report_dir), "--step-timeout", STEP_TIMEOUT]
    )
    (report,) = report_dir.glob("*.json")
    return Run(code, json.loads(report.read_text(encoding="utf-8")), lake.state())


@pytest.fixture(scope="session")
def first_run(stack: None, lake: Lake, tmp_path_factory: pytest.TempPathFactory) -> Run:
    return _run(QUARTER, lake, tmp_path_factory)


@pytest.fixture(scope="session")
def second_run(first_run: Run, lake: Lake, tmp_path_factory: pytest.TempPathFactory) -> Run:
    if first_run.exit_code != 0:
        pytest.fail("erster Lauf fehlgeschlagen, Wiederholung nicht aussagekräftig")
    return _run(QUARTER, lake, tmp_path_factory)


@pytest.fixture(scope="session")
def unpublished_run(second_run: Run, lake: Lake, tmp_path_factory: pytest.TempPathFactory) -> Run:
    return _run(UNPUBLISHED_QUARTER, lake, tmp_path_factory)
