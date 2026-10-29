"""Fixtures des End-to-End-Tests.

Der Test startet die Pipeline wie die README mit docker compose up; nur COMPOSE_FILE ergänzt den
Override mit Mock-Quelle und eigenem Projekt, INGEST_PERIOD wählt den Zeitraum je Lauf. Die
teuren Läufe sind Session-Fixtures und laufen je einmal, in fester Reihenfolge:
first_run → second_run (Wiederholung) → month_run (ein Monat) → unpublished_run.
"""

import json
import logging
import os
import socket
import subprocess
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from lake import ROOT, Container, Lake, Run, Settings

# Eingaben des Tests: ein Quartal mit Fixtures, ein Monat daraus und ein Quartal, für das der Mock
# nur 403 liefert.
QUARTER = "2024Q1"
MONTH = "2024-03"
UNPUBLISHED_QUARTER = "2024Q2"
# Frist für compose up und compose wait je Lauf, in Sekunden.
RUN_TIMEOUT = 600

E2E_DIR = Path(__file__).resolve().parent
COMPOSE_FILES = [ROOT / "docker-compose.yml", E2E_DIR / "compose.e2e.yml"]
# Die .env im Repo-Root, ergänzt um die Mock-Quelle; die spätere Datei gewinnt.
ENV_FILES = [ROOT / ".env", E2E_DIR / "e2e.env"]
# E2E_KEEP=1 lässt den Stack nach dem Test stehen (CI sammelt danach Logs und räumt selbst auf).
KEEP_STACK = os.environ.get("E2E_KEEP") == "1"

log = logging.getLogger("e2e")


@pytest.fixture(scope="session")
def compose() -> Iterator[list[str]]:
    """Compose-Befehl der eigenen Container-Engine; alle Aufrufe im Repo-Root mit dem Override."""
    command = os.environ.get("COMPOSE_CMD", "").split()
    if not command:
        pytest.fail("COMPOSE_CMD fehlt: den Compose-Befehl der eigenen Container-Engine setzen")
    with pytest.MonkeyPatch.context() as patch:
        patch.chdir(ROOT)
        patch.setenv("COMPOSE_FILE", os.pathsep.join(map(str, COMPOSE_FILES)))
        patch.setenv("COMPOSE_ENV_FILES", ",".join(map(str, ENV_FILES)))
        yield command


def _compose(
    command: list[str], *args: str, period: str | None = None, capture: bool = True
) -> subprocess.CompletedProcess[str]:
    # Die Umgebung des Aufrufs hat bei der Interpolation Vorrang vor den Env-Dateien.
    env = os.environ | ({"INGEST_PERIOD": period} if period else {})
    return subprocess.run(
        [*command, *args],
        capture_output=capture,
        text=True,
        check=False,
        env=env,
        timeout=RUN_TIMEOUT,
    )


@pytest.fixture(scope="session")
def config(compose: list[str]) -> dict[str, Any]:
    result = _compose(compose, "config", "--format", "json")
    if result.returncode != 0:
        pytest.fail(f"compose config fehlgeschlagen (cp .env.example .env?):\n{result.stderr}")
    return json.loads(result.stdout)


@pytest.fixture(scope="session")
def settings(config: dict[str, Any]) -> Settings:
    return Settings.from_compose(config)


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
    """Leerer Data Lake und frisch gebaute Images zu Beginn; die Läufe starten die Container."""
    _compose(compose, "down", "--volumes", "--remove-orphans").check_returncode()
    busy = _busy(settings.host_ports)
    if busy:
        pytest.fail(f"Ports {busy} belegt: die normale Umgebung vorher mit compose down stoppen")
    _compose(compose, "build", capture=False).check_returncode()
    yield
    if KEEP_STACK:
        log.info("E2E_KEEP=1: Stack bleibt bestehen")
        return
    _compose(compose, "down", "--volumes", "--remove-orphans")


def _containers(compose: list[str]) -> dict[str, Container]:
    result = _compose(compose, "ps", "--all", "--format", "json")
    result.check_returncode()
    # Je nach Compose-Version ein JSON-Array oder ein Objekt pro Zeile.
    output = result.stdout.strip()
    entries = json.loads(output) if output.startswith("[") else map(json.loads, output.splitlines())
    return {
        entry["Service"]: Container(entry["State"], int(entry["ExitCode"])) for entry in entries
    }


def _run_id(compose: list[str]) -> str:
    """run-init gibt die Run-ID aus; ein neu gestarteter Container hängt seine Ausgabe an."""
    result = _compose(compose, "logs", "--no-log-prefix", "--no-color", "run-init")
    result.check_returncode()
    return result.stdout.split()[-1]


def _run(compose: list[str], lake: Lake, period: str) -> Run:
    """Ein Lauf wie docker compose up, nur abgekoppelt, damit der Test das Ende abwarten kann."""
    log.info("compose up INGEST_PERIOD=%s", period)
    # up -d wartet die depends_on-Bedingungen ab und scheitert, sobald ein Schritt fehlschlägt.
    code = _compose(compose, "up", "--detach", period=period, capture=False).returncode
    if code == 0:
        code = _compose(compose, "wait", "run-report", period=period).returncode
    return Run(code, _containers(compose), _run_id(compose), lake.state())


@pytest.fixture(scope="session")
def first_run(stack: None, compose: list[str], lake: Lake) -> Run:
    return _run(compose, lake, QUARTER)


@pytest.fixture(scope="session")
def second_run(first_run: Run, compose: list[str], lake: Lake) -> Run:
    if first_run.exit_code != 0:
        pytest.fail("erster Lauf fehlgeschlagen, Wiederholung nicht aussagekräftig")
    return _run(compose, lake, QUARTER)


@pytest.fixture(scope="session")
def month_run(second_run: Run, compose: list[str], lake: Lake) -> Run:
    return _run(compose, lake, MONTH)


@pytest.fixture(scope="session")
def unpublished_run(month_run: Run, compose: list[str], lake: Lake) -> Run:
    return _run(compose, lake, UNPUBLISHED_QUARTER)
