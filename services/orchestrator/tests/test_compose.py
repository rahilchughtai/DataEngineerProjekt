import json
import os
import shutil
import subprocess
from pathlib import Path
from typing import Any

import pytest

from orchestrator.cli import DEFAULT_COMPOSE

ROOT = Path(__file__).resolve().parents[3]
JOBS = {"ingestion", "silver-job", "gold-job"}
INFRASTRUCTURE = {"seaweedfs", "lake-init", "spark-master", "spark-worker"}


@pytest.fixture(scope="module")
def compose() -> list[str]:
    command = (os.environ.get("COMPOSE_CMD") or DEFAULT_COMPOSE).split()
    if not shutil.which(command[0]):
        message = f"Compose-Konfigurationstests brauchen {command[0]} (COMPOSE_CMD)"
        # In CI (GitHub setzt CI=true) wäre ein stilles Überspringen eine Lücke.
        if os.environ.get("CI"):
            pytest.fail(message)
        pytest.skip(message)
    return [*command, "--env-file", str(ROOT / ".env.example")]


@pytest.mark.parametrize("with_jobs", [False, True], ids=["infrastructure", "jobs"])
def test_compose_execution_boundaries(compose: list[str], with_jobs: bool) -> None:
    profile = ["--profile", "jobs"] if with_jobs else []
    result = subprocess.run(
        [*compose, *profile, "config", "--format", "json"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    services: dict[str, Any] = json.loads(result.stdout)["services"]
    assert set(services) == INFRASTRUCTURE | (JOBS if with_jobs else set())
    if not with_jobs:
        return

    for job in JOBS:
        service = services[job]
        assert service["profiles"] == ["jobs"]
        dependencies = service["depends_on"]
        assert not JOBS.intersection(dependencies)
        assert dependencies["lake-init"]["condition"] == "service_completed_successfully"
        if job != "ingestion":
            assert dependencies["spark-worker"]["condition"] == "service_healthy"
