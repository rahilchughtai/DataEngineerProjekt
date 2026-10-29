import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from orchestrator.cli import (
    TIMEOUT,
    Step,
    build_steps,
    main,
    new_run_id,
    run_command,
    run_steps,
)

# Beliebiger Befehl: Der Orchestrator macht keine Annahmen über die Container-Engine.
COMPOSE = ["engine", "compose"]
STEPS = ["build", "ingestion", "silver-job", "gold-job"]
RUN_ID = "20240401T000000Z-abc123"


class FakeRunner:
    """Gibt pro Service-Name einen Exit-Code zurück und merkt sich die Aufrufe."""

    def __init__(self, codes: dict[str, int] | None = None) -> None:
        self.codes = codes or {}
        self.calls: list[list[str]] = []

    def __call__(self, command: list[str]) -> int:
        self.calls.append(command)
        return next((code for name, code in self.codes.items() if name in command), 0)


@pytest.fixture(autouse=True)
def compose_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("COMPOSE_CMD", " ".join(COMPOSE))


def test_build_steps_order_and_period() -> None:
    steps = build_steps(COMPOSE, "2024Q1", RUN_ID)
    assert [s.name for s in steps] == STEPS
    assert steps[0].command == [*COMPOSE, "--profile", "jobs", "build"]
    for step in steps[1:]:
        assert step.command == [
            *COMPOSE,
            "run",
            "--rm",
            "-e",
            f"PIPELINE_RUN_ID={RUN_ID}",
            step.name,
            "--quarter",
            "2024Q1",
        ]


def test_new_run_id_is_unique_and_sortable() -> None:
    ids = [new_run_id() for _ in range(50)]
    assert len(set(ids)) == len(ids)
    assert all(re.fullmatch(r"\d{8}T\d{6}Z-[0-9a-f]{6}", run_id) for run_id in ids)


def _report(tmp_path: Path) -> dict[str, Any]:
    (report_file,) = tmp_path.glob("*.json")
    return json.loads(report_file.read_text(encoding="utf-8"))


def test_compose_from_environment(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("COMPOSE_CMD", " my-engine  compose ")
    runner = FakeRunner()
    main(["--quarter", "2024Q1", "--report-dir", str(tmp_path)], runner)
    assert all(call[:2] == ["my-engine", "compose"] for call in runner.calls)
    assert _report(tmp_path)["compose"] == "my-engine compose"


def test_compose_flag_overrides_environment(tmp_path: Path) -> None:
    runner = FakeRunner()
    argv = ["--quarter", "2024Q1", "--compose", "other-compose", "--report-dir", str(tmp_path)]
    main(argv, runner)
    assert all(call[0] == "other-compose" for call in runner.calls)
    assert _report(tmp_path)["compose"] == "other-compose"


@pytest.mark.parametrize(
    ("env", "flag"),
    [(None, []), ("  ", []), ("engine compose", ["--compose", ""])],
    ids=["unset", "blank", "empty-flag"],
)
def test_compose_command_is_required(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    env: str | None,
    flag: list[str],
) -> None:
    if env is None:
        monkeypatch.delenv("COMPOSE_CMD")
    else:
        monkeypatch.setenv("COMPOSE_CMD", env)
    runner = FakeRunner()
    with pytest.raises(SystemExit) as exc:
        main(["--quarter", "2024Q1", "--report-dir", str(tmp_path), *flag], runner)

    assert exc.value.code == 2
    assert "COMPOSE_CMD" in capsys.readouterr().err
    assert runner.calls == []
    assert not list(tmp_path.glob("*.json"))


@pytest.mark.parametrize("failed_step", range(len(STEPS)))
def test_run_steps_stops_at_first_failure(failed_step: int) -> None:
    steps = build_steps(COMPOSE, "2024Q1", RUN_ID)
    calls: list[list[str]] = []

    def runner(command: list[str]) -> int:
        calls.append(command)
        return 1 if command == steps[failed_step].command else 0

    results = run_steps(steps, runner)
    assert [r["step"] for r in results] == [s.name for s in steps[: failed_step + 1]]
    assert [r["status"] for r in results] == ["ok"] * failed_step + ["failed"]
    assert results[-1]["exit_code"] == 1
    assert calls == [s.command for s in steps[: failed_step + 1]]


def test_run_steps_missing_compose_binary() -> None:
    def runner(command: list[str]) -> int:
        raise FileNotFoundError(command[0])

    results = run_steps([Step("build", ["missing", "build"])], runner)
    assert results == [results[0] | {"status": "failed", "exit_code": 127}]


def test_run_command_returns_exit_code() -> None:
    assert run_command([sys.executable, "-c", "raise SystemExit(3)"], timeout=30) == 3


def test_run_command_stops_process_after_timeout() -> None:
    with pytest.raises(subprocess.TimeoutExpired):
        run_command([sys.executable, "-c", "import time; time.sleep(30)"], timeout=0.2)


class HangingRunner(FakeRunner):
    """Simuliert einen hängenden Schritt: Der Befehl mit `hang` überschreitet die Frist."""

    def __init__(self, hang: str) -> None:
        super().__init__()
        self.hang = hang

    def __call__(self, command: list[str]) -> int:
        self.calls.append(command)
        if self.hang in command:
            raise subprocess.TimeoutExpired(command, 1200)
        return 0


def test_timeout_stops_pipeline() -> None:
    steps = build_steps(COMPOSE, "2024Q1", RUN_ID)
    runner = HangingRunner("silver-job")

    results = run_steps(steps, runner)

    assert [r["step"] for r in results] == ["build", "ingestion", "silver-job"]
    assert results[-1] == results[-1] | {"status": "timeout", "exit_code": TIMEOUT}
    assert runner.calls == [s.command for s in steps[:3]]


def test_main_reports_timeout(tmp_path: Path) -> None:
    code = main(["--quarter", "2024Q1", "--report-dir", str(tmp_path)], HangingRunner("gold-job"))

    assert code == TIMEOUT
    report = _report(tmp_path)
    assert report["status"] == "failed"
    assert report["steps"][-1]["status"] == "timeout"
    assert report["step_timeout_seconds"] == 1200


@pytest.mark.parametrize(("value", "expected"), [("0", None), ("90", 90)])
def test_step_timeout_option(tmp_path: Path, value: str, expected: int | None) -> None:
    argv = ["--quarter", "2024Q1", "--report-dir", str(tmp_path), "--step-timeout", value]
    main(argv, FakeRunner())
    assert _report(tmp_path)["step_timeout_seconds"] == expected


@pytest.mark.parametrize("value", ["-1", "abc", "1.5"])
def test_step_timeout_rejects_invalid_values(tmp_path: Path, value: str) -> None:
    argv = ["--quarter", "2024Q1", "--report-dir", str(tmp_path), "--step-timeout", value]
    with pytest.raises(SystemExit) as exc:
        main(argv, FakeRunner())
    assert exc.value.code == 2


def test_main_writes_report(tmp_path: Path) -> None:
    runner = FakeRunner()
    code = main(["--quarter", "2024Q1", "--report-dir", str(tmp_path)], runner)

    assert code == 0
    (report_file,) = tmp_path.glob("*.json")
    report = json.loads(report_file.read_text(encoding="utf-8"))
    assert report["quarter"] == "2024Q1"
    assert report["status"] == "ok"
    assert [s["step"] for s in report["steps"]] == STEPS
    assert report["run_id"] == report_file.stem
    job_calls = runner.calls[1:]
    assert len(job_calls) == 3
    assert all(f"PIPELINE_RUN_ID={report['run_id']}" in call for call in job_calls)


def test_main_passes_on_exit_code_of_failed_step(tmp_path: Path) -> None:
    runner = FakeRunner({"ingestion": 3})
    code = main(["--quarter", "2026Q3", "--report-dir", str(tmp_path)], runner)

    assert code == 3
    (report_file,) = tmp_path.glob("*.json")
    report = json.loads(report_file.read_text(encoding="utf-8"))
    assert report["status"] == "failed"
    assert report["steps"][-1] == report["steps"][-1] | {"step": "ingestion", "exit_code": 3}
    assert len(runner.calls) == 2


@pytest.mark.parametrize("quarter", ["2024Q5", "2024-01", "foo"])
def test_main_rejects_invalid_quarter_before_any_step(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], quarter: str
) -> None:
    runner = FakeRunner()
    with pytest.raises(SystemExit) as exc:
        main(["--quarter", quarter, "--report-dir", str(tmp_path)], runner)

    assert exc.value.code == 2
    assert "YYYYQn" in capsys.readouterr().err
    assert runner.calls == []
    assert not list(tmp_path.glob("*.json"))


def test_main_normalizes_quarter(tmp_path: Path) -> None:
    runner = FakeRunner()
    main(["--quarter", "2024-q1", "--report-dir", str(tmp_path)], runner)

    assert all(call[-2:] == ["--quarter", "2024Q1"] for call in runner.calls[1:])
    assert _report(tmp_path)["quarter"] == "2024Q1"
