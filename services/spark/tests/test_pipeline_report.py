import json
from datetime import datetime, timezone
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from spark_jobs import pipeline_report
from spark_jobs.lake import LakePaths
from spark_jobs.pipeline_report import MissingReportError, build_report, run, step_reports

RUN_ID = "20240401T060000Z-abc123"


@pytest.fixture
def paths(tmp_path: Path) -> LakePaths:
    root = tmp_path.as_uri()
    return LakePaths(
        bronze=f"{root}/bronze",
        silver=f"{root}/silver",
        gold=f"{root}/gold",
        dataset="yellow_tripdata",
    )


def local(path: str) -> Path:
    return Path(path.removeprefix("file://"))


def put_reports(paths: LakePaths, skip: str | None = None) -> None:
    for number, (step, path) in enumerate(step_reports(paths, RUN_ID).items(), start=1):
        if step == skip:
            continue
        local(path).parent.mkdir(parents=True, exist_ok=True)
        report = {"run_id": RUN_ID, "counts": {"processed": 3}, "seconds": float(number)}
        local(path).write_text(json.dumps(report))


@pytest.mark.parametrize(
    ("period", "months"),
    [("2024Q1", ["2024-01", "2024-02", "2024-03"]), ("2024-03", ["2024-03"])],
)
def test_build_report(period: str, months: list[str]) -> None:
    reports = {
        "ingestion": {"counts": {"loaded": 1}, "seconds": 4.5},
        "silver-job": {"counts": {"processed": 1}, "seconds": 20.0},
    }
    finished_at = datetime(2024, 4, 1, 6, 2, 5, tzinfo=timezone.utc)

    report = build_report(RUN_ID, period, reports, finished_at)

    assert report["period"] == period
    assert report["months"] == months
    assert report["started_at"] == "2024-04-01T06:00:00+00:00"
    assert report["seconds"] == 125
    assert report["steps"] == [
        {"step": "ingestion", "counts": {"loaded": 1}, "seconds": 4.5},
        {"step": "silver-job", "counts": {"processed": 1}, "seconds": 20.0},
    ]


def test_run_writes_pipeline_report(spark: SparkSession, paths: LakePaths) -> None:
    put_reports(paths)

    report = run(spark, paths, RUN_ID, "2024-03")

    assert [step["step"] for step in report["steps"]] == ["ingestion", "silver-job", "gold-job"]
    assert [step["seconds"] for step in report["steps"]] == [1.0, 2.0, 3.0]
    assert report["months"] == ["2024-03"]
    assert json.loads(local(paths.pipeline_report(RUN_ID)).read_text()) == report


def test_run_writes_host_copy(spark: SparkSession, paths: LakePaths, tmp_path: Path) -> None:
    put_reports(paths)
    runs_dir = tmp_path / "runs"
    runs_dir.mkdir()

    report = run(spark, paths, RUN_ID, "2024-03", str(runs_dir))

    assert json.loads((runs_dir / f"{RUN_ID}.json").read_text()) == report


def test_unwritable_host_copy_does_not_fail(tmp_path: Path) -> None:
    pipeline_report.write_host_copy(str(tmp_path / "missing"), {"run_id": RUN_ID})

    assert not (tmp_path / "missing").exists()


def test_missing_step_report_fails(spark: SparkSession, paths: LakePaths) -> None:
    put_reports(paths, skip="gold-job")

    with pytest.raises(MissingReportError, match="gold-job"):
        run(spark, paths, RUN_ID, "2024Q1")
    assert not local(paths.pipeline_report(RUN_ID)).exists()


@pytest.mark.parametrize("period", ["", "2024/01"])
def test_main_rejects_invalid_period_before_spark_starts(
    monkeypatch: pytest.MonkeyPatch, period: str
) -> None:
    monkeypatch.setenv("INGEST_PERIOD", period)

    def no_session(_: str) -> None:
        raise AssertionError("Spark-Session darf bei Konfigurationsfehler nicht starten")

    monkeypatch.setattr(pipeline_report, "build_session", no_session)
    assert pipeline_report.main() == 2
