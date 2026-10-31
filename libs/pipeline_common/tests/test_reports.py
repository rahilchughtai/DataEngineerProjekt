import time

import pytest

from pipeline_common.reports import (
    MAX_ERROR_LENGTH,
    count_statuses,
    exit_code,
    failed_entry,
    run_report,
)


def test_count_statuses() -> None:
    results = [{"status": "loaded"}, {"status": "loaded"}, {"status": "failed"}]
    assert count_statuses(results) == {"loaded": 2, "failed": 1}


def test_run_report_with_and_without_job() -> None:
    results = [{"month": "2024-01", "status": "processed"}]
    started = time.monotonic()
    report = run_report("run", "yellow_tripdata", started, results, job="bronze_to_silver")
    assert report["job"] == "bronze_to_silver"
    assert report["counts"] == {"processed": 1}
    assert report["months"] == results
    assert "job" not in run_report("run", "yellow_tripdata", started, results)


@pytest.mark.parametrize(
    ("counts", "expected"),
    [
        ({"loaded": 3}, 0),
        ({"loaded": 2, "not_published": 1}, 3),
        ({"loaded": 2, "failed": 1}, 1),
        ({"failed": 1, "not_published": 1}, 1),
    ],
)
def test_exit_code(counts: dict[str, int], expected: int) -> None:
    assert exit_code(counts, "not_published") == expected


def test_failed_entry_names_error_type() -> None:
    entry = failed_entry({"month": "2024-01"}, TimeoutError())
    assert entry == {"month": "2024-01", "status": "failed", "error": "TimeoutError: "}


def test_failed_entry_truncates_long_errors() -> None:
    entry = failed_entry({}, RuntimeError("x" * 5000))
    assert len(entry["error"]) == MAX_ERROR_LENGTH
