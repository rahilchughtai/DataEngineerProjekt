import pytest

from spark_jobs.runtime import exit_code


@pytest.mark.parametrize(
    ("counts", "expected"),
    [
        ({"processed": 3}, 0),
        ({"processed": 2, "missing": 1}, 3),
        ({"missing": 3}, 3),
        ({}, 3),
        ({"processed": 2, "failed": 1}, 1),
        ({"failed": 1}, 1),
    ],
)
def test_exit_code(counts: dict[str, int], expected: int) -> None:
    assert exit_code({"counts": counts}, "Bronze") == expected
