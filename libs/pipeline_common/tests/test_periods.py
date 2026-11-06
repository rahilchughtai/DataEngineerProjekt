import pytest

from pipeline_common.periods import months, partition


@pytest.mark.parametrize(
    ("period", "expected"),
    [
        ("2024Q1", [(2024, 1), (2024, 2), (2024, 3)]),
        ("2024-Q4", [(2024, 10), (2024, 11), (2024, 12)]),
        ("2024q2", [(2024, 4), (2024, 5), (2024, 6)]),
        (" 2024-03 ", [(2024, 3)]),
    ],
)
def test_months(period: str, expected: list[tuple[int, int]]) -> None:
    assert months(period) == expected


@pytest.mark.parametrize(
    "period", ["2024Q5", "2024Q0", "2024-13", "2024-00", "2024/01", "24-01", ""]
)
def test_months_rejects_invalid(period: str) -> None:
    with pytest.raises(ValueError, match="INGEST_PERIOD"):
        months(period)


def test_partition_is_zero_padded() -> None:
    assert partition(2024, 3) == "year=2024/month=03"
