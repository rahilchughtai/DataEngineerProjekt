import pytest

from ingestion.months import Month, month_range, parse_month, quarter_months


def test_parse_month() -> None:
    assert parse_month(" 2024-03 ") == Month(2024, 3)


@pytest.mark.parametrize("value", ["2024-3", "2024/03", "24-03", "2024-13", ""])
def test_parse_month_rejects_invalid(value: str) -> None:
    with pytest.raises(ValueError):
        parse_month(value)


def test_month_validates_range() -> None:
    with pytest.raises(ValueError):
        Month(2024, 0)


def test_month_str_is_zero_padded() -> None:
    assert str(Month(2024, 1)) == "2024-01"


def test_next_rolls_over_year() -> None:
    assert Month(2023, 12).next() == Month(2024, 1)


def test_month_range_across_year() -> None:
    months = month_range(Month(2023, 11), Month(2024, 2))
    assert [str(m) for m in months] == ["2023-11", "2023-12", "2024-01", "2024-02"]


def test_month_range_single_month() -> None:
    assert month_range(Month(2024, 5), Month(2024, 5)) == [Month(2024, 5)]


def test_month_range_rejects_reversed() -> None:
    with pytest.raises(ValueError):
        month_range(Month(2024, 2), Month(2024, 1))


@pytest.mark.parametrize(
    ("value", "first", "last"),
    [
        ("2024Q1", 1, 3),
        ("2024Q2", 4, 6),
        ("2024-Q3", 7, 9),
        ("2024q4", 10, 12),
    ],
)
def test_quarter_months(value: str, first: int, last: int) -> None:
    months = quarter_months(value)
    assert months == [Month(2024, m) for m in range(first, last + 1)]


@pytest.mark.parametrize("value", ["2024Q5", "2024Q0", "Q1", "2024-03"])
def test_quarter_months_rejects_invalid(value: str) -> None:
    with pytest.raises(ValueError):
        quarter_months(value)
