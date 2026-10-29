from datetime import date

import pytest

from orchestrator.quarters import latest_published_quarter, parse_quarter


@pytest.mark.parametrize(
    ("today", "expected"),
    [
        (date(2026, 10, 29), "2026Q2"),
        # September liegt erst etwa Ende November vor, Q3 also erst ab Dezember.
        (date(2026, 11, 1), "2026Q2"),
        (date(2026, 11, 30), "2026Q2"),
        (date(2026, 12, 1), "2026Q3"),
        (date(2027, 1, 10), "2026Q3"),
        (date(2027, 2, 1), "2026Q3"),
        (date(2027, 2, 28), "2026Q3"),
        (date(2027, 3, 1), "2026Q4"),
        (date(2027, 5, 1), "2026Q4"),
        (date(2027, 5, 31), "2026Q4"),
        (date(2027, 6, 1), "2027Q1"),
    ],
)
def test_latest_published_quarter(today: date, expected: str) -> None:
    assert latest_published_quarter(today) == expected


@pytest.mark.parametrize(
    ("today", "expected"),
    [(date(2026, 9, 30), "2026Q2"), (date(2026, 10, 1), "2026Q3")],
)
def test_latest_published_quarter_without_lag(today: date, expected: str) -> None:
    assert latest_published_quarter(today, lag_months=0) == expected


@pytest.mark.parametrize("value", ["2024Q1", "2024q1", "2024-Q1", " 2024Q1 "])
def test_parse_quarter_normalizes(value: str) -> None:
    assert parse_quarter(value) == "2024Q1"


@pytest.mark.parametrize("value", ["", "2024", "2024Q0", "2024Q5", "24Q1", "2024-01", "Q1 2024"])
def test_parse_quarter_rejects_invalid(value: str) -> None:
    with pytest.raises(ValueError, match="YYYYQn"):
        parse_quarter(value)
