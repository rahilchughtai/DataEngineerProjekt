import pytest

from pipeline_common.periods import Month, months_from_env, parse_period


def test_month_validates_range() -> None:
    with pytest.raises(ValueError):
        Month(2024, 0)


def test_month_str_is_zero_padded() -> None:
    assert str(Month(2024, 1)) == "2024-01"


def test_month_partition_is_zero_padded() -> None:
    assert Month(2024, 3).partition == "year=2024/month=03"
    assert Month(2024, 3).partition_values == {"year": "2024", "month": "03"}


@pytest.mark.parametrize(
    ("month", "expected"),
    [(Month(2024, 1), Month(2024, 2)), (Month(2024, 12), Month(2025, 1))],
)
def test_next_month(month: Month, expected: Month) -> None:
    assert month.next() == expected


@pytest.mark.parametrize(
    ("value", "first", "last"),
    [
        ("2024Q1", 1, 3),
        ("2024Q2", 4, 6),
        ("2024-Q3", 7, 9),
        ("2024q4", 10, 12),
    ],
)
def test_parse_period_quarter(value: str, first: int, last: int) -> None:
    assert parse_period(value) == [Month(2024, m) for m in range(first, last + 1)]


def test_parse_period_month() -> None:
    assert parse_period(" 2024-03 ") == [Month(2024, 3)]


@pytest.mark.parametrize(
    "value", ["2024Q5", "2024Q0", "Q1", "2024-3", "2024/03", "24-03", "2024-13", ""]
)
def test_parse_period_rejects_invalid(value: str) -> None:
    with pytest.raises(ValueError):
        parse_period(value)


@pytest.mark.parametrize(
    ("period", "expected"),
    [
        ("2024Q2", [Month(2024, 4), Month(2024, 5), Month(2024, 6)]),
        (" 2024-01 ", [Month(2024, 1)]),
    ],
)
def test_months_from_env(
    monkeypatch: pytest.MonkeyPatch, period: str, expected: list[Month]
) -> None:
    monkeypatch.setenv("INGEST_PERIOD", period)
    assert months_from_env() == expected


def test_invalid_period_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("INGEST_PERIOD", "2024/01")
    with pytest.raises(ValueError, match="YYYYQn oder YYYY-MM"):
        months_from_env()


@pytest.mark.parametrize("value", [None, "", "  "])
def test_period_is_required(monkeypatch: pytest.MonkeyPatch, value: str | None) -> None:
    monkeypatch.delenv("INGEST_PERIOD", raising=False)
    if value is not None:
        monkeypatch.setenv("INGEST_PERIOD", value)
    with pytest.raises(ValueError, match="INGEST_PERIOD fehlt"):
        months_from_env()
