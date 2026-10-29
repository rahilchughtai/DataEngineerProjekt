import pytest

from pipeline_common.env import ConfigError, optional, required


def test_required_strips_value(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SOME_VAR", " wert ")
    assert required("SOME_VAR") == "wert"


@pytest.mark.parametrize("value", [None, "", "  "])
def test_required_rejects_missing_or_blank(
    monkeypatch: pytest.MonkeyPatch, value: str | None
) -> None:
    monkeypatch.delenv("SOME_VAR", raising=False)
    if value is not None:
        monkeypatch.setenv("SOME_VAR", value)
    with pytest.raises(ConfigError, match="SOME_VAR fehlt"):
        required("SOME_VAR")


def test_optional_falls_back_to_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SOME_VAR", raising=False)
    assert optional("SOME_VAR", "standard") == "standard"
    monkeypatch.setenv("SOME_VAR", "eigener")
    assert optional("SOME_VAR", "standard") == "eigener"
