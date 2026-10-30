from pathlib import Path

import pytest

from pipeline_common.run_id import read_run_id

RUN_ID = "20240401T000000Z-abc123"


@pytest.fixture(autouse=True)
def clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PIPELINE_RUN_ID_FILE", raising=False)


def write_run_id(monkeypatch: pytest.MonkeyPatch, path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")
    monkeypatch.setenv("PIPELINE_RUN_ID_FILE", str(path))


def test_run_id_from_run_init(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    write_run_id(monkeypatch, tmp_path / "run-id", f"{RUN_ID}\n")
    assert read_run_id() == RUN_ID


@pytest.mark.parametrize("value", [None, "", "   "])
def test_run_id_file_is_required(monkeypatch: pytest.MonkeyPatch, value: str | None) -> None:
    if value is not None:
        monkeypatch.setenv("PIPELINE_RUN_ID_FILE", value)
    with pytest.raises(ValueError, match="PIPELINE_RUN_ID_FILE fehlt"):
        read_run_id()


@pytest.mark.parametrize(
    "value", ["", "abc", "20240401T000000Z-ABC123", "20240401T000000Z-abc1234", "../x"]
)
def test_invalid_run_id_is_rejected(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, value: str
) -> None:
    write_run_id(monkeypatch, tmp_path / "run-id", value)
    with pytest.raises(ValueError, match="Ungültige Run-ID"):
        read_run_id()


def test_missing_run_id_file_is_rejected(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("PIPELINE_RUN_ID_FILE", str(tmp_path / "fehlt"))
    with pytest.raises(ValueError, match="PIPELINE_RUN_ID_FILE"):
        read_run_id()
