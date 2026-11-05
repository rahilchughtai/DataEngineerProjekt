import json
from datetime import datetime
from pathlib import Path

from report import write_local


def row(month: str, loaded: datetime, gold: datetime) -> dict:
    return {
        "month": month,
        "bronze_rows": 10,
        "silver_rows": 8,
        "quarantined_rows": 2,
        "duplicates": 0,
        "rejected_by_reason": {"amount_invalid": 1, "distance_invalid": 1},
        "gold_rows": 744,
        "gold_trips": 8,
        "bronze_loaded_at": loaded,
        "silver_processed_at": gold,
        "gold_processed_at": gold,
        "bronze_to_silver_s": 5.0,
        "silver_to_gold_s": None,
        "source_sha256": "ab12",
        "source_last_modified": "Wed, 01 May 2024 00:00:00 GMT",
    }


def test_write_local_reports_run_time_and_markdown(tmp_path: Path) -> None:
    rows = [
        row("2024-01", datetime(2024, 4, 1, 12, 0, 0), datetime(2024, 4, 1, 12, 1, 0)),
        row("2024-02", datetime(2024, 4, 1, 12, 0, 10), datetime(2024, 4, 1, 12, 2, 30)),
    ]
    write_local(tmp_path, "2024Q1", rows)

    report = json.loads((tmp_path / "report.json").read_text(encoding="utf-8"))
    assert report["pipeline_s"] == 150.0
    assert [m["month"] for m in report["months"]] == ["2024-01", "2024-02"]

    md = (tmp_path / "report.md").read_text(encoding="utf-8")
    assert "150.0 s" in md
    assert "| 2024-01 | 10 | 8 | 2 | 0 | 744 | 8 | 5.0 | n/a |" in md
    assert "| amount_invalid | 2 |" in md
    assert "| 2024-01 | Wed, 01 May 2024 00:00:00 GMT | `ab12` |" in md
