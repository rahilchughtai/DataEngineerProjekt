from pipeline_common.layout import month_prefix, runs_key
from pipeline_common.periods import Month


def test_month_prefix() -> None:
    assert month_prefix("yellow_tripdata", Month(2024, 3)) == "yellow_tripdata/year=2024/month=03"


def test_runs_key() -> None:
    assert runs_key("yellow_tripdata", "run") == "_runs/yellow_tripdata/run.json"
