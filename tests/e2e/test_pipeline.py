"""End-to-End-Test: Ingestion (Mock-TLC) → Job 1 → Job 2 → Laufbericht für ein Quartal.

Geprüft werden Invarianten statt fest eingetragener Zahlen: Bronze gegen die Dateien der Quelle,
Zähler der Reports gegen die Daten im Lake, Silver gegen Gold, die Run-ID über alle Schichten und
die Zugriffsrechte gegen seaweedfs/s3.json.template.
"""

import calendar
import json
import posixpath
from collections import Counter
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest

from lake import FIXTURES, IDENTITIES, Lake, Settings, sha256

Tables = dict[str, dict[str, pa.Table]]
# Schritt im Laufbericht → Schicht, in die er seinen eigenen Report schreibt.
STEP_LAYERS = {"ingestion": "bronze", "silver-job": "silver", "gold-job": "gold"}


def by_month(report: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {entry["month"]: entry for entry in report["months"]}


def year_month(month: str) -> tuple[int, int]:
    year, number = month.split("-")
    return int(year), int(number)


def month_rows(table: pa.Table, month: str) -> pa.Table:
    year, number = year_month(month)
    return table.filter((pc.field("year") == year) & (pc.field("month") == number))


def silver_tables(tables: Tables) -> tuple[pa.Table, pa.Table]:
    """Gültige Fahrten und Quarantäne; die Quarantäne erkennt man am Ablehnungsgrund."""
    (valid,) = [t for t in tables["silver"].values() if "reject_reason" not in t.column_names]
    (quarantine,) = [t for t in tables["silver"].values() if "reject_reason" in t.column_names]
    return valid, quarantine


def distinct_rows(table: pa.Table) -> int:
    return table.group_by(table.column_names).aggregate([]).num_rows


def run_ids(table: pa.Table) -> set[str]:
    return set(pc.unique(table["_run_id"]).to_pylist())


def test_pipeline_report_adds_up(run_id: str, lake: Lake, settings: Settings) -> None:
    report = lake.pipeline_report(run_id)
    steps = {step["step"]: step for step in report["steps"]}

    assert report["run_id"] == run_id
    assert report["period"] == settings.period
    assert report["months"] == sorted(path.stem.rsplit("_", 1)[1] for path in FIXTURES.iterdir())
    assert list(steps) == list(STEP_LAYERS)
    for step, layer in STEP_LAYERS.items():
        service = lake.report(settings.buckets[layer], run_id)
        assert steps[step]["counts"] == service["counts"], step
        assert steps[step]["seconds"] == service["seconds"], step
    # Die Gesamtzeit enthält zusätzlich Containerstarts und das Warten auf den Spark-Cluster.
    assert report["seconds"] >= sum(step["seconds"] for step in steps.values())


def test_run_record_on_host_matches_lake(run_id: str, lake: Lake, settings: Settings) -> None:
    record = (settings.runs_dir / f"{run_id}.json").read_text(encoding="utf-8")
    assert json.loads(record) == lake.pipeline_report(run_id)


def test_ingestion_stores_source_files_unchanged(
    run_id: str, lake: Lake, settings: Settings
) -> None:
    bronze = settings.buckets["bronze"]
    report = lake.report(bronze, run_id)
    published = {f"{settings.tlc_base_url}/{path.name}": path for path in FIXTURES.iterdir()}

    assert sorted(entry["source_url"] for entry in report["months"]) == sorted(published)
    for entry in report["months"]:
        data = published[entry["source_url"]].read_bytes()
        metadata = lake.client.head_object(Bucket=bronze, Key=entry["key"])["Metadata"]
        # Bei einer Wiederholung auf befülltem Lake überspringt die Ingestion unveränderte Dateien.
        assert entry["status"] in {"loaded", "skipped"}
        assert lake.read(bronze, entry["key"]) == data
        assert (metadata["size-bytes"], metadata["sha256"]) == (str(len(data)), sha256(data))
        assert metadata["source-url"] == entry["source_url"]
        # Ohne ETag könnte eine Wiederholung nicht erkennen, dass die Quelle unverändert ist.
        assert metadata["source-etag"]


def test_one_file_per_partition(lake: Lake, settings: Settings) -> None:
    """Eine überschriebene Partition darf keine Dateien früherer Läufe behalten."""
    for bucket in settings.buckets.values():
        files = Counter(
            posixpath.dirname(key)
            for key in lake.keys(bucket)
            if key.endswith(".parquet") and "=" in key
        )
        assert files, bucket
        assert set(files.values()) == {1}, (bucket, files)


def test_silver_report_adds_up(run_id: str, lake: Lake, settings: Settings) -> None:
    ingested = by_month(lake.report(settings.buckets["bronze"], run_id))
    for month, entry in by_month(lake.report(settings.buckets["silver"], run_id)).items():
        source = pq.read_table(FIXTURES / ingested[month]["source_url"].rsplit("/", 1)[1])
        assert entry["status"] == "processed", month
        assert entry["rows_in"] == source.num_rows, month
        assert entry["duplicates"] == source.num_rows - distinct_rows(source), month
        assert (
            entry["rows_in"] == entry["rows_valid"] + entry["rows_rejected"] + entry["duplicates"]
        ), month
        assert sum(entry["rejected_by_rule"].values()) == entry["rows_rejected"], month
        # Die Fixtures enthalten echte fehlerhafte Fahrten, also muss die Quarantäne greifen.
        assert entry["rows_valid"] > 0 and entry["rows_rejected"] > 0, month


def test_silver_and_quarantine_match_report(
    run_id: str, tables: Tables, lake: Lake, settings: Settings
) -> None:
    ingested = by_month(lake.report(settings.buckets["bronze"], run_id))
    valid, quarantine = silver_tables(tables)
    for month, entry in by_month(lake.report(settings.buckets["silver"], run_id)).items():
        rows = month_rows(valid, month)
        rejected = month_rows(quarantine, month)

        assert rows.num_rows == entry["rows_valid"], month
        assert pc.unique(pc.strftime(rows["pickup_datetime"], "%Y-%m")).to_pylist() == [month]
        source = f"s3a://{settings.buckets['bronze']}/{ingested[month]['key']}"
        assert pc.unique(rows["_source_file"]).to_pylist() == [source], month

        assert rejected.num_rows == entry["rows_rejected"], month
        counts = pc.value_counts(rejected["reject_reason"]).to_pylist()
        reasons = {count["values"]: count["counts"] for count in counts}
        assert reasons == {rule: n for rule, n in entry["rejected_by_rule"].items() if n}, month


def test_gold_reconciles_with_silver(
    run_id: str, tables: Tables, lake: Lake, settings: Settings
) -> None:
    silver = by_month(lake.report(settings.buckets["silver"], run_id))
    (gold,) = tables["gold"].values()
    for month, entry in by_month(lake.report(settings.buckets["gold"], run_id)).items():
        rows = month_rows(gold, month)
        hours = pc.count_distinct(rows["pickup_hour"]).as_py()
        zones = pc.count_distinct(rows["pu_location_id"]).as_py()

        assert entry["status"] == "processed", month
        assert entry["rows_silver"] == entry["trips_in_gold"] == silver[month]["rows_valid"], month
        # Dichtes Raster: jede Zone in jeder Stunde des Monats genau einmal.
        assert hours == calendar.monthrange(*year_month(month))[1] * 24, month
        assert rows.num_rows == entry["rows_gold"] == hours * zones, month
        assert distinct_rows(rows.select(["pickup_hour", "pu_location_id"])) == rows.num_rows
        assert pc.sum(rows["trip_count"]).as_py() == silver[month]["rows_valid"], month


def test_run_id_links_all_layers(
    run_id: str, tables: Tables, lake: Lake, settings: Settings
) -> None:
    # lake.report findet den Report jeder Schicht über die gemeinsame Run-ID von run-init.
    gold_report = lake.report(settings.buckets["gold"], run_id)
    assert all(entry["silver_run_ids"] == [run_id] for entry in gold_report["months"])
    for layer in ("silver", "gold"):
        for name, table in tables[layer].items():
            assert run_ids(table) == {run_id}, name


@pytest.mark.parametrize("identity", IDENTITIES, ids=lambda identity: identity["name"])
def test_access_matches_policy(
    run_id: str, lake: Lake, settings: Settings, identity: dict[str, Any]
) -> None:
    client = Lake(settings, identity["name"])
    actions = identity["actions"]
    mismatches = []
    for bucket in settings.buckets.values():
        existing = lake.report_key(bucket, run_id)
        for operation in ("List", "Read", "Write"):
            expected = operation in actions or f"{operation}:{bucket}" in actions
            actual = client.can(operation, bucket, existing)
            if operation == "Write" and actual:
                lake.client.delete_object(Bucket=bucket, Key="_e2e/probe")
            if actual != expected:
                mismatches.append(f"{operation}:{bucket} erwartet {expected}, erhalten {actual}")
    assert not mismatches
