"""End-to-End-Test: Orchestrator → Ingestion (Mock-TLC) → Job 1 → Job 2 mit gekürzten echten Daten.

Geprüft werden Invarianten statt fest eingetragener Zahlen: Bronze gegen die Dateien der Quelle,
Zähler der Reports gegen die Daten im Lake, Silver gegen Gold, die Run-ID über alle Schichten und
die Zugriffsrechte gegen seaweedfs/s3.json.template. Die Tests folgen der Reihenfolge der Läufe.
"""

import calendar
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest

from lake import FIXTURES, IDENTITIES, Lake, LakeState, Run, Settings, sha256
from orchestrator.cli import INCOMPLETE


def by_month(report: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {entry["month"]: entry for entry in report["months"]}


def month_rows(table: pa.Table, month: str) -> pa.Table:
    year, number = month.split("-")
    return table.filter(pc.and_(pc.equal(table["year"], year), pc.equal(table["month"], number)))


def silver_tables(state: LakeState, settings: Settings) -> tuple[pa.Table, pa.Table]:
    """Gültige Fahrten und Quarantäne; die Quarantäne erkennt man am Ablehnungsgrund."""
    tables = state.tables[settings.buckets["silver"]].values()
    (valid,) = [t for t in tables if "reject_reason" not in t.column_names]
    (quarantine,) = [t for t in tables if "reject_reason" in t.column_names]
    return valid, quarantine


def gold_table(state: LakeState, settings: Settings) -> pa.Table:
    (gold,) = state.tables[settings.buckets["gold"]].values()
    return gold


def distinct_rows(table: pa.Table) -> int:
    return table.group_by(table.column_names).aggregate([]).num_rows


def run_ids(table: pa.Table) -> set[str]:
    return set(pc.unique(table["_run_id"]).to_pylist())


def without_lineage(table: pa.Table) -> pa.Table:
    """Ohne die Lineage-Spalten (_run_id, _processed_at, …) müssen zwei Läufe gleich sein."""
    data = table.select([name for name in table.column_names if not name.startswith("_")])
    return data.sort_by([(name, "ascending") for name in data.column_names])


# --- Erster Lauf ---------------------------------------------------------------------------------


def test_orchestrator_succeeds(first_run: Run) -> None:
    assert first_run.exit_code == 0, first_run.report
    assert first_run.report["status"] == "ok"
    assert all(step["status"] == "ok" for step in first_run.report["steps"])


def test_ingestion_stores_source_files_unchanged(
    first_run: Run, lake: Lake, settings: Settings
) -> None:
    bronze = settings.buckets["bronze"]
    report = lake.report(bronze, first_run.run_id)
    published = {f"{settings.tlc_base_url}/{path.name}": path for path in FIXTURES.iterdir()}

    assert sorted(entry["source_url"] for entry in report["months"]) == sorted(published)
    for entry in report["months"]:
        data = published[entry["source_url"]].read_bytes()
        metadata = lake.client.head_object(Bucket=bronze, Key=entry["key"])["Metadata"]
        assert entry["status"] == "loaded"
        assert (entry["bytes"], entry["sha256"]) == (len(data), sha256(data))
        assert lake.read(bronze, entry["key"]) == data
        assert metadata["sha256"] == sha256(data)
        assert metadata["source-url"] == entry["source_url"]
        # Ohne ETag könnte die Wiederholung nicht erkennen, dass die Quelle unverändert ist.
        assert metadata["source-etag"]


def test_silver_report_adds_up(first_run: Run, lake: Lake, settings: Settings) -> None:
    ingested = by_month(lake.report(settings.buckets["bronze"], first_run.run_id))
    for month, entry in by_month(lake.report(settings.buckets["silver"], first_run.run_id)).items():
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


def test_silver_and_quarantine_match_report(first_run: Run, lake: Lake, settings: Settings) -> None:
    ingested = by_month(lake.report(settings.buckets["bronze"], first_run.run_id))
    valid, quarantine = silver_tables(first_run.state, settings)
    for month, entry in by_month(lake.report(settings.buckets["silver"], first_run.run_id)).items():
        rows = month_rows(valid, month)
        rejected = month_rows(quarantine, month)
        year, number = map(int, month.split("-"))

        assert rows.num_rows == entry["rows_valid"], month
        assert pc.unique(pc.year(rows["pickup_datetime"])).to_pylist() == [year], month
        assert pc.unique(pc.month(rows["pickup_datetime"])).to_pylist() == [number], month
        source = f"s3a://{settings.buckets['bronze']}/{ingested[month]['key']}"
        assert pc.unique(rows["_source_file"]).to_pylist() == [source], month

        assert rejected.num_rows == entry["rows_rejected"], month
        counts = pc.value_counts(rejected["reject_reason"]).to_pylist()
        reasons = {count["values"]: count["counts"] for count in counts}
        assert reasons == {rule: n for rule, n in entry["rejected_by_rule"].items() if n}, month


def test_gold_reconciles_with_silver(first_run: Run, lake: Lake, settings: Settings) -> None:
    silver = by_month(lake.report(settings.buckets["silver"], first_run.run_id))
    gold = gold_table(first_run.state, settings)
    for month, entry in by_month(lake.report(settings.buckets["gold"], first_run.run_id)).items():
        rows = month_rows(gold, month)
        year, number = map(int, month.split("-"))
        hours = pc.count_distinct(rows["pickup_hour"]).as_py()
        zones = pc.count_distinct(rows["pu_location_id"]).as_py()

        assert entry["status"] == "processed", month
        assert entry["rows_silver"] == entry["trips_in_gold"] == silver[month]["rows_valid"], month
        # Dichtes Raster: jede Zone in jeder Stunde des Monats genau einmal.
        assert hours == calendar.monthrange(year, number)[1] * 24, month
        assert rows.num_rows == entry["rows_gold"] == hours * zones, month
        assert distinct_rows(rows.select(["pickup_hour", "pu_location_id"])) == rows.num_rows
        assert pc.sum(rows["trip_count"]).as_py() == silver[month]["rows_valid"], month


def test_run_id_links_all_layers(first_run: Run, lake: Lake, settings: Settings) -> None:
    run_id = first_run.run_id
    # lake.report findet den Report jeder Schicht über die Run-ID des Orchestrators.
    gold_report = lake.report(settings.buckets["gold"], run_id)
    assert all(entry["silver_run_ids"] == [run_id] for entry in gold_report["months"])
    for layer in ("silver", "gold"):
        for name, table in first_run.state.tables[settings.buckets[layer]].items():
            assert run_ids(table) == {run_id}, name


@pytest.mark.parametrize("identity", IDENTITIES, ids=lambda identity: identity["name"])
def test_access_matches_policy(
    first_run: Run, lake: Lake, settings: Settings, identity: dict[str, Any]
) -> None:
    client = Lake(settings, identity["name"])
    actions = identity["actions"]
    mismatches = []
    for bucket in settings.buckets.values():
        existing = lake.report_key(bucket, first_run.run_id)
        for operation in ("List", "Read", "Write"):
            expected = operation in actions or f"{operation}:{bucket}" in actions
            actual = client.can(operation, bucket, existing)
            if operation == "Write" and actual:
                lake.client.delete_object(Bucket=bucket, Key="_e2e/probe")
            if actual != expected:
                mismatches.append(f"{operation}:{bucket} erwartet {expected}, erhalten {actual}")
    assert not mismatches


# --- Wiederholung --------------------------------------------------------------------------------


def test_rerun_succeeds_with_new_run_id(first_run: Run, second_run: Run) -> None:
    assert second_run.exit_code == 0, second_run.report
    assert all(step["status"] == "ok" for step in second_run.report["steps"])
    assert second_run.run_id != first_run.run_id


def test_rerun_skips_unchanged_source(second_run: Run, lake: Lake, settings: Settings) -> None:
    report = lake.report(settings.buckets["bronze"], second_run.run_id)
    assert {entry["status"] for entry in report["months"]} == {"skipped"}


def test_rerun_is_idempotent(first_run: Run, second_run: Run, settings: Settings) -> None:
    before, after = first_run.state, second_run.state
    # Dieselben Partitionen ersetzt, keine zusätzlichen Dateien.
    assert after.files == before.files
    for bucket, tables in after.tables.items():
        assert tables.keys() == before.tables[bucket].keys()
        for name, table in tables.items():
            assert without_lineage(table).equals(without_lineage(before.tables[bucket][name]))
            if bucket != settings.buckets["bronze"]:
                assert run_ids(table) == {second_run.run_id}, name


# --- Nicht veröffentlichtes Quartal --------------------------------------------------------------


def test_unpublished_quarter_stops_after_ingestion(
    second_run: Run, unpublished_run: Run, lake: Lake, settings: Settings
) -> None:
    report = unpublished_run.report
    *completed, last = report["steps"]
    assert unpublished_run.exit_code == INCOMPLETE, report
    assert report["status"] == "failed"
    assert (last["step"], last["exit_code"]) == ("ingestion", INCOMPLETE)
    assert all(step["status"] == "ok" for step in completed)

    ingestion = lake.report(settings.buckets["bronze"], unpublished_run.run_id)
    assert ingestion["months"]
    assert {entry["status"] for entry in ingestion["months"]} == {"not_published"}
    # Kein Job hat etwas geschrieben: der Lake ist wie nach dem vorigen Lauf.
    assert unpublished_run.state.files == second_run.state.files
