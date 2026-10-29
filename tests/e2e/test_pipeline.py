"""End-to-End-Test: docker compose up → Ingestion (Mock-TLC) → Job 1 → Job 2 → Laufbericht.

Geprüft werden Invarianten statt fest eingetragener Zahlen: Bronze gegen die Dateien der Quelle,
Zähler der Reports gegen die Daten im Lake, Silver gegen Gold, die Run-ID über alle Schichten und
die Zugriffsrechte gegen seaweedfs/s3.json.template. Die Tests folgen der Reihenfolge der Läufe.
"""

import calendar
import itertools
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest

from conftest import MONTH, QUARTER
from lake import FIXTURES, IDENTITIES, Lake, LakeState, Run, Settings, sha256

# Exit-Code der Services, wenn Quelldaten für einen Monat fehlen oder nicht veröffentlicht sind.
INCOMPLETE = 3
# Die Kette, wie sie docker compose up über depends_on abarbeitet.
CHAIN = ["ingestion", "silver-job", "gold-job", "run-report"]
ONE_SHOTS = ["lake-init", "run-init", *CHAIN]
# Schritt im Laufbericht → Schicht, in die er seinen eigenen Report schreibt.
STEP_LAYERS = {"ingestion": "bronze", "silver-job": "silver", "gold-job": "gold"}


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


def months_of(report: dict[str, Any]) -> list[str]:
    return [entry["month"] for entry in report["months"]]


def assert_succeeded(run: Run) -> None:
    assert run.exit_code == 0, run.containers
    for service in ONE_SHOTS:
        container = run.containers[service]
        assert (container.state, container.exit_code) == ("exited", 0), service


# --- Konfiguration -------------------------------------------------------------------------------


def test_compose_up_runs_the_chain(config: dict[str, Any]) -> None:
    services = config["services"]
    # Ohne Profile: docker compose up startet alle Schritte.
    assert not [name for name, service in services.items() if service.get("profiles")]
    for init in ("lake-init", "run-init"):
        condition = services["ingestion"]["depends_on"][init]["condition"]
        assert condition == "service_completed_successfully", init
    for previous, step in itertools.pairwise(CHAIN):
        condition = services[step]["depends_on"][previous]["condition"]
        assert condition == "service_completed_successfully", step
    # Ein fehlgeschlagener Schritt hält die Kette an, statt neu zu starten.
    assert {services[name].get("restart", "no") for name in ONE_SHOTS} == {"no"}


# --- Erster Lauf ---------------------------------------------------------------------------------


def test_pipeline_succeeds(first_run: Run) -> None:
    assert_succeeded(first_run)


def test_pipeline_report_adds_up(first_run: Run, lake: Lake, settings: Settings) -> None:
    report = lake.pipeline_report(first_run.run_id)
    steps = {step["step"]: step for step in report["steps"]}

    assert report["run_id"] == first_run.run_id
    assert report["period"] == QUARTER
    assert report["months"] == ["2024-01", "2024-02", "2024-03"]
    assert list(steps) == list(STEP_LAYERS)
    for step, layer in STEP_LAYERS.items():
        service = lake.report(settings.buckets[layer], first_run.run_id)
        assert steps[step]["counts"] == service["counts"], step
        assert steps[step]["seconds"] == service["seconds"], step
    # Die Gesamtzeit enthält zusätzlich Containerstarts und das Warten auf den Spark-Cluster.
    assert report["seconds"] >= sum(step["seconds"] for step in steps.values())


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
    # lake.report findet den Report jeder Schicht über die gemeinsame Run-ID von run-init.
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
    assert_succeeded(second_run)
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


# --- Einzelner Monat -----------------------------------------------------------------------------


def test_single_month_succeeds(month_run: Run, lake: Lake, settings: Settings) -> None:
    assert_succeeded(month_run)
    for layer in ("bronze", "silver", "gold"):
        assert months_of(lake.report(settings.buckets[layer], month_run.run_id)) == [MONTH], layer
    assert lake.pipeline_report(month_run.run_id)["months"] == [MONTH]
    ingestion = lake.report(settings.buckets["bronze"], month_run.run_id)
    assert {entry["status"] for entry in ingestion["months"]} == {"skipped"}


def test_single_month_replaces_only_its_partition(
    second_run: Run, month_run: Run, settings: Settings
) -> None:
    before, after = second_run.state, month_run.state
    assert after.files == before.files
    for layer in ("silver", "gold"):
        bucket = settings.buckets[layer]
        for name, table in after.tables[bucket].items():
            assert without_lineage(table).equals(without_lineage(before.tables[bucket][name]))
            for month in ("2024-01", "2024-02", MONTH):
                expected = month_run.run_id if month == MONTH else second_run.run_id
                assert run_ids(month_rows(table, month)) == {expected}, (name, month)


# --- Nicht veröffentlichtes Quartal --------------------------------------------------------------


def test_unpublished_quarter_stops_after_ingestion(
    month_run: Run, unpublished_run: Run, lake: Lake, settings: Settings
) -> None:
    containers = unpublished_run.containers
    assert unpublished_run.exit_code != 0
    ingestion_container = containers["ingestion"]
    assert (ingestion_container.state, ingestion_container.exit_code) == ("exited", INCOMPLETE)
    # Die Schritte nach der Ingestion sind nie gestartet.
    for service in CHAIN[1:]:
        assert containers[service].state not in {"running", "exited"}, service

    ingestion = lake.report(settings.buckets["bronze"], unpublished_run.run_id)
    assert ingestion["months"]
    assert {entry["status"] for entry in ingestion["months"]} == {"not_published"}
    # Kein Job hat etwas geschrieben: der Lake ist wie nach dem vorigen Lauf.
    assert unpublished_run.state.files == month_run.state.files
