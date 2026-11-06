"""End-to-end test: mock TLC → ingestion → silver → gold → run report for 2024Q1.

It checks invariants instead of hard-coded numbers. In CI the test runs twice on the same
lake; the same checks then show that a rerun duplicates nothing.
"""

import hashlib
import json
import posixpath
from collections import Counter
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest
from botocore.exceptions import ClientError
from pyarrow import fs

from conftest import BUCKETS, FIXTURES, IDENTITIES, keys, s3_client


def fixture_file(month: str):
    return FIXTURES / f"yellow_tripdata_{month}.parquet"


def distinct_rows(table: pa.Table) -> int:
    return table.group_by(table.column_names).aggregate([]).num_rows


def test_bronze_stores_source_files_unchanged(s3: Any) -> None:
    for path in sorted(FIXTURES.iterdir()):
        year, month = path.stem.rsplit("_", 1)[1].split("-")
        key = f"yellow_tripdata/year={year}/month={month}/{path.name}"
        assert s3.get_object(Bucket="bronze", Key=key)["Body"].read() == path.read_bytes(), key


def test_manifest_and_report_carry_source_checksum(
    s3: Any, report: dict[str, dict[str, Any]]
) -> None:
    for month, row in report.items():
        year, number = month.split("-")
        key = f"_manifests/yellow_tripdata/year={year}/month={number}/manifest.json"
        manifest = json.loads(s3.get_object(Bucket="bronze", Key=key)["Body"].read())
        source = fixture_file(month).read_bytes()
        assert manifest["sha256"] == hashlib.sha256(source).hexdigest(), month
        assert manifest["size_bytes"] == len(source), month
        assert manifest["etag"] and manifest["last_modified"], month
        assert row["source_sha256"] == manifest["sha256"], month


def test_report_adds_up(report: dict[str, dict[str, Any]]) -> None:
    assert sorted(report) == ["2024-01", "2024-02", "2024-03"]
    for month, row in report.items():
        source = pq.read_table(fixture_file(month))
        assert row["bronze_rows"] == source.num_rows, month
        assert row["duplicates"] == source.num_rows - distinct_rows(source), month
        assert sum(row["rejected_by_reason"].values()) == row["quarantined_rows"], month
        # The test data contains real faulty trips, so the quarantine must catch them.
        assert row["silver_rows"] > 0 and row["quarantined_rows"] > 0, month
        assert row["gold_trips"] == row["silver_rows"], month
        # Each stage builds on the previous one (ISO timestamps in UTC sort as text).
        stages = [row["bronze_loaded_at"], row["silver_processed_at"], row["gold_processed_at"]]
        assert stages == sorted(stages), month
        assert row["bronze_to_silver_s"] >= 0 and row["silver_to_gold_s"] >= 0, month


def test_silver_pickups_lie_in_their_month(
    lake_fs: fs.S3FileSystem, report: dict[str, dict[str, Any]]
) -> None:
    silver = pq.read_table("silver/yellow_tripdata", filesystem=lake_fs)
    for month, row in report.items():
        year, number = (int(part) for part in month.split("-"))
        rows = silver.filter((pc.field("year") == year) & (pc.field("month") == number))
        assert rows.num_rows == row["silver_rows"], month
        assert pc.unique(pc.strftime(rows["pickup_datetime"], "%Y-%m")).to_pylist() == [month]


def test_one_file_per_month(s3: Any) -> None:
    """An overwritten month must not keep files from earlier runs."""
    for bucket in BUCKETS:
        files = Counter(posixpath.dirname(k) for k in keys(s3, bucket) if k.endswith(".parquet"))
        assert files and set(files.values()) == {1}, (bucket, files)


def allowed(client: Any, operation: str, bucket: str, existing_key: str) -> bool:
    """Performs List, Read or Write; False if SeaweedFS denies access."""
    actions = {
        "List": lambda: client.list_objects_v2(Bucket=bucket, MaxKeys=1),
        "Read": lambda: client.get_object(Bucket=bucket, Key=existing_key),
        "Write": lambda: client.put_object(Bucket=bucket, Key="_e2e/probe", Body=b""),
    }
    try:
        actions[operation]()
    except ClientError as err:
        if err.response["Error"]["Code"] == "AccessDenied":
            return False
        raise
    return True


@pytest.mark.parametrize("identity", IDENTITIES, ids=lambda identity: identity["name"])
def test_access_matches_policy(s3: Any, identity: dict[str, Any]) -> None:
    client = s3_client(identity)
    mismatches = []
    for bucket in BUCKETS:
        existing_key = keys(s3, bucket)[0]
        for operation in ("List", "Read", "Write"):
            expected = (
                operation in identity["actions"] or f"{operation}:{bucket}" in identity["actions"]
            )
            actual = allowed(client, operation, bucket, existing_key)
            if operation == "Write" and actual:
                s3.delete_object(Bucket=bucket, Key="_e2e/probe")
            if actual != expected:
                mismatches.append(f"{operation}:{bucket} expected {expected}, got {actual}")
    assert not mismatches
