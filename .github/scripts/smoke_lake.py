"""Hilfsskript für den Smoke-Test in CI: echter Data Lake, echte Images, synthetische Daten.

Unterbefehle:
  seed   legt eine synthetische TLC-Datei für 2024-01 mit gültigen und ungültigen Fahrten in Bronze
  check  prüft die neuesten Reports von Job 1 und Job 2 gegen die erwarteten Zahlen

Braucht pyarrow und boto3 (uv run --no-project --with ...) und eine .env im Repo-Root.
"""

import io
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
ENDPOINT = "http://127.0.0.1:8333"
DATASET = "yellow_tripdata"
MONTH = "2024-01"
KEY = f"{DATASET}/year=2024/month=01/{DATASET}_{MONTH}.parquet"

VALID_TRIPS = 200
# Regel → Zahl der Fahrten, die genau an dieser Regel scheitern (siehe trips()).
REJECTED = {"amount_invalid": 7, "location_invalid": 3}
ZONES = 265
HOURS_JANUARY = 31 * 24


def read_env() -> dict[str, str]:
    values: dict[str, str] = {}
    for line in (ROOT / ".env").read_text(encoding="utf-8").splitlines():
        if line and not line.startswith("#") and "=" in line:
            name, value = line.split("=", 1)
            values[name] = value
    return values


def s3_client(identity: str) -> Any:
    import boto3
    from botocore.config import Config

    env = read_env()
    return boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        aws_access_key_id=env[f"S3_{identity}_ACCESS_KEY"],
        aws_secret_access_key=env[f"S3_{identity}_SECRET_KEY"],
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}),
    )


def trips() -> list[dict[str, Any]]:
    start = datetime(2024, 1, 1)
    rows = []
    for i in range(VALID_TRIPS):
        # 7 und 744 sind teilerfremd: jede Fahrt bekommt eine eigene Stunde, keine Duplikate.
        pickup = start + timedelta(hours=(i * 7) % HOURS_JANUARY, minutes=i % 60)
        rows.append(
            {
                "VendorID": 2,
                "tpep_pickup_datetime": pickup,
                "tpep_dropoff_datetime": pickup + timedelta(minutes=5 + i % 40),
                "passenger_count": 1 + i % 4,
                "trip_distance": 0.5 + (i % 20) / 2,
                "PULocationID": 1 + (i * 13) % ZONES,
                "DOLocationID": 1 + (i * 17) % ZONES,
                "payment_type": 1,
                "fare_amount": 8.0 + i % 30,
                "total_amount": 12.0 + i % 30,
                "store_and_fwd_flag": "N",
            }
        )
    # Abgelehnte Fahrten unterscheiden sich untereinander, sonst entfernt sie die Deduplizierung.
    template = rows[0]
    rows += [template | {"fare_amount": -5.0 - n} for n in range(5)]
    rows += [template | {"total_amount": float("inf"), "fare_amount": 9.0 + n} for n in range(2)]
    rows += [template | {"PULocationID": 999 + n} for n in range(3)]
    return rows


def seed() -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    # Typen wie in den TLC-Dateien 2024.
    schema = pa.schema(
        [
            ("VendorID", pa.int32()),
            ("tpep_pickup_datetime", pa.timestamp("us")),
            ("tpep_dropoff_datetime", pa.timestamp("us")),
            ("passenger_count", pa.int64()),
            ("trip_distance", pa.float64()),
            ("PULocationID", pa.int32()),
            ("DOLocationID", pa.int32()),
            ("payment_type", pa.int64()),
            ("fare_amount", pa.float64()),
            ("total_amount", pa.float64()),
            ("store_and_fwd_flag", pa.string()),
        ]
    )
    table = pa.Table.from_pylist(trips(), schema=schema)
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    s3_client("INGESTION").put_object(Bucket="bronze", Key=KEY, Body=buffer.getvalue())
    print(f"seed: {table.num_rows} Zeilen nach s3://bronze/{KEY}")


def latest_report(client: Any, bucket: str) -> dict[str, Any]:
    prefix = f"_runs/{DATASET}/"
    listing = client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    keys = [item["Key"] for item in listing.get("Contents", [])]
    if not keys:
        raise AssertionError(f"kein Report unter s3://{bucket}/{prefix}")
    # Run-IDs beginnen mit dem Zeitstempel und sind deshalb nach Startzeit sortierbar.
    body = client.get_object(Bucket=bucket, Key=max(keys))["Body"].read()
    return json.loads(body)


def expect(label: str, actual: Any, expected: Any) -> None:
    if actual != expected:
        raise AssertionError(f"{label}: erwartet {expected!r}, erhalten {actual!r}")
    print(f"check: {label} = {actual!r}")


def check() -> None:
    client = s3_client("SPARK")
    silver = latest_report(client, "silver")
    (silver_month,) = silver["months"]
    expect("silver.status", silver_month["status"], "processed")
    expect("silver.rows_valid", silver_month["rows_valid"], VALID_TRIPS)
    expect("silver.rows_rejected", silver_month["rows_rejected"], sum(REJECTED.values()))
    for rule, count in REJECTED.items():
        expect(f"silver.rejected_by_rule.{rule}", silver_month["rejected_by_rule"][rule], count)

    gold = latest_report(client, "gold")
    (gold_month,) = gold["months"]
    expect("gold.status", gold_month["status"], "processed")
    expect("gold.trips_in_gold", gold_month["trips_in_gold"], VALID_TRIPS)
    expect("gold.rows_gold", gold_month["rows_gold"], ZONES * HOURS_JANUARY)
    expect("gold.silver_run_ids", gold_month["silver_run_ids"], [silver["run_id"]])


COMMANDS = {"seed": seed, "check": check}

if __name__ == "__main__":
    if len(sys.argv) != 2 or sys.argv[1] not in COMMANDS:
        sys.exit(f"Aufruf: {Path(__file__).name} {{{','.join(COMMANDS)}}}")
    COMMANDS[sys.argv[1]]()
