#!/usr/bin/env python3
"""Legt die Buckets der Lake-Schichten über die S3-API an.

Idempotent: vorhandene Buckets bleiben unverändert. Läuft mit der Admin-Identität, weil nur sie
Buckets anlegen darf; die Pipeline-Services erhalten diese Zugangsdaten nie.
"""

import os

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

BUCKETS = os.environ.get("LAKE_BUCKETS", "bronze silver gold").split()


def log(message: str) -> None:
    print(f"init-buckets: {message}", flush=True)


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT"],
        aws_access_key_id=os.environ["S3_ADMIN_ACCESS_KEY"],
        aws_secret_access_key=os.environ["S3_ADMIN_SECRET_KEY"],
        region_name="us-east-1",
        # Der Healthcheck meldet SeaweedFS bereit, bevor der Filer jede Anfrage annimmt;
        # Wiederholungen mit Backoff überbrücken diese Anlaufzeit.
        config=Config(
            s3={"addressing_style": "path"}, retries={"mode": "standard", "max_attempts": 10}
        ),
    )


def bucket_exists(s3, bucket: str) -> bool:
    try:
        s3.head_bucket(Bucket=bucket)
    except ClientError as err:
        # Nur "nicht gefunden" heißt fehlend; z. B. AccessDenied bricht den Lauf ab.
        if err.response["Error"]["Code"] in ("404", "NoSuchBucket", "NotFound"):
            return False
        raise
    return True


def main() -> None:
    s3 = s3_client()
    for bucket in BUCKETS:
        if bucket_exists(s3, bucket):
            log(f"{bucket} existiert bereits")
        else:
            s3.create_bucket(Bucket=bucket)
            log(f"{bucket} angelegt")


if __name__ == "__main__":
    main()
