#!/usr/bin/env python3
"""Creates the buckets of the lake layers via the S3 API.

Idempotent: existing buckets stay unchanged. Runs with the admin identity because only it may
create buckets; the pipeline services never receive these credentials.
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
        # The healthcheck reports SeaweedFS ready before the filer accepts every request;
        # retries with backoff bridge this startup time.
        config=Config(
            s3={"addressing_style": "path"}, retries={"mode": "standard", "max_attempts": 10}
        ),
    )


def bucket_exists(s3, bucket: str) -> bool:
    try:
        s3.head_bucket(Bucket=bucket)
    except ClientError as err:
        # Only "not found" means missing; e.g. AccessDenied aborts the run.
        if err.response["Error"]["Code"] in ("404", "NoSuchBucket", "NotFound"):
            return False
        raise
    return True


def main() -> None:
    s3 = s3_client()
    for bucket in BUCKETS:
        if bucket_exists(s3, bucket):
            log(f"{bucket} already exists")
        else:
            s3.create_bucket(Bucket=bucket)
            log(f"{bucket} created")


if __name__ == "__main__":
    main()
