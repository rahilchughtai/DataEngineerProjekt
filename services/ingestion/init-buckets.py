"""Init-Buckets: legt die Buckets der Lake-Schichten über die S3-API an.

Idempotent: vorhandene Buckets bleiben unverändert. Läuft mit der Admin-Identität, weil nur sie
Buckets anlegen darf; die Pipeline-Services selbst erhalten diese Zugangsdaten nie.
"""

import logging
import os

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from pipeline_common.logs import setup_logging

log = logging.getLogger("init-buckets")

BUCKETS = ("bronze", "silver", "gold")


def bucket_exists(s3, bucket: str) -> bool:
    try:
        s3.head_bucket(Bucket=bucket)
    except ClientError as err:
        if err.response["Error"]["Code"] in ("404", "NoSuchBucket", "NotFound"):
            return False
        raise
    return True


def ensure_bucket(s3, bucket: str) -> None:
    if bucket_exists(s3, bucket):
        log.info("%s existiert bereits", bucket)
        return
    s3.create_bucket(Bucket=bucket)
    log.info("%s angelegt", bucket)


def main() -> None:
    setup_logging()
    s3 = boto3.client(
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
    for bucket in BUCKETS:
        ensure_bucket(s3, bucket)


if __name__ == "__main__":
    main()
