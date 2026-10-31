import importlib.util
from pathlib import Path

import pytest
from botocore.exceptions import ClientError

# Der Dateiname enthält einen Bindestrich und ist deshalb nicht per import ladbar.
_spec = importlib.util.spec_from_file_location(
    "init_buckets", Path(__file__).with_name("init-buckets.py")
)
init_buckets = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(init_buckets)


class FakeS3:
    """Minimaler Ersatz für den boto3-Client."""

    def __init__(self, existing: set[str] | None = None, error: str | None = None) -> None:
        self.buckets = set(existing or ())
        self.error = error

    def head_bucket(self, Bucket: str) -> None:  # noqa: N803
        if self.error:
            raise ClientError({"Error": {"Code": self.error}}, "HeadBucket")
        if Bucket not in self.buckets:
            raise ClientError({"Error": {"Code": "404"}}, "HeadBucket")

    def create_bucket(self, Bucket: str) -> None:  # noqa: N803
        self.buckets.add(Bucket)


def test_missing_bucket_is_created() -> None:
    s3 = FakeS3()

    init_buckets.ensure_bucket(s3, "bronze")

    assert s3.buckets == {"bronze"}


def test_existing_bucket_is_left_alone() -> None:
    s3 = FakeS3(existing={"bronze"})
    s3.create_bucket = None  # ein Aufruf würde scheitern

    init_buckets.ensure_bucket(s3, "bronze")


def test_access_denied_is_not_mistaken_for_missing_bucket() -> None:
    s3 = FakeS3(error="403")

    with pytest.raises(ClientError):
        init_buckets.ensure_bucket(s3, "bronze")
    assert s3.buckets == set()
