"""Shared parts of the Spark jobs: session, tables in the data lake and writing a month."""

import os

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from pipeline_common.logs import setup_logging
from pipeline_common.periods import months, partition

# Tables in the data lake (SeaweedFS S3); each is partitioned by year=YYYY/month=MM.
BRONZE = "s3a://bronze/yellow_tripdata"
# Checksum and source version per month, written by the ingestion.
MANIFESTS = "s3a://bronze/_manifests/yellow_tripdata"
SILVER = "s3a://silver/yellow_tripdata"
QUARANTINE = "s3a://silver/_quarantine/yellow_tripdata"
GOLD = "s3a://gold/yellow_tripdata/zone_hourly"
REPORTS = "s3a://gold/_reports"

# TLC zones have IDs 1 to 263; 264/265 stand for "unknown" / "outside NYC".
LAST_ZONE = 263


def start(app: str) -> SparkSession:
    """SparkSession with the job's S3 credentials.

    Only the driver knows the credentials; the executors on the workers receive them via the
    job's Hadoop configuration. Endpoint and S3A settings live in spark-defaults.conf.
    """
    setup_logging("py4j")
    spark = (
        SparkSession.builder.appName(app)
        .config("spark.hadoop.fs.s3a.access.key", os.environ["S3_SPARK_ACCESS_KEY"])
        .config("spark.hadoop.fs.s3a.secret.key", os.environ["S3_SPARK_SECRET_KEY"])
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def period_months() -> list[tuple[int, int]]:
    return months(os.environ["INGEST_PERIOD"])


def month_bounds(year: int, month: int) -> tuple[Column, Column]:
    """First instant of the month and of the next month, as local time (timestamp_ntz)."""

    def first_instant(y: int, m: int) -> Column:
        return F.lit(f"{y:04d}-{m:02d}-01 00:00:00").cast("timestamp_ntz")

    end = first_instant(year + 1, 1) if month == 12 else first_instant(year, month + 1)
    return first_instant(year, month), end


def month_path(table: str, year: int, month: int) -> str:
    return f"{table}/{partition(year, month)}"


def write_month(df: DataFrame, table: str, year: int, month: int) -> None:
    """Overwrites exactly this month of the table; all other months stay unchanged.

    This makes every job idempotent: a rerun replaces the month instead of duplicating rows.
    coalesce(1): one file per month instead of 200 small files after the shuffle.
    """
    df.coalesce(1).write.mode("overwrite").parquet(month_path(table, year, month))
