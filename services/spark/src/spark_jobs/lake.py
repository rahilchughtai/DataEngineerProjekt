"""Pfade im Data Lake und Dateioperationen über die Hadoop-FileSystem-API.

Über die API des Drivers funktionieren dieselben Funktionen für s3a:// (SeaweedFS) und
file:// (Tests), ohne einen zweiten S3-Client im Image.
"""

import os
from dataclasses import dataclass
from typing import Any

from pyspark.sql import SparkSession

from spark_jobs.periods import Month


@dataclass(frozen=True)
class LakePaths:
    bronze: str
    silver: str
    dataset: str

    @classmethod
    def from_env(cls) -> "LakePaths":
        return cls(
            bronze=f"s3a://{os.environ.get('BRONZE_BUCKET', 'bronze')}",
            silver=f"s3a://{os.environ.get('SILVER_BUCKET', 'silver')}",
            dataset=os.environ.get("TLC_DATASET", "yellow_tripdata"),
        )

    def bronze_month(self, month: Month) -> str:
        return f"{self.bronze}/{self.dataset}/{partition(month)}"

    def silver_table(self) -> str:
        return f"{self.silver}/{self.dataset}"

    def quarantine_table(self) -> str:
        return f"{self.silver}/_quarantine/{self.dataset}"

    def run_report(self, run_id: str) -> str:
        return f"{self.silver}/_runs/{self.dataset}/{run_id}.json"


def partition(month: Month) -> str:
    return f"year={month.year:04d}/month={month.month:02d}"


def _fs(spark: SparkSession, path: str) -> tuple[Any, Any]:
    jvm = spark.sparkContext._jvm
    hadoop_path = jvm.org.apache.hadoop.fs.Path(path)
    return hadoop_path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration()), hadoop_path


def exists(spark: SparkSession, path: str) -> bool:
    fs, hadoop_path = _fs(spark, path)
    return bool(fs.exists(hadoop_path))


def delete(spark: SparkSession, path: str) -> None:
    fs, hadoop_path = _fs(spark, path)
    fs.delete(hadoop_path, True)


def write_text(spark: SparkSession, path: str, text: str) -> None:
    fs, hadoop_path = _fs(spark, path)
    stream = fs.create(hadoop_path, True)
    try:
        stream.write(bytearray(text.encode()))
    finally:
        stream.close()
