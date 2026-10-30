"""Pfade im Data Lake und Dateioperationen über die Hadoop-FileSystem-API.

Über die API des Drivers funktionieren dieselben Funktionen für s3a:// (SeaweedFS) und
file:// (Tests), ohne einen zweiten S3-Client im Image.
"""

from dataclasses import dataclass
from typing import Any

from pyspark.sql import DataFrame, SparkSession

from pipeline_common.env import required
from pipeline_common.periods import Month


@dataclass(frozen=True)
class LakePaths:
    bronze: str
    silver: str
    gold: str
    dataset: str

    @classmethod
    def from_env(cls) -> "LakePaths":
        return cls(
            bronze=f"s3a://{required('BRONZE_BUCKET')}",
            silver=f"s3a://{required('SILVER_BUCKET')}",
            gold=f"s3a://{required('GOLD_BUCKET')}",
            dataset=required("TLC_DATASET"),
        )

    def bronze_month(self, month: Month) -> str:
        return f"{self.bronze}/{self.dataset}/{partition(month)}"

    def bronze_run_report(self, run_id: str) -> str:
        return f"{self.bronze}/_runs/{self.dataset}/{run_id}.json"

    def silver_table(self) -> str:
        return f"{self.silver}/{self.dataset}"

    def silver_month(self, month: Month) -> str:
        return f"{self.silver_table()}/{partition(month)}"

    def quarantine_table(self) -> str:
        return f"{self.silver}/_quarantine/{self.dataset}"

    def run_report(self, run_id: str) -> str:
        return f"{self.silver}/_runs/{self.dataset}/{run_id}.json"

    def gold_table(self) -> str:
        return f"{self.gold}/{self.dataset}/zone_hourly"

    def gold_run_report(self, run_id: str) -> str:
        return f"{self.gold}/_runs/{self.dataset}/{run_id}.json"

    def pipeline_report(self, run_id: str) -> str:
        return f"{self.gold}/_runs/pipeline/{run_id}.json"


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


def read_text(spark: SparkSession, path: str) -> str:
    fs, hadoop_path = _fs(spark, path)
    io_utils = spark.sparkContext._jvm.org.apache.commons.io.IOUtils
    stream = fs.open(hadoop_path)
    try:
        return bytes(io_utils.toByteArray(stream)).decode()
    finally:
        stream.close()


def write_text(spark: SparkSession, path: str, text: str) -> None:
    fs, hadoop_path = _fs(spark, path)
    stream = fs.create(hadoop_path, True)
    try:
        stream.write(bytearray(text.encode()))
    finally:
        stream.close()


def write_partition(
    spark: SparkSession, df: DataFrame, table: str, month: Month, has_rows: bool
) -> None:
    """Überschreibt genau die Partition des Monats, alle anderen bleiben unverändert."""
    if not has_rows:
        # Ein leerer DataFrame schreibt keine Partition und würde einen alten Stand stehen lassen.
        delete(spark, f"{table}/{partition(month)}")
        return
    (
        df.write.mode("overwrite")
        # Idempotenz: dynamic ersetzt nur die Partitionen, die dieser Schreibvorgang enthält.
        .option("partitionOverwriteMode", "dynamic")
        .partitionBy("year", "month")
        .parquet(table)
    )
