"""Pfade im Data Lake und Dateioperationen über die Hadoop-FileSystem-API.

Über die API des Drivers funktionieren dieselben Funktionen für s3a:// (SeaweedFS) und
file:// (Tests), ohne einen zweiten S3-Client im Image.
"""

import logging
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pipeline_common.env import required
from pipeline_common.periods import Month
from pipeline_common.reports import runs_key

log = logging.getLogger(__name__)


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
        return partition_path(f"{self.bronze}/{self.dataset}", month)

    def bronze_run_report(self, run_id: str) -> str:
        return f"{self.bronze}/{runs_key(self.dataset, run_id)}"

    def silver_table(self) -> str:
        return f"{self.silver}/{self.dataset}"

    def silver_month(self, month: Month) -> str:
        return partition_path(self.silver_table(), month)

    def quarantine_table(self) -> str:
        return f"{self.silver}/_quarantine/{self.dataset}"

    def silver_run_report(self, run_id: str) -> str:
        return f"{self.silver}/{runs_key(self.dataset, run_id)}"

    def gold_table(self) -> str:
        return f"{self.gold}/{self.dataset}/zone_hourly"

    def gold_run_report(self, run_id: str) -> str:
        return f"{self.gold}/{runs_key(self.dataset, run_id)}"

    def pipeline_report(self, run_id: str) -> str:
        return f"{self.gold}/{runs_key('pipeline', run_id)}"


def partition_path(table: str, month: Month) -> str:
    """Pfad der Partition eines Monats in einer Tabelle (oder unter einem Bronze-Präfix)."""
    return f"{table}/{month.partition}"


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
        delete(spark, partition_path(table, month))
        return
    (
        df.write.mode("overwrite")
        # Idempotenz: dynamic ersetzt nur die Partitionen, die dieser Schreibvorgang enthält.
        .option("partitionOverwriteMode", "dynamic")
        .partitionBy("year", "month")
        .parquet(table)
    )


@contextmanager
def removed_on_failure(spark: SparkSession, month: Month, tables: Iterable[str]) -> Iterator[None]:
    """Scheitert das Schreiben, fehlt der Monat in allen Tabellen ganz statt halb ersetzt."""
    try:
        yield
    except Exception:
        for table in tables:
            target = partition_path(table, month)
            try:
                delete(spark, target)
            except Exception:
                log.exception("month=%s Aufräumen fehlgeschlagen: %s", month, target)
        raise


def with_lineage(df: DataFrame, month: Month, run_id: str, processed_at: datetime) -> DataFrame:
    """Ergänzt Lineage- und Partitionsspalten eines Monats."""
    return (
        df.withColumn("_run_id", F.lit(run_id))
        .withColumn("_processed_at", F.lit(processed_at))
        # Als zweistelliger String, damit die Pfade wie in Bronze year=YYYY/month=MM heißen.
        .withColumn("year", F.lit(f"{month.year:04d}"))
        .withColumn("month", F.lit(f"{month.month:02d}"))
    )
