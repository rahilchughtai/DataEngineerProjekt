"""Gemeinsames der Spark-Jobs: Session, Tabellen im Data Lake und Schreiben eines Monats."""

import logging
import os

from pyspark.sql import DataFrame, SparkSession

from pipeline_common.periods import months, partition

# Tabellen im Data Lake (SeaweedFS S3); jede ist nach year=YYYY/month=MM partitioniert.
BRONZE = "s3a://bronze/yellow_tripdata"
SILVER = "s3a://silver/yellow_tripdata"
QUARANTINE = "s3a://silver/_quarantine/yellow_tripdata"
GOLD = "s3a://gold/yellow_tripdata/zone_hourly"
REPORTS = "s3a://gold/_reports"


def start(app: str) -> SparkSession:
    """SparkSession mit den S3-Zugangsdaten des Jobs.

    Nur der Driver kennt die Zugangsdaten; die Executors auf den Workern erhalten sie über die
    Hadoop-Konfiguration des Jobs. Endpoint und S3A-Einstellungen stehen in spark-defaults.conf.
    """
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    logging.getLogger("py4j").setLevel(logging.WARNING)
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


def month_path(table: str, year: int, month: int) -> str:
    return f"{table}/{partition(year, month)}"


def write_month(df: DataFrame, table: str, year: int, month: int) -> None:
    """Überschreibt genau diesen Monat der Tabelle; alle anderen Monate bleiben unverändert.

    Dadurch ist jeder Job idempotent: Ein erneuter Lauf ersetzt den Monat, statt Zeilen zu
    verdoppeln. coalesce(1): eine Datei je Monat statt 200 kleiner Dateien nach dem Shuffle.
    """
    df.coalesce(1).write.mode("overwrite").parquet(month_path(table, year, month))
