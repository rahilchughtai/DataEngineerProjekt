"""SparkSession mit S3A-Zugangsdaten aus der Umgebung.

Die Zugangsdaten setzt nur der Driver. Die Executors erhalten sie über die Hadoop-Konfiguration
des Jobs, deshalb brauchen die Worker-Container selbst keine S3-Credentials.
"""

import os

from pyspark.sql import SparkSession


class ConfigError(ValueError):
    pass


def _required(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise ConfigError(f"Umgebungsvariable {name} fehlt (siehe .env.example)")
    return value


def s3_conf() -> dict[str, str]:
    return {
        "spark.hadoop.fs.s3a.endpoint": _required("S3_ENDPOINT"),
        "spark.hadoop.fs.s3a.endpoint.region": os.environ.get("S3_REGION", "us-east-1"),
        "spark.hadoop.fs.s3a.access.key": _required("S3_SPARK_ACCESS_KEY"),
        "spark.hadoop.fs.s3a.secret.key": _required("S3_SPARK_SECRET_KEY"),
    }


def build_session(app_name: str) -> SparkSession:
    builder = SparkSession.builder.appName(app_name)
    for key, value in s3_conf().items():
        builder = builder.config(key, value)
    return builder.getOrCreate()
