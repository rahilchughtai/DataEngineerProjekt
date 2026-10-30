"""SparkSession mit S3A-Zugangsdaten aus der Umgebung.

Die Zugangsdaten setzt nur der Driver. Die Executors erhalten sie über die Hadoop-Konfiguration
des Jobs, deshalb brauchen die Worker-Container selbst keine S3-Credentials.
"""

from pyspark.sql import SparkSession

from pipeline_common.env import optional, required


def s3_conf() -> dict[str, str]:
    return {
        "spark.hadoop.fs.s3a.endpoint": required("S3_ENDPOINT"),
        "spark.hadoop.fs.s3a.endpoint.region": optional("S3_REGION", "us-east-1"),
        "spark.hadoop.fs.s3a.access.key": required("S3_SPARK_ACCESS_KEY"),
        "spark.hadoop.fs.s3a.secret.key": required("S3_SPARK_SECRET_KEY"),
    }


def build_session(app_name: str) -> SparkSession:
    builder = SparkSession.builder.appName(app_name)
    for key, value in s3_conf().items():
        builder = builder.config(key, value)
    return builder.getOrCreate()
