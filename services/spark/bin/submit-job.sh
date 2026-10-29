#!/bin/sh
# Startet einen Spark-Job im Client-Modus: Der Driver läuft in diesem Container, die Executors auf
# den Workern. Die Executors verbinden sich zurück zum Driver, deshalb meldet er seine Adresse im
# gemeinsamen Netz. Ein fester Name wie der Service-Name fehlt bei docker compose run.
# Aufruf: submit-job.sh [spark-submit-Optionen] <job.py> [Job-Argumente]
set -eu

executor_memory=${SPARK_EXECUTOR_MEMORY:-}
master="spark://${SPARK_MASTER_HOST:-spark-master}:${SPARK_MASTER_PORT:-7077}"
host=$(master-route-address.sh)
echo "submit-job: Driver auf $host, Master $master"

# Ohne gesetzten Wert gilt der Standard von Spark; "${var:+…}" lässt die Option dann weg.
exec /opt/spark/bin/spark-submit \
  --master "$master" \
  ${executor_memory:+--conf "spark.executor.memory=$executor_memory"} \
  --conf "spark.driver.host=$host" \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.driver.port=7078 \
  --conf spark.blockManager.port=7079 \
  "$@"
