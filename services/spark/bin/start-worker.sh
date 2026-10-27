#!/bin/sh
# Startet einen Spark-Worker, der dem Master seine Adresse im gemeinsamen Netz meldet.
set -eu

master="spark://${SPARK_MASTER_HOST:-spark-master}:${SPARK_MASTER_PORT:-7077}"
host=$(master-route-address.sh)
echo "start-worker: melde Adresse $host an $master"

exec /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker \
  --host "$host" \
  --webui-port 8081 \
  "$master"
