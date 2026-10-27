#!/bin/sh
# Startet einen Spark-Job im Client-Modus: Der Driver läuft in diesem Container, die Executors auf
# den Workern. Die Executors verbinden sich zurück zum Driver, deshalb meldet er seine Adresse im
# gemeinsamen Netz. Ein fester Name wie der Service-Name fehlt bei docker compose run.
# Aufruf: submit-job.sh [spark-submit-Optionen] <job.py> [Job-Argumente]
set -eu

# Spark-Speichergrößen (1024k, 512m, 2g, 1t, auch mit b) in MiB umrechnen. Ohne Einheit gilt MiB
# wie bei Spark. Spark erlaubt nur ganze Zahlen.
to_mib() {
  value=$(echo "$1" | tr '[:upper:]' '[:lower:]')
  case "$value" in *[kmgt]b) value=${value%b} ;; esac
  number=${value%[kmgt]}
  case "$number" in
    '' | *[!0-9]*)
      echo "submit-job: ungültige Speichergröße '$1' (erlaubt: ganze Zahl mit k, m, g oder t)" >&2
      exit 2 ;;
  esac
  case "$value" in
    *k) echo $((number / 1024)) ;;
    *g) echo $((number * 1024)) ;;
    *t) echo $((number * 1024 * 1024)) ;;
    *) echo "$number" ;;
  esac
}

# Fordert der Executor mehr, als ein Worker hat, bekommt der Job nie Ressourcen und wartet endlos.
# Deshalb hier mit einer klaren Meldung abbrechen.
executor_memory=${SPARK_EXECUTOR_MEMORY:-}
worker_memory=${SPARK_WORKER_MEMORY:-}
if [ -n "$executor_memory" ] && [ -n "$worker_memory" ]; then
  executor_mib=$(to_mib "$executor_memory")
  worker_mib=$(to_mib "$worker_memory")
  if [ "$executor_mib" -gt "$worker_mib" ]; then
    echo "submit-job: SPARK_EXECUTOR_MEMORY=$executor_memory ist größer als" \
      "SPARK_WORKER_MEMORY=$worker_memory, kein Worker könnte den Executor starten" >&2
    exit 2
  fi
fi

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
