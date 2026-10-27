#!/bin/sh
# Rendert die S3-Identitäten aus Umgebungsvariablen und startet SeaweedFS.
# Zugangsdaten stehen nur in .env, nie im Image oder Repository.
set -eu

# Ohne Signing-Key verwirft SeaweedFS 4.x die Identitäten und läuft ohne Authentifizierung.
# Deshalb hier hart abbrechen statt offen zu starten (fail closed).
signing_key=${WEED_JWT_FILER_SIGNING_KEY:-}
if [ "${#signing_key}" -lt 32 ]; then
  echo "entrypoint: SEAWEEDFS_SIGNING_KEY fehlt oder ist kürzer als 32 Zeichen" >&2
  exit 1
fi

CONFIG=/etc/seaweedfs/s3.json
cp /etc/seaweedfs/s3.json.template "$CONFIG"

for var in S3_ADMIN_ACCESS_KEY S3_ADMIN_SECRET_KEY \
           S3_INGESTION_ACCESS_KEY S3_INGESTION_SECRET_KEY \
           S3_SPARK_ACCESS_KEY S3_SPARK_SECRET_KEY \
           S3_ML_ACCESS_KEY S3_ML_SECRET_KEY; do
  value=$(printenv "$var" || true)
  if [ -z "$value" ]; then
    echo "entrypoint: $var ist nicht gesetzt (siehe .env.example)" >&2
    exit 1
  fi
  case "$value" in
    *[!A-Za-z0-9_-]*)
      echo "entrypoint: $var darf nur A-Z, a-z, 0-9, _ und - enthalten" >&2
      exit 1
      ;;
  esac
  sed -i "s|__${var}__|${value}|g" "$CONFIG"
done
chmod 600 "$CONFIG"

exec weed server \
  -dir=/data \
  -master.volumeSizeLimitMB=1024 \
  -volume.max=0 \
  -s3 \
  -s3.port=8333 \
  -s3.config="$CONFIG"
