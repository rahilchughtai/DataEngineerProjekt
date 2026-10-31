#!/bin/sh
# Legt die Buckets der Lake-Schichten an. Idempotent: vorhandene Buckets bleiben unverändert.
set -eu

MASTER=${SEAWEEDFS_MASTER:-seaweedfs:9333}
BUCKETS=${LAKE_BUCKETS:-"bronze silver gold"}

attempt=1
until echo "cluster.check" | weed shell -master="$MASTER" >/dev/null 2>&1; do
  if [ "$attempt" -ge 30 ]; then
    echo "init-buckets: Master $MASTER nach $attempt Versuchen nicht erreichbar" >&2
    exit 1
  fi
  echo "init-buckets: warte auf $MASTER (Versuch $attempt)"
  attempt=$((attempt + 1))
  sleep 2
done

# Bucket-Namen aus s3.bucket.list, eine Zeile je Bucket: "  <name>\tsize:…". Ein vorangestellter
# Prompt ("> ") wird entfernt, damit nur der exakte Name verglichen wird (gold ≠ gold-archive).
bucket_names() {
  echo "s3.bucket.list" | weed shell -master="$MASTER" |
    awk -F '\t' '$2 ~ /^size:/ { name = $1; sub(/^.*[> ]/, "", name); print name }'
}

existing=$(bucket_names)
for bucket in $BUCKETS; do
  if printf '%s\n' "$existing" | grep -qxF "$bucket"; then
    echo "init-buckets: $bucket existiert bereits"
  else
    echo "s3.bucket.create -name $bucket" | weed shell -master="$MASTER"
    echo "init-buckets: $bucket angelegt"
  fi
done

# weed shell meldet Fehler einzelner Befehle nicht über den Exit-Code; deshalb nachprüfen.
existing=$(bucket_names)
for bucket in $BUCKETS; do
  if ! printf '%s\n' "$existing" | grep -qxF "$bucket"; then
    echo "init-buckets: $bucket fehlt nach dem Anlegen" >&2
    exit 1
  fi
done
