#!/bin/sh
# Renders the S3 identities from environment variables and starts SeaweedFS.
set -eu

TEMPLATE=/etc/seaweedfs/s3.template.json
CONFIG=/etc/seaweedfs/s3.json

fail() {
  echo "entrypoint: $*" >&2
  exit 1
}

key=${WEED_JWT_FILER_SIGNING_KEY:-}
[ "${#key}" -ge 32 ] || fail "WEED_JWT_FILER_SIGNING_KEY is missing or shorter than 32 characters."

(umask 077; cp "$TEMPLATE" "$CONFIG")

# Every __NAME__ in the template must come from the environment.
for placeholder in $(grep -o '__[A-Z0-9_]*__' "$TEMPLATE" | sort -u); do
  var=${placeholder#__}
  var=${var%__}
  value=$(printenv "$var" || true)
  [ -n "$value" ] || fail "missing value for $var"
  # Restricted charset keeps sed and JSON safe without escaping.
  case "$value" in
    *[!A-Za-z0-9_-]*) fail "invalid value for $var (allowed: A-Z, a-z, 0-9, _ and -)" ;;
  esac
  sed -i "s|${placeholder}|${value}|g" "$CONFIG"
done

exec weed server \
  -dir=/data \
  -master.volumeSizeLimitMB=1024 \
  -volume.max=100 \
  -s3 \
  -s3.port=8333 \
  -s3.config="$CONFIG"
