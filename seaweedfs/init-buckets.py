#!/usr/bin/env python3
"""Legt die Buckets der Lake-Schichten an. Idempotent: vorhandene Buckets bleiben unverändert."""

import os
import subprocess
import sys
import time

MASTER = os.environ.get("SEAWEEDFS_MASTER", "seaweedfs:9333")
BUCKETS = os.environ.get("LAKE_BUCKETS", "bronze silver gold").split()
MAX_ATTEMPTS = 30


def log(message: str) -> None:
    print(f"init-buckets: {message}", flush=True)


def weed_shell(command: str) -> str:
    """Führt einen Befehl in weed shell aus und gibt dessen Ausgabe zurück."""
    result = subprocess.run(
        ["weed", "shell", f"-master={MASTER}"],
        input=command,
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout


def wait_for_master() -> None:
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            weed_shell("cluster.check")
            return
        except subprocess.CalledProcessError:
            log(f"warte auf {MASTER} (Versuch {attempt})")
            time.sleep(2)
    sys.exit(f"init-buckets: Master {MASTER} nach {MAX_ATTEMPTS} Versuchen nicht erreichbar")


def bucket_names() -> set[str]:
    """Namen aus s3.bucket.list, das je Bucket eine Zeile "  <name>\\tsize:…" ausgibt.

    Ein vorangestellter Prompt ("> ") wird entfernt, damit nur der exakte Name verglichen wird.
    """
    names = set()
    for line in weed_shell("s3.bucket.list").splitlines():
        fields = line.split("\t")
        if len(fields) > 1 and fields[1].startswith("size:") and fields[0].split():
            names.add(fields[0].split()[-1].lstrip(">"))
    return names


def main() -> None:
    wait_for_master()

    existing = bucket_names()
    for bucket in BUCKETS:
        if bucket in existing:
            log(f"{bucket} existiert bereits")
        else:
            weed_shell(f"s3.bucket.create -name {bucket}")
            log(f"{bucket} angelegt")

    # weed shell meldet Fehler einzelner Befehle nicht über den Exit-Code; deshalb nachprüfen.
    missing = sorted(set(BUCKETS) - bucket_names())
    if missing:
        sys.exit(f"init-buckets: {', '.join(missing)} fehlt nach dem Anlegen")


if __name__ == "__main__":
    main()
