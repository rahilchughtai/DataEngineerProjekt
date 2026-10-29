# Ingestion-Service

Lädt die monatlichen NYC TLC Yellow Taxi Trip Records per HTTPS und legt sie unverändert in `s3://bronze/yellow_tripdata/year=YYYY/month=MM/` ab.

- **Idempotent:** Der Key hängt nur vom Monat ab. Ein Monat wird übersprungen, wenn der ETag der Quelle mit dem in Bronze gespeicherten übereinstimmt. Mit `--force` wird er neu geladen und das Objekt atomar überschrieben, Duplikate entstehen nicht.
- **Retries:** HEAD und Download werden bei Netzwerkfehlern, HTTP 5xx/429 und unvollständiger Übertragung mit exponentiellem Backoff wiederholt. HTTP 403/404 gilt als „noch nicht veröffentlicht“ und wird ohne Retry protokolliert.
- **Integrität und Lineage:** Die Parquet-Magic-Bytes werden geprüft, die SHA-256-Summe wird beim Streamen berechnet. Jedes Objekt trägt die Metadaten `source-url`, `source-etag`, `source-last-modified`, `sha256`, `size-bytes`, `ingested-at` und `ingested-by`. Pro Lauf entsteht ein JSON-Protokoll mit Status und Latenzen unter `s3://bronze/_runs/yellow_tripdata/<run_id>.json`. Die Run-ID übernimmt der Service aus `PIPELINE_RUN_ID`, die der Orchestrator setzt; ohne die Variable, etwa bei einem manuellen Lauf, erzeugt er eine eigene.
- **Exit-Code:** `0` bei Erfolg, `1` wenn mindestens ein Monat fehlschlägt, `2` bei falscher Konfiguration (auch ungültige `PIPELINE_RUN_ID`), `3` wenn mindestens ein Monat noch nicht veröffentlicht ist. Wie bei `silver-job` gilt ein unvollständiger Zeitraum nicht als Erfolg, die Pipeline hält also schon hier an.

## Aufruf

```sh
python -m ingestion --quarter 2024Q1
python -m ingestion --month 2024-01 --force
```

Ohne Argumente gilt `INGEST_PERIOD` (Quartal `YYYYQn` oder Monat `YYYY-MM`); fehlt der Zeitraum oder ist er ungültig, bricht der Service mit Exit-Code `2` ab.

Für einen vollständigen Pipeline-Lauf im Repo-Root den [Orchestrator](../orchestrator/README.md) verwenden: `uv run --project services/orchestrator orchestrator --quarter 2024Q1`. Ein Einzellauf mit `docker compose run --rm ingestion --month 2024-01` startet nur die Ingestion samt SeaweedFS, keine Spark-Jobs (Images vorher mit `docker compose --profile jobs build` bauen).

## Umgebungsvariablen

| Variable | Standard | Bedeutung |
|---|---|---|
| `S3_ENDPOINT` | – | S3-Endpoint von SeaweedFS |
| `S3_INGESTION_ACCESS_KEY` / `S3_INGESTION_SECRET_KEY` | – | eigene Identität, darf nur `bronze` |
| `BRONZE_BUCKET` | `bronze` | Ziel-Bucket |
| `INGEST_PERIOD` | – | Zeitraum (`YYYYQn` oder `YYYY-MM`), wenn kein `--quarter`/`--month` übergeben wird |
| `TLC_BASE_URL` | CloudFront-URL der TLC | Quelle |
| `TLC_DATASET` | `yellow_tripdata` | Dateipräfix; in Compose fest, weil Spark-Job 1 nur das Yellow-Schema kennt |
| `DOWNLOAD_MAX_ATTEMPTS` | `5` | Versuche pro Anfrage |
| `DOWNLOAD_BACKOFF_SECONDS` | `2` | Basis des exponentiellen Backoffs |
| `DOWNLOAD_TIMEOUT_SECONDS` | `60` | Timeout pro Anfrage |
| `LOG_LEVEL` | `INFO` | Log-Level |

Die `DOWNLOAD_*`-Werte reicht Compose nicht aus `.env` durch, es gelten die Standards. Für Experimente lassen sie sich per `docker compose run --rm -e DOWNLOAD_TIMEOUT_SECONDS=120 ingestion` setzen.
