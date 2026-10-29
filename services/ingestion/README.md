# Ingestion-Service

Lädt die monatlichen NYC TLC Yellow Taxi Trip Records per HTTPS und legt sie unverändert in `s3://bronze/yellow_tripdata/year=YYYY/month=MM/` ab.

- **Idempotent:** Der Key hängt nur vom Monat ab. Ein Monat wird übersprungen, wenn der ETag der Quelle mit dem in Bronze gespeicherten übereinstimmt. Hat sich die Quelle geändert, wird er neu geladen und das Objekt atomar überschrieben, Duplikate entstehen nicht.
- **Retries:** HEAD und Download werden bei Netzwerkfehlern, HTTP 5xx/429 und unvollständiger Übertragung mit exponentiellem Backoff wiederholt. HTTP 403/404 gilt als „noch nicht veröffentlicht“ und wird ohne Retry protokolliert.
- **Integrität und Lineage:** Die Parquet-Magic-Bytes werden geprüft, die SHA-256-Summe wird beim Streamen berechnet. Jedes Objekt trägt die Metadaten `source-url`, `source-etag`, `source-last-modified`, `sha256`, `size-bytes`, `ingested-at` und `ingested-by`. Pro Lauf entsteht ein JSON-Protokoll mit Status und Latenzen unter `s3://bronze/_runs/yellow_tripdata/<run_id>.json`. Die Run-ID liest der Service aus der Datei in `PIPELINE_RUN_ID_FILE`, die `run-init` zu Beginn des Laufs schreibt.
- **Exit-Code:** `0` bei Erfolg, `1` wenn mindestens ein Monat fehlschlägt, `2` bei falscher Konfiguration (auch fehlende oder ungültige Run-ID-Datei), `3` wenn mindestens ein Monat noch nicht veröffentlicht ist. Wie bei `silver-job` gilt ein unvollständiger Zeitraum nicht als Erfolg, die Pipeline hält also schon hier an.

## Aufruf

Der Service ist der erste Schritt von `docker compose up` und startet, sobald `lake-init` und `run-init` erfolgreich beendet sind. Den Zeitraum liest er aus `INGEST_PERIOD` (Quartal `YYYYQn` oder Monat `YYYY-MM`); fehlt er oder ist er ungültig, bricht der Service mit Exit-Code `2` ab.

## Umgebungsvariablen

| Variable | Standard | Bedeutung |
|---|---|---|
| `S3_ENDPOINT` | – | S3-Endpoint von SeaweedFS |
| `S3_INGESTION_ACCESS_KEY` / `S3_INGESTION_SECRET_KEY` | – | eigene Identität, darf nur `bronze` |
| `BRONZE_BUCKET` | `bronze` | Ziel-Bucket |
| `INGEST_PERIOD` | – | Zeitraum des Laufs (`YYYYQn` oder `YYYY-MM`), Pflicht |
| `PIPELINE_RUN_ID_FILE` | – | Datei mit der Run-ID des Laufs, Pflicht, in Compose `/run/pipeline/run-id` |
| `TLC_BASE_URL` | – | Quelle der Monatsdateien, aus `.env` (siehe `.env.example`) |
| `TLC_DATASET` | `yellow_tripdata` | Dateipräfix; in Compose fest, weil Spark-Job 1 nur das Yellow-Schema kennt |
| `DOWNLOAD_MAX_ATTEMPTS` | `5` | Versuche pro Anfrage |
| `DOWNLOAD_BACKOFF_SECONDS` | `2` | Basis des exponentiellen Backoffs |
| `DOWNLOAD_TIMEOUT_SECONDS` | `60` | Timeout pro Anfrage |
| `LOG_LEVEL` | `INFO` | Log-Level |

Die `DOWNLOAD_*`-Werte reicht Compose nicht aus `.env` durch, es gelten die Standards. Für Experimente lassen sie sich in einer Override-Datei setzen, wie es der End-to-End-Test für `DOWNLOAD_BACKOFF_SECONDS` tut.
