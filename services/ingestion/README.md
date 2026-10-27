# Ingestion-Service

Lädt die monatlichen NYC TLC Yellow Taxi Trip Records per HTTPS und legt sie unverändert in `s3://bronze/yellow_tripdata/year=YYYY/month=MM/` ab.

- **Idempotent:** Der Key hängt nur vom Monat ab. Ein Monat wird übersprungen, wenn der ETag der Quelle mit dem in Bronze gespeicherten übereinstimmt. Mit `--force` wird er neu geladen und das Objekt atomar überschrieben, Duplikate entstehen nicht.
- **Retries:** HEAD und Download werden bei Netzwerkfehlern, HTTP 5xx/429 und unvollständiger Übertragung mit exponentiellem Backoff wiederholt. HTTP 403/404 gilt als „noch nicht veröffentlicht“ und wird ohne Retry protokolliert.
- **Integrität und Lineage:** Die Parquet-Magic-Bytes werden geprüft, die SHA-256-Summe wird beim Streamen berechnet. Jedes Objekt trägt die Metadaten `source-url`, `source-etag`, `source-last-modified`, `sha256`, `size-bytes`, `ingested-at` und `ingested-by`. Pro Lauf entsteht ein JSON-Protokoll mit Status und Latenzen unter `s3://bronze/_runs/yellow_tripdata/`.
- **Exit-Code:** `0` bei Erfolg, `1` wenn mindestens ein Monat fehlschlägt, `2` bei falscher Konfiguration.

## Aufruf

```sh
python -m ingestion --quarter 2024Q1
python -m ingestion --month 2024-01 --force
```

Ohne Argumente gelten `INGEST_QUARTER` bzw. `INGEST_MONTH`; sind beide gesetzt, bricht der Service mit Exit-Code `2` ab.

## Umgebungsvariablen

| Variable | Standard | Bedeutung |
|---|---|---|
| `S3_ENDPOINT` | – | S3-Endpoint von SeaweedFS |
| `S3_INGESTION_ACCESS_KEY` / `S3_INGESTION_SECRET_KEY` | – | eigene Identität, darf nur `bronze` |
| `BRONZE_BUCKET` | `bronze` | Ziel-Bucket |
| `TLC_BASE_URL` | CloudFront-URL der TLC | Quelle |
| `TLC_DATASET` | `yellow_tripdata` | Dateipräfix |
| `DOWNLOAD_MAX_ATTEMPTS` | `5` | Versuche pro Anfrage |
| `DOWNLOAD_BACKOFF_SECONDS` | `2` | Basis des exponentiellen Backoffs |
| `DOWNLOAD_TIMEOUT_SECONDS` | `60` | Timeout pro Anfrage |
| `LOG_LEVEL` | `INFO` | Log-Level |
