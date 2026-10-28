# Spark-Cluster und Jobs

Ein Image (`localhost/taxi-pipeline/spark:0.1.0`, Basis `apache/spark:4.1.3`) für den Master, die Worker und die Job-Container. Spark läuft im Standalone-Modus. Ein Hadoop-Cluster (HDFS, YARN) wird nicht eingesetzt: Spark liest Dateien über die Hadoop-FileSystem-API und braucht für S3 nur den S3A-Connector (`hadoop-aws` 3.4.2 plus AWS SDK 2.29.52). Beide Jars werden beim Build per SHA-256 geprüft ins Image gelegt, sodass zur Laufzeit nichts nachgeladen wird.

- **Cluster:** `spark-master` (UI unter `http://127.0.0.1:8080`) und `spark-worker` mit zwei Replikas, skalierbar mit `docker compose up --scale spark-worker=N`. Die Worker bekommen keine S3-Zugangsdaten, die Executors erhalten sie über die Konfiguration des Jobs. Spark blendet sie in der UI aus (`spark.redaction.regex`).
- **Netze:** Der Master hängt nur im Rechennetz `sparknet`, Worker und Job-Container zusätzlich im Speichernetz `lakenet`. Weil diese Container in zwei Netzen hängen, melden sie dem Master gezielt ihre Adresse in `sparknet` ([bin/master-route-address.sh](bin/master-route-address.sh)).
- **Restrisiko:** Die Spark-RPC-Schnittstelle des Masters (Port 7077) ist nicht authentifiziert (`spark.authenticate` ist aus). Ein Container in `sparknet` könnte Code auf den Workern ausführen, die auch in `lakenet` hängen. Das begrenzen drei Dinge: In `sparknet` liegen nur Master, Worker und Jobs, die Worker besitzen keine S3-Zugangsdaten, und die Master-UI ist nur an `127.0.0.1` gebunden. Für den lokalen Betrieb ist das gewollt, in einer Cloud wären `spark.authenticate` und TLS Pflicht.
- **Jobs** laufen im Client-Modus über [bin/submit-job.sh](bin/submit-job.sh): Der Driver läuft im One-Shot-Container, die Executors laufen auf den Workern.

## Job 1: Bronze → Silver (`silver-job`)

[bronze_to_silver.py](src/spark_jobs/bronze_to_silver.py) verarbeitet jeden Monat einzeln:

1. Lesen von `s3a://bronze/yellow_tripdata/year=YYYY/month=MM/` und Harmonisierung auf ein festes Schema ([schema.py](src/spark_jobs/schema.py)). Die TLC-Dateien wechseln über die Jahre Typen und Schreibweisen von Spalten. Fehlt eine Pflichtspalte, gilt der Monat als `failed`.
2. **Datenminimierung:** Übernommen werden nur Pickup- und Dropoff-Zeit, Pickup- und Dropoff-Zone, Passagierzahl, Distanz, Fahrpreis, Gesamtbetrag und Zahlungsart. `VendorID`, `RatecodeID`, `store_and_fwd_flag` und die Einzelgebühren entfallen.
3. **Deduplizierung** exakt gleicher Bronze-Zeilen über alle Quellspalten, vor der Datenminimierung. Fahrten, die sich nur in verworfenen Spalten wie `VendorID` unterscheiden, bleiben als eigene Fahrten erhalten.
4. **Qualitätsprüfungen** ([quality.py](src/spark_jobs/quality.py)), siehe Tabelle unten. Die erste verletzte Regel wird als `reject_reason` gespeichert.
5. **Lineage-Spalten:** `_source_file` (Bronze-Datei), `_run_id`, `_processed_at`.
6. **Schreiben**, partitioniert nach `year=YYYY/month=MM` wie in Bronze. `year` und `month` sind Strings; `spark.sql.sources.partitionColumnTypeInference.enabled=false` ([conf/spark-defaults.conf](conf/spark-defaults.conf)) verhindert, dass Spark sie beim Lesen zu Zahlen macht:
   - gültige Zeilen nach `s3a://silver/yellow_tripdata/`
   - abgelehnte Zeilen mit Grund nach `s3a://silver/_quarantine/yellow_tripdata/`
   - Report pro Lauf nach `s3a://silver/_runs/yellow_tripdata/<run_id>.json`, mit `rows_in`, `rows_valid`, `rows_rejected`, `duplicates`, Ablehnungen je Regel und Laufzeit je Monat

**Idempotenz:** `mode("overwrite")` mit `partitionOverwriteMode=dynamic` ersetzt nur die Partition des verarbeiteten Monats, alle anderen bleiben unverändert. Hat ein Monat beim erneuten Lauf keine abgelehnten Zeilen mehr, wird die alte Quarantäne-Partition gelöscht. Der Report rechnet sich auf: `rows_in = rows_valid + duplicates + rows_rejected`.

**Exit-Code:** `0` bei Erfolg, `1` wenn mindestens ein Monat fehlschlägt, `2` bei falscher Konfiguration, `3` wenn für mindestens einen Monat die Bronze-Daten fehlen (`missing`). Ein unvollständiges Quartal gilt nicht als Erfolg, sonst würden Gold und das Training still auf einem Teil der Monate weiterlaufen. Das Compose-Service hat `restart: "no"`, Wiederholungen übernimmt der Orchestrator. Schlägt das Schreiben eines Monats fehl, werden seine Silver- und Quarantäne-Partition gelöscht: Der Monat fehlt dann ganz, statt halb aktualisiert zu sein, bis ein erneuter Lauf ihn schreibt.

**Umwandlungsfehler:** Spark 4 rechnet standardmäßig im ANSI-Modus, ein normaler `cast` bricht dort bei einem unpassenden Wert den ganzen Monat ab. Die Harmonisierung nutzt deshalb `try_cast`: Der Wert wird `null` und die Zeile von den Regeln unten abgelehnt.

**Schreibpfad:** S3A „benennt" beim Commit mit dem Standard-Committer per Kopieren und Löschen um. Auf SeaweedFS ist das langsamer als ein echtes Rename und nicht atomar. Die Laufzeit je Monat steht im Run-Report (`seconds`). Ein Magic-Committer wäre die Alternative, braucht aber ein weiteres Jar und ist nicht aktiviert.

### Qualitätsregeln

| Regel | Abgelehnt, wenn | Begründung |
|---|---|---|
| `pickup_null` | Pickup-Zeit fehlt | ohne Zeitstempel keine zeitliche Zuordnung |
| `dropoff_null` | Dropoff-Zeit fehlt | Fahrtdauer nicht bestimmbar |
| `pickup_outside_month` | Pickup liegt nicht im Monat der Datei | TLC-Dateien enthalten Ausreißer aus anderen Monaten und Jahren; sonst entstünden Duplikate über Partitionen |
| `negative_duration` | Dropoff vor Pickup | technisch unmöglich |
| `duration_over_24h` | Fahrt länger als 24 Stunden | Taxameter nicht beendet |
| `distance_invalid` | Distanz fehlt, < 0 oder > 200 Meilen | Messfehler |
| `amount_invalid` | Fahrpreis oder Gesamtbetrag fehlt, ist NaN oder < 0 | Stornos und Korrekturbuchungen, keine Fahrten |
| `location_invalid` | Pickup- oder Dropoff-Zone fehlt oder liegt außerhalb 1 bis 265 | keine gültige TLC-Zone |
| `passenger_count_invalid` | negativ oder mehr als 9 Passagiere | Eingabefehler; ein fehlender Wert bleibt erlaubt, weil er in der Quelle häufig fehlt |

Beim Lauf über 2024Q1 (9,55 Mio. Fahrten) wurden 1,4 % der Zeilen abgelehnt, fast alle wegen `amount_invalid`.

## Aufruf

```sh
docker compose up silver-job                          # Zeitraum aus INGEST_QUARTER bzw. INGEST_MONTH
docker compose up -d spark-worker                     # Cluster für Einzelläufe
docker compose run --rm --no-deps silver-job --month 2024-01    # einzelner Monat, z. B. zur Wiederholung
docker compose run --rm --no-deps silver-job --quarter 2024Q1
```

`--no-deps` verhindert, dass vorher `ingestion` für den Zeitraum aus `.env` läuft; die Bronze-Daten des gewünschten Zeitraums müssen schon vorliegen.

## Umgebungsvariablen

| Variable | Standard | Bedeutung |
|---|---|---|
| `S3_ENDPOINT` | – | S3-Endpoint von SeaweedFS |
| `S3_SPARK_ACCESS_KEY` / `S3_SPARK_SECRET_KEY` | – | Identität `spark`: liest Bronze, schreibt Silver und Gold |
| `BRONZE_BUCKET` / `SILVER_BUCKET` | `bronze` / `silver` | Buckets |
| `TLC_DATASET` | `yellow_tripdata` | Präfix der Daten |
| `INGEST_QUARTER` / `INGEST_MONTH` | – | Zeitraum, wenn kein `--quarter`/`--month` übergeben wird |
| `SPARK_WORKER_CORES` / `SPARK_WORKER_MEMORY` | `2` / `2g` | Ressourcen pro Worker |
| `SPARK_EXECUTOR_MEMORY` | `1g` | Speicher pro Executor |
| `LOG_LEVEL` | `INFO` | Log-Level des Jobs; Spark selbst loggt auf WARN ([conf/log4j2.properties](conf/log4j2.properties)) |

## Tests

Der Code muss zu Python 3.10 passen, der Version im Spark-Image. In CI laufen die Tests über uv mit PySpark 4.1.3 und Java 17. Lokal unter Windows fehlen dafür Java und die Hadoop-Hilfsprogramme. Die Tests laufen deshalb im Spark-Image, das beides mitbringt:

```sh
podman run --rm -v "$PWD/services/spark:/work:ro" -w /work -e HOME=/tmp -e PYTHONDONTWRITEBYTECODE=1 \
  --entrypoint sh docker.io/apache/spark:4.1.3-scala2.13-java17-python3-ubuntu -c \
  'pip install -q --user pytest && PYTHONPATH=src:/opt/spark/python/lib/pyspark.zip:/opt/spark/python/lib/py4j-0.10.9.9-src.zip python3 -m pytest -p no:cacheprovider'
```

Mit installiertem Java 17 (Linux, macOS, WSL) reicht `uv run --directory services/spark pytest`.
