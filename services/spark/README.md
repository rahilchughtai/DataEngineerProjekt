# Spark-Cluster und Jobs

Ein Image (`localhost/taxi-pipeline/spark:0.1.0`, Basis `apache/spark:4.1.3`) für den Master, die Worker und die Job-Container. Spark läuft im Standalone-Modus. Ein Hadoop-Cluster (HDFS, YARN) wird nicht eingesetzt: Spark liest Dateien über die Hadoop-FileSystem-API und braucht für S3 nur den S3A-Connector (`hadoop-aws` 3.4.2 plus AWS SDK 2.29.52). Beide Jars werden beim Build per SHA-256 geprüft ins Image gelegt, sodass zur Laufzeit nichts nachgeladen wird.

- **Cluster:** `spark-master` (UI unter `http://127.0.0.1:8080`) und `spark-worker` mit zwei Replikas, skalierbar mit `docker compose up --scale spark-worker=N`. Die Worker bekommen keine S3-Zugangsdaten, die Executors erhalten sie über die Konfiguration des Jobs. Spark blendet sie in der UI aus (`spark.redaction.regex`).
- **Netze:** Der Master hängt nur im Rechennetz `sparknet`, Worker und Job-Container zusätzlich im Speichernetz `lakenet`. Weil diese Container in zwei Netzen hängen, melden sie dem Master gezielt ihre Adresse in `sparknet` ([bin/master-route-address.sh](bin/master-route-address.sh)).
- **Restrisiko:** Die Spark-RPC-Schnittstelle des Masters (Port 7077) ist nicht authentifiziert (`spark.authenticate` ist aus). Ein Container in `sparknet` könnte Code auf den Workern ausführen, die auch in `lakenet` hängen. Das begrenzen drei Dinge: In `sparknet` liegen nur Master, Worker und Jobs, die Worker besitzen keine S3-Zugangsdaten, und die Master-UI ist nur an `127.0.0.1` gebunden. Für den lokalen Betrieb ist das gewollt, in einer Cloud wären `spark.authenticate` und TLS Pflicht.
- **Jobs** laufen im Client-Modus über [bin/submit-job.sh](bin/submit-job.sh): Der Driver läuft im One-Shot-Container, die Executors laufen auf den Workern. Zeiträume, Run-ID, Logging und Run-Report kommen aus der gemeinsamen Bibliothek [libs/pipeline_common](../../libs/pipeline_common), ebenso der Exit-Code; das partitionsweise Überschreiben steckt in [lake.py](src/spark_jobs/lake.py).

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
   - Report pro Lauf nach `s3a://silver/_runs/yellow_tripdata/<run_id>.json`, mit `rows_in`, `rows_valid`, `rows_rejected`, `duplicates`, Ablehnungen je Regel und Laufzeit je Monat. Die Run-ID hat die Form `YYYYMMDDTHHMMSSZ-xxxxxx`: nach Startzeit sortierbar, der Zufallsteil verhindert, dass zwei Läufe in derselben Sekunde ihre Reports überschreiben. Die Run-ID des Laufs liest der Job aus der Datei in `PIPELINE_RUN_ID_FILE`, die `run-init` schreibt; fehlen Variable oder Datei oder ist die Run-ID ungültig, endet der Job mit Exit-Code `2`, bevor Spark startet

**Idempotenz:** `mode("overwrite")` mit `partitionOverwriteMode=dynamic` ersetzt nur die Partition des verarbeiteten Monats, alle anderen bleiben unverändert. Hat ein Monat beim erneuten Lauf keine abgelehnten Zeilen mehr, wird die alte Quarantäne-Partition gelöscht. Der Report rechnet sich auf: `rows_in = rows_valid + duplicates + rows_rejected`.

**Exit-Code:** `0` bei Erfolg, `1` wenn mindestens ein Monat fehlschlägt, `2` bei falscher Konfiguration, `3` wenn für mindestens einen Monat die Bronze-Daten fehlen (`missing`). Ein unvollständiges Quartal gilt nicht als Erfolg, sonst würden Gold und das Training still auf einem Teil der Monate weiterlaufen. Compose startet den Job nicht neu (`restart: "no"`), und die folgenden Schritte starten nicht, weil sie auf `service_completed_successfully` warten. Schlägt das Schreiben eines Monats fehl, werden seine Silver- und Quarantäne-Partition gelöscht: Der Monat fehlt dann ganz, statt halb aktualisiert zu sein, bis ein erneuter Lauf ihn schreibt.

**Leere Monate:** Enthält die Bronze-Datei keine gültige Zeile (leer oder komplett abgelehnt), gilt der Monat als `failed` (Exit-Code `1`). Geschrieben wird dann nichts, Silver und Quarantäne behalten ihren bisherigen Stand; die Zähler stehen trotzdem im Report. Eine Schwelle für den Anteil abgelehnter Zeilen gibt es nicht, der Anteil lässt sich aus `rows_rejected` und `rows_in` im Report ablesen.

**Umwandlungsfehler:** Spark 4 rechnet standardmäßig im ANSI-Modus, ein normaler `cast` bricht dort bei einem unpassenden Wert den ganzen Monat ab. Die Harmonisierung nutzt deshalb `try_cast`: Der Wert wird `null`. Ob die Zeile dann abgelehnt wird, entscheiden die Regeln unten. Bei Zeit, Zonen, Distanz und Beträgen ist `null` ein Ablehnungsgrund. `passenger_count` darf dagegen fehlen, und `payment_type` hat keine Regel; ein nicht umwandelbarer Wert bleibt dort als `null` in Silver und ist von einem in der Quelle fehlenden Wert nicht zu unterscheiden. Beide Spalten fließen nicht in Gold ein.

**Schreibpfad:** S3A „benennt" beim Commit mit dem Standard-Committer per Kopieren und Löschen um. Auf SeaweedFS ist das langsamer als ein echtes Rename und nicht atomar. Die Laufzeit je Monat steht im Run-Report (`seconds`). Weil jede Datei einzeln kopiert wird, schreibt der Job je angefangene Million Zeilen eine Datei (knapp 20 MB) statt der 200 Dateien, die Spark nach dem Shuffle der Deduplizierung sonst anlegt; die Quarantäne passt in eine Datei. Das halbierte die Laufzeit je Monat ungefähr. Ein Magic-Committer wäre die Alternative, braucht aber ein weiteres Jar und ist nicht aktiviert.

### Qualitätsregeln

| Regel | Abgelehnt, wenn | Begründung |
|---|---|---|
| `pickup_null` | Pickup-Zeit fehlt | ohne Zeitstempel keine zeitliche Zuordnung |
| `dropoff_null` | Dropoff-Zeit fehlt | Fahrtdauer nicht bestimmbar |
| `pickup_outside_month` | Pickup liegt nicht im Monat der Datei | TLC-Dateien enthalten Ausreißer aus anderen Monaten und Jahren; sonst entstünden Duplikate über Partitionen |
| `negative_duration` | Dropoff vor Pickup | technisch unmöglich |
| `duration_over_24h` | Fahrt länger als 24 Stunden | Taxameter nicht beendet |
| `distance_invalid` | Distanz fehlt, < 0 oder > 200 Meilen | Messfehler |
| `amount_invalid` | Fahrpreis oder Gesamtbetrag fehlt, ist NaN, unendlich oder < 0 | Stornos und Korrekturbuchungen, keine Fahrten; unendliche Werte würden die Mittelwerte in Gold verfälschen. Eine Obergrenze gibt es nicht |
| `location_invalid` | Pickup- oder Dropoff-Zone fehlt oder liegt außerhalb 1 bis 265 | keine gültige TLC-Zone |
| `passenger_count_invalid` | negativ oder mehr als 9 Passagiere | Eingabefehler; ein fehlender Wert bleibt erlaubt, weil er in der Quelle häufig fehlt |

Beim Lauf über 2024Q1 (9,55 Mio. Fahrten) wurden 1,4 % der Zeilen abgelehnt, fast alle wegen `amount_invalid`.

## Job 2: Silver → Gold (`gold-job`)

[silver_to_gold.py](src/spark_jobs/silver_to_gold.py) aggregiert jeden Monat aus `s3a://silver/yellow_tripdata/year=YYYY/month=MM/` zur stündlichen Nachfrage je Pickup-Zone und schreibt nach `s3a://gold/yellow_tripdata/zone_hourly/`, partitioniert nach `year=YYYY/month=MM`. Das ist die Tabelle, die die ML-Anwendung mit der Identität `ml_reader` liest.

**Dichtes Raster:** Jede Zone (1 bis 265) und jede Stunde des Monats bekommt eine Zeile, auch ohne Fahrten. Ausbleibende Nachfrage ist für ein Nachfragemodell ebenfalls ein Signal, und die Zeilenzahl pro Monat steht fest: 265 × Stunden, im Januar also 197.160.

| Spalte | Bedeutung |
|---|---|
| `pickup_hour` | Beginn der Stunde, lokale NYC-Zeit wie in Silver (`TIMESTAMP_NTZ`) |
| `pu_location_id` | Pickup-Zone |
| `trip_count` | Zahl der Fahrten, `0` ohne Fahrten |
| `avg_trip_distance`, `avg_duration_minutes`, `avg_fare_amount`, `avg_total_amount` | Mittelwerte der Fahrten, `null` bei `trip_count = 0` |
| `hour_of_day`, `day_of_week`, `is_weekend` | Kalendermerkmale, `day_of_week` von 1 (Montag) bis 7 (Sonntag) |
| `_run_id`, `_processed_at` | Lineage: Run-ID des Laufs und Zeitpunkt von Job 2 |

Lag-Features wie die Nachfrage der Vorwoche fehlen bewusst. Sie bräuchten Daten des Vormonats, und ein Monat ließe sich dann nicht mehr für sich neu berechnen. Die ML-Anwendung bildet sie beim Training aus der Zeitreihe. Weil die Zeit lokal ist, hat die Stunde der Sommerzeitumstellung im März keine Fahrten, die im November enthält zwei echte Stunden.

**Abgleich:** Vor dem Schreiben muss die Summe von `trip_count` genau der Zahl der Silver-Zeilen des Monats entsprechen, und die Silver-Daten des Monats müssen aus genau einem Lauf von Job 1 stammen (mehrere `_run_id` deuten auf einen abgebrochenen Commit oder parallele Läufe hin). Fiele eine Fahrt beim Join auf das Raster still weg (Zone oder Zeit außerhalb), gilt der Monat als `failed`. Weil diese Prüfungen vor dem Überschreiben laufen, bleibt der bisherige Gold-Monat dann unverändert; welcher Lauf ihn geschrieben hat, zeigt `_run_id`. Nur wenn das Schreiben selbst scheitert, wird die Gold-Partition gelöscht, weil sie dann halb ersetzt sein kann.

**Bekannte Grenze:** Gold wird direkt überschrieben, es gibt keine veröffentlichte Version oder Historie. Scheitert ein Lauf nach Job 1, liegt neues Silver neben älterem Gold. Ob Gold zum aktuellen Silver passt, zeigen der Report (`status`, `silver_run_ids`) und der Vergleich mit dem neuesten Report von Job 1.

**Report** pro Lauf nach `s3a://gold/_runs/yellow_tripdata/<run_id>.json`, je Monat mit `rows_silver`, `rows_gold`, `trips_in_gold`, `rows_without_trips`, `silver_run_ids` (aus welchen Läufen von Job 1 die Daten stammen) und Laufzeit. Über `_run_id` in Silver und `_source_file` lässt sich jede Gold-Partition so bis zur Bronze-Datei zurückverfolgen. Nach einem Lauf mit `docker compose up` tragen Gold, das zugehörige Silver, alle Reports unter `_runs/` und der Laufbericht unter `gold/_runs/pipeline/` dieselbe Run-ID.

Idempotenz und Exit-Codes wie bei Job 1; Exit `3` heißt hier, dass für einen Monat Silver-Daten fehlen. Jeder Monat wird als eine Datei geschrieben, weil Spark nach dem Join sonst 200 Dateien mit je rund 1.000 Zeilen anlegt.

Beim Lauf über 2024Q1 (9,42 Mio. Silver-Fahrten) brauchte Job 2 je Monat 6 bis 15 Sekunden, Gold umfasst 578.760 Zeilen und 8,4 MB. 55 bis 61 % der Zone-Stunden haben keine Fahrt, vor allem in Randzonen und nachts.

## Laufbericht (`run-report`)

[pipeline_report.py](src/spark_jobs/pipeline_report.py) ist der letzte Schritt von `docker compose up` und startet, wenn `gold-job` erfolgreich war. Er liest die Reports von Ingestion, Job 1 und Job 2 unter der Run-ID des Laufs und schreibt `s3a://gold/_runs/pipeline/<run_id>.json` mit Zeitraum, Monaten, Start- und Endzeit, Gesamtdauer und je Schritt Zählern und Laufzeit. Ist `RUNS_DIR` gesetzt (in Compose `/runs`, auf dem Host `runs/`), schreibt er dieselbe Datei zusätzlich dorthin; scheitert das, bleibt es bei einer Warnung. Weil er nur kleine JSON-Dateien liest, läuft er mit `--master local[1]` im eigenen Container statt auf dem Cluster und hängt nur im Speichernetz. Fehlt ein Report, endet er mit Exit-Code `1`.

## Aufruf

Beide Jobs und der Laufbericht starten über `docker compose up` in der Reihenfolge der `depends_on`-Kette (siehe [README](../../README.md#start)). Den Zeitraum legt `INGEST_PERIOD` fest; ein einzelner Monat (`INGEST_PERIOD=2024-03 docker compose up`) ersetzt nur dessen Partitionen.

## Umgebungsvariablen

| Variable | Standard | Bedeutung |
|---|---|---|
| `S3_ENDPOINT` | – | S3-Endpoint von SeaweedFS |
| `S3_SPARK_ACCESS_KEY` / `S3_SPARK_SECRET_KEY` | – | Identität `spark`: liest Bronze, schreibt Silver und Gold |
| `BRONZE_BUCKET` / `SILVER_BUCKET` / `GOLD_BUCKET` | – | Buckets, Pflicht, in Compose fest `bronze` / `silver` / `gold` |
| `TLC_DATASET` | – | Präfix der Daten, Pflicht; in Compose fest `yellow_tripdata`, weil [schema.py](src/spark_jobs/schema.py) nur das Yellow-Schema kennt |
| `INGEST_PERIOD` | – | Zeitraum des Laufs (`YYYYQn` oder `YYYY-MM`), Pflicht |
| `PIPELINE_RUN_ID_FILE` | – | Datei mit der Run-ID des Laufs, Pflicht, in Compose `/run/pipeline/run-id` |
| `SPARK_WORKER_CORES` / `SPARK_WORKER_MEMORY` | `2` / `2g` | Ressourcen pro Worker |
| `SPARK_EXECUTOR_MEMORY` | `1g` | Speicher pro Executor |
| `LOG_LEVEL` | `INFO` | Log-Level des Jobs; Spark selbst loggt auf WARN ([conf/log4j2.properties](conf/log4j2.properties)) |

## Tests

Der Code muss zu Python 3.10 passen, der Version im Spark-Image. In CI laufen die Tests über uv mit PySpark 4.1.3 und Java 17. Lokal unter Windows fehlen dafür Java und die Hadoop-Hilfsprogramme. Die Tests laufen deshalb im Spark-Image, das beides mitbringt:

```sh
podman run --rm -v "$PWD:/work:ro" -w /work/services/spark -e HOME=/tmp -e PYTHONDONTWRITEBYTECODE=1 \
  --entrypoint sh docker.io/apache/spark:4.1.3-scala2.13-java17-python3-ubuntu -c \
  'pip install -q --user pytest && PYTHONPATH=src:/work/libs/pipeline_common/src:/opt/spark/python/lib/pyspark.zip:/opt/spark/python/lib/py4j-0.10.9.9-src.zip python3 -m pytest -p no:cacheprovider'
```

Mit installiertem Java 17 (Linux, macOS, WSL) reicht `uv run --directory services/spark pytest`.
