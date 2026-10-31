# End-to-End-Test

Der Test startet dieselbe Pipeline wie im Normalbetrieb, mit den echten Images, SeaweedFS und dem Spark-Cluster. [docker-compose.test.yml](../../docker-compose.test.yml) im Repo-Root legt sich als Override über `docker-compose.yml` und ändert nur, was ein Test braucht: Ein lokaler Mock-Server (`tlc-mock`) ersetzt CloudFront und liefert nur die Dateien unter [fixtures/trip-data/](fixtures/trip-data). `INGEST_PERIOD` steht fest auf `2024Q1`, der Spark-Cluster ist kleiner, und am Ende der Kette steht der Container `e2e-tests`. Laptop und CI führen denselben Befehl aus:

```sh
docker compose -f docker-compose.yml -f docker-compose.test.yml --env-file .env.example run --build --rm e2e-tests
docker compose -f docker-compose.yml -f docker-compose.test.yml --env-file .env.example down -v
```

Mit Podman lautet der Befehl `podman compose` statt `docker compose`. Auf dem Host braucht es nur die Container-Engine, kein Python. `--env-file .env.example` macht den Lauf unabhängig von der lokalen `.env`.

`compose run` startet zuerst alle Abhängigkeiten von `e2e-tests` und wartet ihre `depends_on`-Bedingungen ab. `e2e-tests` hängt mit `service_completed_successfully` an `run-report` und startet deshalb nur, wenn Ingestion, beide Spark-Jobs und der Laufbericht erfolgreich waren. Danach führt der Container pytest gegen SeaweedFS aus, `run` zeigt dessen Ausgabe und gibt seinen Exit-Code weiter. Scheitert ein Schritt vorher, startet `e2e-tests` nicht und `run` endet ebenfalls mit einem Fehler; die Ausgabe der Pipeline zeigt dann `compose logs`. `up --exit-code-from` wie in vielen Anleitungen funktioniert hier nicht, weil es den Stack schon beim Ende des ersten One-Shots (`run-init`) abbricht.

Der Test läuft im eigenen Compose-Projekt `taxi-pipeline-test` mit eigenen Volumes und Netzen und ohne Host-Ports (`ports: !reset []`). Er berührt also nie den lokalen Data Lake und darf neben der normalen Umgebung laufen. `down -v` entfernt danach seine Volumes. Der Spark-Cluster schrumpft auf einen Worker mit 1 GiB und Executors mit 512 MiB, weil 2.000 Fahrten je Monat nicht mehr brauchen und lokale VMs oft nur 2 GiB RAM haben.

## Prüfungen

Der Test liest alles aus dem System selbst: Endpunkt, Buckets, Mock-URL und Zugangsdaten aus der Umgebung des Containers, Identitäten und Rechte aus [seaweedfs/s3.json.template](../../seaweedfs/s3.json.template) (ins Image kopiert), die Run-ID aus dem Volume von `run-init`, Objekt-Keys aus den Reports. Tabellen erkennt er an ihren Partitionen `year=…/month=…`. Geprüft werden Invarianten statt fest eingetragener Zahlen:

| Test | Prüfung |
|---|---|
| Laufbericht | Run-ID, Zeitraum, Monate, Zähler und Laufzeiten der drei Service-Reports, Gesamtdauer mindestens deren Summe |
| Bronze | byte-gleich mit den ausgelieferten Dateien (SHA-256, Größe, Quelle, ETag) |
| Silver-Report | `rows_in = valid + rejected + duplicates`, Duplikate wie in der Quelle, Quarantäne greift |
| Silver und Quarantäne | passen zum Report, Pickups im Monat, Herkunft aus der Bronze-Datei, Ablehnungsgründe wie im Report |
| Gold | dichtes Raster aus Zonen × Stunden des Monats, `sum(trip_count)` = Silver |
| Dateien | je Partition in allen Buckets genau eine Parquet-Datei, also keine Reste früherer Läufe |
| Lineage | dieselbe Run-ID in allen Reports und in `_run_id` |
| Zugriffsrechte | List, Read und Write jeder Identität auf jedem Bucket wie im Template |

Der Test prüft einen Lauf; die Idempotenz entsteht durch einen zweiten Lauf auf dem befüllten Lake. `down` ohne `-v` entfernt die Container, behält aber die Volumes und damit den Lake, und `run-init` erzeugt eine neue Run-ID:

```sh
docker compose -f docker-compose.yml -f docker-compose.test.yml --env-file .env.example down
docker compose -f docker-compose.yml -f docker-compose.test.yml --env-file .env.example run --rm e2e-tests
```

Dieselben Prüfungen müssen wieder bestehen. Hängt die Wiederholung Zeilen an, passen Zeilenzahl und Report nicht mehr zusammen; bleiben Dateien oder Run-IDs früherer Läufe stehen, scheitern der Dateien- und der Lineage-Test. Die Ingestion meldet unveränderte Dateien als `skipped`. CI führt beide Läufe nacheinander aus. Einzelmonat und nicht veröffentlichte Monate prüfen die Unit-Tests der Services: `test_rerun_*` in [services/spark/tests](../../services/spark/tests) (nur die eigene Monatspartition wird ersetzt), `test_ingest_month_reloads_only_changed_source` und `test_download_missing_file_is_not_retried` in [services/ingestion/tests](../../services/ingestion/tests).

## Fixtures

[fixtures/trip-data/](fixtures/trip-data) sind die Mock-Daten des Tests: je 2.000 Zeilen aus den echten TLC-Dateien für Januar bis März 2024, unverändert im Originalschema und rund 70 KB pro Monat. Quelle: [NYC Taxi & Limousine Commission, TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page). Neben einer Zufallsstichprobe enthalten sie gezielt echte fehlerhafte Fahrten (Pickup außerhalb des Monats, negative oder über 24 Stunden lange Fahrzeit, unplausible Strecke, negative Beträge) und im Februar ein echtes exaktes Duplikat. Sie liegen fest im Repository; weder der Test noch CI greifen auf die echte Quelle zu.
