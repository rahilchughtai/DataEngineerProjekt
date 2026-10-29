# End-to-End-Test

Startet die Pipeline wie im Normalbetrieb mit `docker compose up` über die echten Images, SeaweedFS und den Spark-Cluster. Der Test ergänzt nur zwei Dateien: [compose.e2e.yml](compose.e2e.yml) über `COMPOSE_FILE` fügt einen lokalen Mock-Server (`tlc-mock`) hinzu, der nur die Dateien unter [fixtures/trip-data/](fixtures/trip-data) ausliefert. [e2e.env](e2e.env) über `COMPOSE_ENV_FILES` legt sich über die `.env` im Repo-Root und setzt `TLC_BASE_URL` auf diesen Mock. Den Zeitraum je Lauf setzt der Test über `INGEST_PERIOD` in der Umgebung des Compose-Aufrufs.

```sh
cp .env.example .env                       # einmalig im Repo-Root
COMPOSE_CMD="<compose>" uv run --directory tests/e2e pytest -v   # z. B. "docker compose" oder "podman compose"
```

Jeder Lauf ist `compose up --detach`, gefolgt von `compose wait run-report`. Abgekoppelt deshalb, weil SeaweedFS und der Spark-Cluster nach dem Lauf weiterlaufen und ein angehängtes `up` nicht von selbst endet. `up --detach` wartet die `depends_on`-Bedingungen ab und scheitert, sobald ein Schritt fehlschlägt. Danach liest der Test Zustand und Exit-Code jedes Containers aus `compose ps --all` und die Run-ID aus dem neuen Ingestion-Report. Für `up` und `wait` gilt je eine Frist von 600 Sekunden.

Der Test läuft im eigenen Compose-Projekt `taxi-pipeline-e2e` mit eigenen Volumes und Netzen. Er löscht also nie den lokalen Data Lake. Die Host-Ports sind dieselben wie in der normalen Umgebung, die deshalb vorher gestoppt sein muss. Vor dem ersten Lauf entfernt der Test sein Projekt samt Volumes und baut die Images neu, danach räumt er wieder auf; mit `E2E_KEEP=1` bleibt der Stack zur Fehlersuche stehen. Der Override verkleinert den Spark-Cluster auf einen Worker mit 1 GiB und Executors mit 512 MiB, weil 2.000 Fahrten je Monat nicht mehr brauchen und lokale VMs oft nur 2 GiB RAM haben.

## Ablauf und Prüfungen

Der Test legt nur seine Eingaben fest: das Quartal mit Fixtures (`2024Q1`), einen Monat daraus (`2024-03`) und ein Quartal ohne Fixtures (`2024Q2`). Alles andere liest er aus dem System selbst: Endpunkt, Buckets, Mock-URL und Zugangsdaten aus der aufgelösten Compose-Konfiguration, Identitäten und Rechte aus [seaweedfs/s3.json.template](../../seaweedfs/s3.json.template), Objekt-Keys aus den Reports. Tabellen erkennt er an ihren Partitionen `year=…/month=…`. Geprüft werden Invarianten statt fest eingetragener Zahlen.

| Lauf | Prüfungen |
|---|---|
| Konfiguration | keine Profile; `ingestion` wartet auf `lake-init` und `run-init`, jeder weitere Schritt auf den vorigen mit `service_completed_successfully`; alle One-Shots mit `restart: "no"` |
| `INGEST_PERIOD=2024Q1` | alle One-Shots `exited` mit `0`; Laufbericht mit Run-ID, Zeitraum und den Zählern und Laufzeiten der drei Service-Reports, Gesamtdauer mindestens deren Summe; Bronze byte-gleich mit den ausgelieferten Dateien (SHA-256, Größe, Quelle, ETag); Silver-Report summiert sich auf (`rows_in = valid + rejected + duplicates`, Duplikate wie in der Quelle, Quarantäne greift); Silver und Quarantäne passen zum Report, Pickups im Monat, Herkunft aus der Bronze-Datei; Gold ist ein dichtes Raster aus Zonen × Stunden des Monats, `sum(trip_count)` = Silver; dieselbe Run-ID in allen Reports und in `_run_id`; Zugriffsrechte jeder Identität wie im Template |
| `INGEST_PERIOD=2024Q1`, Wiederholung | neue Run-ID; Ingestion überspringt alle Monate (ETag unverändert); alle Schichten ohne Lineage-Spalten inhaltlich gleich, keine zusätzlichen Dateien |
| `INGEST_PERIOD=2024-03` | alle One-Shots erfolgreich; alle Reports und der Laufbericht enthalten nur `2024-03`; nur die März-Partitionen in Silver und Gold tragen die neue Run-ID, Januar und Februar die der Wiederholung; Inhalt und Dateien sonst unverändert |
| `INGEST_PERIOD=2024Q2` | Mock antwortet mit 403 → Ingestion `not_published` mit Exit-Code 3, `up` scheitert, `silver-job`, `gold-job` und `run-report` starten nicht, der Lake bleibt unverändert |

## Fixtures

[fixtures/trip-data/](fixtures/trip-data) sind die Mock-Daten des Tests: je 2.000 Zeilen aus den echten TLC-Dateien für Januar bis März 2024, unverändert im Originalschema und rund 70 KB pro Monat. Quelle: [NYC Taxi & Limousine Commission, TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page). Neben einer Zufallsstichprobe enthalten sie gezielt echte fehlerhafte Fahrten (Pickup außerhalb des Monats, negative oder über 24 Stunden lange Fahrzeit, unplausible Strecke, negative Beträge) und im Februar ein echtes exaktes Duplikat. Sie liegen fest im Repository; weder der Test noch CI greifen auf die echte Quelle zu.
