# End-to-End-Test

Ruft den echten [Orchestrator](../../services/orchestrator/README.md) auf (`orchestrator.cli.main`), der wie im Normalbetrieb die echten Images, SeaweedFS und den Spark-Cluster über Compose startet. Der Test ergänzt nur zwei Dateien: [compose.e2e.yml](compose.e2e.yml) über `COMPOSE_FILE` fügt einen lokalen Mock-Server (`tlc-mock`) hinzu, der nur die Dateien unter [fixtures/trip-data/](fixtures/trip-data) ausliefert. [e2e.env](e2e.env) über `COMPOSE_ENV_FILES` legt sich über die `.env` im Repo-Root und setzt `TLC_BASE_URL` auf diesen Mock.

```sh
cp .env.example .env                       # einmalig im Repo-Root
uv run --directory tests/e2e pytest -v     # Docker
COMPOSE_CMD="podman compose" uv run --directory tests/e2e pytest -v   # Podman
```

Der Test läuft im eigenen Compose-Projekt `taxi-pipeline-e2e` mit eigenem Volume und eigenen Netzen. Er löscht also nie den lokalen Data Lake. Die Host-Ports sind dieselben wie in der normalen Umgebung, die deshalb vorher gestoppt sein muss. Vor und nach dem Lauf entfernt der Test sein Projekt samt Volume; mit `E2E_KEEP=1` bleibt der Stack zur Fehlersuche stehen. Der Override verkleinert den Spark-Cluster auf einen Worker mit 1 GiB und Executors mit 512 MiB, weil 2.000 Fahrten je Monat nicht mehr brauchen und lokale VMs oft nur 2 GiB RAM haben.

## Ablauf und Prüfungen

Der Test legt nur seine Eingaben fest: das Quartal mit Fixtures (`2024Q1`) und eines ohne (`2024Q2`). Alles andere liest er aus dem System selbst: Endpunkt, Buckets, Mock-URL und Zugangsdaten aus der aufgelösten Compose-Konfiguration, Identitäten und Rechte aus [seaweedfs/s3.json.template](../../seaweedfs/s3.json.template), Objekt-Keys und Reports aus den Laufberichten. Tabellen erkennt er an ihren Partitionen `year=…/month=…`. Geprüft werden Invarianten statt fest eingetragener Zahlen.

| Lauf | Prüfungen |
|---|---|
| `--quarter 2024Q1` | alle Schritte `ok`; Bronze byte-gleich mit den ausgelieferten Dateien (SHA-256, Größe, Quelle, ETag); Silver-Report summiert sich auf (`rows_in = valid + rejected + duplicates`, Duplikate wie in der Quelle, Quarantäne greift); Silver und Quarantäne passen zum Report, Pickups im Monat, Herkunft aus der Bronze-Datei; Gold ist ein dichtes Raster aus Zonen × Stunden des Monats, `sum(trip_count)` = Silver; dieselbe Run-ID in allen Reports und in `_run_id`; Zugriffsrechte jeder Identität wie im Template |
| `--quarter 2024Q1`, Wiederholung | neue Run-ID; Ingestion überspringt alle Monate (ETag unverändert); alle Schichten ohne Lineage-Spalten inhaltlich gleich, keine zusätzlichen Dateien |
| `--quarter 2024Q2` | Mock antwortet mit 403 → Ingestion `not_published`, Orchestrator endet nach der Ingestion mit Exit-Code 3, der Lake bleibt unverändert |

## Fixtures

[fixtures/trip-data/](fixtures/trip-data) sind die Mock-Daten des Tests: je 2.000 Zeilen aus den echten TLC-Dateien für Januar bis März 2024, unverändert im Originalschema und rund 70 KB pro Monat. Quelle: [NYC Taxi & Limousine Commission, TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page). Neben einer Zufallsstichprobe enthalten sie gezielt echte fehlerhafte Fahrten (Pickup außerhalb des Monats, negative oder über 24 Stunden lange Fahrzeit, unplausible Strecke, negative Beträge) und im Februar ein echtes exaktes Duplikat. Sie liegen fest im Repository; weder der Test noch CI greifen auf die echte Quelle zu.
