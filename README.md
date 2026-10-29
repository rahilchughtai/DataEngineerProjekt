# Batch-Datenarchitektur für Taxinachfrage

Portfolio im Kurs Projekt: Data Engineering an der IU Internationale Hochschule.

- Name: Rahil Chughtai
- Kurs: Projekt: Data Engineering
- Kursnummer: DLMDWWDE02
- Aufgabe: 1.1, batch-basierte Datenarchitektur für eine datenintensive Applikation
- Datenquelle: [NYC TLC Yellow Taxi Trip Records](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

## Konzept

Für das Projekt wurde Aufgabe 1.1 gewählt: Ziel ist eine Datenpipeline, die einmal pro Quartal aufbereitete Trainingsdaten zur Taxinachfrage für eine ML-Anwendung bereitstellt. Als Datenquelle dient der [NYC TLC Yellow Taxi Trip Datensatz](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page), der detaillierte Aufzeichnungen von Taxifahrten veröffentlicht.

Das System ist eine containerisierte Batch-Pipeline aus unabhängig paketierten Jobs. Die Umgebung lässt sich über Docker Compose, Git und die gesperrten Python-Abhängigkeiten jedes Service (`uv.lock`) wiederholbar aufsetzen. Die Architektur gliedert sich in folgende Kernkomponenten:

* Ingestion & Storage: Ein Python-basierter Ingestion-Service lädt die historischen Monatsdaten herunter und speichert sie in einem S3-kompatiblen SeaweedFS-Objektspeicher, der als strukturierter Data Lake (Bronze-, Silver- und Gold-Layer) dient.
* Processing: Ein verteilter Apache-Spark-Cluster bereinigt und validiert die Millionen von Datensätzen und lässt sich durch zusätzliche Worker horizontal skalieren. Die aggregierten Features werden als optimierte Parquet-Dateien im Gold-Layer abgelegt und somit der ML-Anwendung bereitgestellt.
* Orchestrierung: Docker Compose legt den Ablauf deklarativ fest. Jeder Schritt ist ein One-Shot-Container und startet erst, wenn der vorige erfolgreich beendet ist (`depends_on` mit `service_completed_successfully`), sodass `docker compose up` einen vollständigen Lauf ausführt.

Die Verlässlichkeit ergibt sich aus mehreren Bausteinen:

- idempotente Schritte: Ein erneuter Lauf überschreibt dieselben Monats-Partitionen.
- Retries beim Download und Upload der Ingestion.
- Healthchecks für SeaweedFS und den Spark-Cluster; ein fehlgeschlagener Schritt hält die Kette an, statt neu zu starten (`restart: "no"`).
- Datenprüfungen mit Abgleich zwischen Silver und Gold.

Die Wartbarkeit sichern ein Service pro Container, versionierte Images und Abhängigkeiten sowie Tests und Linting in CI. Datenschutz und Governance werden durch Datenminimierung, getrennte Zugriffsrechte und Qualitätsprüfungen mit Quarantäne umgesetzt. Datenstände sind nachvollziehbar: Jede Zeile in Silver und Gold trägt die Run-ID ihres Laufs, und jeder Schritt schreibt einen Report. Die Run-ID erzeugt `run-init` zu Beginn des Laufs, alle Schritte lesen sie aus einem gemeinsamen Volume. Der Laufbericht, die Reports unter `_runs/` und die Spalte `_run_id` lassen sich so direkt verknüpfen. Ältere Datenstände werden nicht aufbewahrt (siehe [Bekannte Grenzen](#bekannte-grenzen)).

## Architektur

![Architekturdiagramm der Batch-Pipeline](figures/architektur.png)

Die editierbare Quelle des Diagramms liegt in [figures/architektur.drawio](figures/architektur.drawio).

## Voraussetzungen

- Docker mit `docker compose` oder alternativ [Podman](https://podman.io/) mit Compose-Provider (`podman compose`). Die Compose-Datei hält sich an die Compose-Spezifikation und läuft mit beiden.
- [uv](https://docs.astral.sh/uv/) nur für die lokale Entwicklung und die Tests (Python 3.12), nicht für einen Lauf.
- Mindestens 8 GB RAM für Container: Master, zwei Worker mit je 2 GiB und der Driver des Spark-Jobs. Bei einer Podman-Machine mit Hyper-V oder unter macOS mit `podman machine set --memory 8192` erhöhen. Unter WSL teilt sich die Machine den Speicher mit Windows.

Gemessen mit 2024Q1 (9,55 Mio. Fahrten): Im Data Lake liegen danach 160 MB Bronze, 203 MB Silver (inklusive Quarantäne) und 8,4 MB Gold. Ein erneuter Lauf desselben Quartals überschreibt die Partitionen und belegt keinen zusätzlichen Platz. Ohne Download braucht Job 1 12 bis 21 Sekunden je Monat, Job 2 6 bis 15 Sekunden. Bei weniger RAM lassen sich `SPARK_WORKER_MEMORY` und `SPARK_EXECUTOR_MEMORY` in `.env` senken.

## Start

```sh
cp .env.example .env        # Beispielwerte reichen für eine lokale Demo, sonst eigene Schlüssel setzen
docker compose up --build
```

`docker compose up` führt einen vollständigen Lauf für den Zeitraum `INGEST_PERIOD` aus `.env` aus (Standard `2024Q1`). Compose arbeitet die Kette über die `depends_on`-Bedingungen ab:

| Schritt | Startet, wenn | Aufgabe |
|---|---|---|
| `seaweedfs`, `spark-master`, `spark-worker` (2×) | sofort bzw. Master gesund | Infrastruktur, läuft dauerhaft und hat Healthchecks |
| `lake-init`, `run-init` | SeaweedFS gesund bzw. sofort | Buckets anlegen, Run-ID des Laufs erzeugen |
| `ingestion` | beide Init-Schritte erfolgreich | TLC-Monatsdateien nach Bronze |
| `silver-job` | Ingestion erfolgreich, Worker gesund | Spark-Job 1, Bronze → Silver |
| `gold-job` | Silver-Job erfolgreich | Spark-Job 2, Silver → Gold |
| `run-report` | Gold-Job erfolgreich | Laufbericht über alle Schritte |

Compose baut von sich aus nur fehlende Images. Weil die Image-Tags bei Codeänderungen gleich bleiben, sorgt `--build` dafür, dass ein Lauf immer den aktuellen Code verwendet; bei einem frischen Clone genügt `docker compose up`. Endet ein Schritt mit einem Fehler, startet keiner der folgenden. Nach `run-report` laufen SeaweedFS und der Spark-Cluster weiter, damit die ML-Anwendung Gold lesen kann; die Spark-UI ist unter `http://127.0.0.1:8080` erreichbar. Ctrl+C oder `docker compose down` beendet die Umgebung, der Data Lake bleibt im Volume erhalten. Mit `--abort-on-container-failure` stoppt Compose bei einem fehlgeschlagenen Schritt die ganze Umgebung und endet mit einem Fehlercode, für Skripte eignet sich `docker compose up -d` gefolgt von `docker compose wait run-report`. Mit Podman lautet der Befehl `podman compose up --build`.

```sh
docker compose up --build                            # Lauf für INGEST_PERIOD aus .env
INGEST_PERIOD=2024Q2 docker compose up                # anderes Quartal
INGEST_PERIOD=2024-03 docker compose up               # nur einen Monat neu verarbeiten
docker compose up --scale spark-worker=3              # mit weiteren Workern
docker compose down                                   # Umgebung beenden
```

Details zu Cluster, Qualitätsregeln und Reports: [services/spark/README.md](services/spark/README.md).

### Zeitraum und Laufbericht

`INGEST_PERIOD` ist Pflicht und nimmt ein Quartal (`YYYYQn`, der quartalsweise Lauf für das Training) oder einen einzelnen Monat (`YYYY-MM`) an. Ein Monat ersetzt nur seine eigenen Partitionen in Bronze, Silver und Gold, die übrigen Monate behalten ihren Stand und ihre Run-ID. Für reproduzierbare Demos wird ein historisches Quartal wie `2024Q1` empfohlen. Ist ein Monat des Zeitraums noch nicht veröffentlicht, endet die Ingestion mit Exit-Code `3` und die Kette hält dort an.

Den Laufbericht schreibt `run-report` nach `gold/_runs/pipeline/<run_id>.json`: Zeitraum, Monate, Start- und Endzeit, die Gesamtdauer und je Schritt die Zähler und Laufzeit aus dessen eigenem Report. Die Startzeit steckt in der Run-ID, die `run-init` vor allen anderen Schritten erzeugt. Die Gesamtdauer umfasst deshalb Containerstarts und das Warten auf den Spark-Cluster, also die End-to-End-Latenz des Laufs.

Gemessen mit 2024Q1 (Bronze schon geladen, Images gebaut, Podman unter WSL): 110 bis 113 Sekunden vom Start von `run-init` bis zum Ende von `run-report`, inklusive Start von SeaweedFS und Spark-Cluster. Davon brauchte Job 1 8 bis 18 Sekunden je Monat und Job 2 5 bis 15 Sekunden, den Rest machen Containerstarts, das Warten auf die Healthchecks und der Start der Spark-Driver aus.

## Data Lake

| Bucket | Inhalt | Schreibt | Liest |
|---|---|---|---|
| `bronze` | TLC-Dateien unverändert, `yellow_tripdata/year=YYYY/month=MM/…parquet`, Laufprotokolle unter `_runs/` | ingestion | ingestion, spark |
| `silver` | bereinigte und validierte Fahrten `yellow_tripdata/year=YYYY/month=MM/`, abgelehnte Zeilen mit Grund unter `_quarantine/`, Qualitätsreports unter `_runs/` | spark | spark |
| `gold` | stündliche Nachfrage je Pickup-Zone mit Kalendermerkmalen `yellow_tripdata/zone_hourly/year=YYYY/month=MM/`, Reports unter `_runs/` | spark | spark, ml_reader |

Jeder Service bekommt in Compose nur die Zugangsdaten seiner eigenen Identität. Die Identitäten werden beim Start von SeaweedFS aus `.env` erzeugt ([seaweedfs/s3.json.template](seaweedfs/s3.json.template)). SeaweedFS 4.x verwirft die Identitäten ohne Signing-Key stillschweigend und läuft dann ohne Authentifizierung. Der Container startet deshalb nicht, wenn `SEAWEEDFS_SIGNING_KEY` fehlt oder kürzer als 32 Zeichen ist.

## Bekannte Grenzen

Die Pipeline ist für einen lokalen Einzelbetrieb ausgelegt. Bewusst nicht umgesetzt:

- **Keine Historie der Datenstände.** Bronze, Silver und Gold werden pro Monat überschrieben. Wiederholbar sind Deployment (Compose, Lockfiles, Image-Tags) und Läufe (idempotent), ein früherer Datenstand lässt sich aber nicht wiederherstellen. Erhalten bleiben nur die Reports unter `_runs/` mit Run-IDs, Zählern und Herkunft.
- **Gold kann hinter Silver zurückliegen.** Gold wird ohne eigene Veröffentlichungsstufe direkt überschrieben. Scheitert ein Lauf nach Job 1, liegt neues Silver neben älterem Gold. Ob Gold aktuell ist, zeigen der Laufbericht unter `gold/_runs/pipeline/` und `silver_run_ids` im Gold-Report: Nach einem vollständigen Lauf enthält es genau dessen Run-ID.
- **Kein atomarer Commit.** S3A ersetzt Partitionen per Kopieren und Löschen. Wird ein Job mitten im Schreiben beendet, kann ein Monat unvollständig sein, bis ein erneuter Lauf ihn ersetzt. Job 2 lehnt Silver ab, das Zeilen aus mehreren Läufen enthält.
- **Ein Lauf zur Zeit.** Ein Compose-Projekt hat genau eine Kette; ein zweites `docker compose up` während eines Laufs greift auf dieselben Container zu. Run-IDs sind eindeutig, Reports kollidieren also nicht.
- **Keine Frist je Schritt.** Compose kennt kein Zeitlimit für einen Container. Hängt ein Schritt, wartet die Kette, bis man sie mit Ctrl+C oder `docker compose down` beendet. Die Downloads der Ingestion haben eigene Timeouts und Retries.
- **Kein eingebauter Zeitplan.** Den quartalsweisen Lauf stößt das Betriebssystem an, z. B. per cron am 1. März, Juni, September und Dezember mit `INGEST_PERIOD=<Vorquartal> docker compose up -d`.
- **Leere Monate gelten als Fehler.** Hat ein Monat keine einzige gültige Fahrt, endet Job 1 mit `failed` und lässt den bisherigen Stand stehen. Eine Schwelle für den Anteil abgelehnter Zeilen gibt es nicht.
- **Beispielschlüssel.** Die Werte aus `.env.example` reichen für eine lokale Demo. Für jeden anderen Betrieb müssen sie durch eigene Zufallswerte ersetzt werden.
- **Podman nicht in CI.** Der End-to-End-Test in CI läuft mit Docker; Podman ist nur lokal getestet.

## Entwicklung

```sh
uv sync                                   # Dev-Tools (Ruff, pre-commit)
uv run pre-commit install                 # Hooks für jeden Commit
uv run ruff format . && uv run ruff check .
uv sync --directory services/ingestion    # Umgebung eines Service, z. B. für die IDE
uv run --directory services/ingestion pytest   # Unit-Tests eines Service
COMPOSE_CMD="docker compose" uv run --directory tests/e2e pytest -v   # End-to-End-Test (normale Umgebung vorher stoppen)
```

Die Spark-Tests brauchen Java 17; unter Windows laufen sie im Spark-Image (Befehl in [services/spark/README.md](services/spark/README.md#tests)).

Formatierung und Linting laufen zentral über Ruff (Konfiguration in [pyproject.toml](pyproject.toml)), dazu `.editorconfig` und LF-Zeilenenden über `.gitattributes`. Jeder Service ist ein eigenes uv-Projekt mit eigenem `uv.lock` und eigenen Tests unter `services/<name>/tests/`; Code, den mehrere Services brauchen (Zeiträume, Run-ID, Umgebung, Logging, Run-Reports), liegt in der Bibliothek [libs/pipeline_common](libs/pipeline_common/README.md) und wird beim Bau ins Image kopiert, deshalb ist der Build-Kontext der Services das Repo-Root; pytest und Test-Hilfen stehen in der `dev`-Gruppe des Service und landen nicht im Image. Der Workflow [tests.yml](.github/workflows/tests.yml) läuft bei jedem Pull Request:

- `unit-tests`: die Unit-Tests, ein Matrix-Job pro Service und für die Bibliothek.
- `lint`: `ruff check` und `ruff format --check`.
- `e2e`: der [End-to-End-Test](tests/e2e/README.md). Er startet die Pipeline mit `docker compose up` über die echten Images, SeaweedFS und den Spark-Cluster; die Ingestion lädt gekürzte echte TLC-Dateien aus dem Repository von einem Mock-Server. Geprüft werden ein Quartal durch alle Schichten, der Laufbericht, die Wiederholung, ein einzelner Monat, ein nicht veröffentlichtes Quartal und die Zugriffsrechte.

Der abschließende Job `tests-passed` ist nur grün, wenn Unit-Tests, Lint und der End-to-End-Test erfolgreich waren; er ist der Pflicht-Check für Merges nach `develop`. Weil der End-to-End-Test die Images baut, blockiert ein Ausfall von Docker Hub oder Maven Central auch den Merge.
