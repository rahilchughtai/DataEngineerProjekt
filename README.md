# Batch-Datenarchitektur für Taxinachfrage

Portfolio im Kurs Projekt: Data Engineering an der IU Internationale Hochschule.

- Name: Rahil Chughtai
- Kurs: Projekt: Data Engineering
- Kursnummer: DLMDWWDE02
- Aufgabe: 1.1, batch-basierte Datenarchitektur für eine datenintensive Applikation
- Datenquelle: [NYC TLC Yellow Taxi Trip Records](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

## Konzept

Für das Projekt wurde Aufgabe 1.1 gewählt: Ziel ist eine Datenpipeline, die einmal pro Quartal aufbereitete Trainingsdaten zur Taxinachfrage für eine ML-Anwendung bereitstellt. Als Datenquelle dient der [NYC TLC Yellow Taxi Trip Datensatz](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page), der detaillierte Aufzeichnungen von Taxifahrten veröffentlicht.

Das System basiert auf einer containerisierten Microservice-Architektur, die durch Nutzung von Docker Compose und Git reproduzierbar ist. Die Architektur gliedert sich in folgende Kernkomponenten:

* Ingestion & Storage: Ein Python-basierter Ingestion-Service lädt die historischen Monatsdaten herunter und speichert sie in einem S3-kompatiblen SeaweedFS-Objektspeicher, der als strukturierter Data Lake (Bronze-, Silver- und Gold-Layer) dient.
* Processing: Ein verteilter Apache-Spark-Cluster bereinigt und validiert die Millionen von Datensätzen und lässt sich durch zusätzliche Worker horizontal skalieren. Die aggregierten Features werden als optimierte Parquet-Dateien im Gold-Layer abgelegt und somit der ML-Anwendung bereitgestellt.
* Orchestrierung: Der Ablauf wird über ein modulares Python-Skript gesteuert, das die Services nacheinander startet.

Die Verlässlichkeit ergibt sich aus idempotenten Schritten, automatischen Retries, Healthchecks und Datenprüfungen. Die Wartbarkeit wird durch einen Service pro Container, gepinnte Versionen und Tests gewährleistet. Datenschutz und Governance werden durch Datenminimierung, getrennte Zugriffsrechte, Qualitätsprüfungen und nachvollziehbare, versionierte Datenstände umgesetzt.

## Architektur

![Architekturdiagramm der Batch-Pipeline](figures/architektur.png)

Die editierbare Quelle des Diagramms liegt in [figures/architektur.drawio](figures/architektur.drawio).

## Voraussetzungen

- [Podman](https://podman.io/) mit Compose-Provider (`podman compose`) oder alternativ Docker mit `docker compose`. Die Compose-Datei hält sich an die Compose-Spezifikation und läuft mit beiden.
- [uv](https://docs.astral.sh/uv/) für die lokale Entwicklung (Python 3.12).
- Für den späteren Spark-Cluster braucht die Podman-Machine mehr als die standardmäßigen 2 GiB RAM (`podman machine set --memory 8192`).

## Start

```sh
cp .env.example .env        # danach alle change-me-Werte durch eigene Schlüssel ersetzen
podman compose up --build   # bzw. docker compose up --build
```

Compose startet SeaweedFS, legt über `lake-init` die Buckets `bronze`, `silver` und `gold` an und führt danach einmal den Ingestion-Service für das in `INGEST_QUARTER` (oder `INGEST_MONTH`) angegebene Quartal bzw. den Monat aus. Einzelne Läufe lassen sich auch gezielt starten:

```sh
podman compose run --rm ingestion --quarter 2024Q1
podman compose run --rm ingestion --month 2024-01 --force
```

## Data Lake

| Bucket | Inhalt | Schreibt | Liest |
|---|---|---|---|
| `bronze` | TLC-Dateien unverändert, `yellow_tripdata/year=YYYY/month=MM/…parquet`, Laufprotokolle unter `_runs/` | ingestion | ingestion, spark |
| `silver` | bereinigte und validierte Fahrten | spark | spark |
| `gold` | aggregierte Features | spark | spark, ml_reader |

Jeder Service bekommt in Compose nur die Zugangsdaten seiner eigenen Identität. Die Identitäten werden beim Start von SeaweedFS aus `.env` erzeugt ([seaweedfs/s3.json.template](seaweedfs/s3.json.template)). SeaweedFS 4.x verwirft die Identitäten ohne Signing-Key stillschweigend und läuft dann ohne Authentifizierung. Der Container startet deshalb nicht, wenn `SEAWEEDFS_SIGNING_KEY` fehlt oder kürzer als 32 Zeichen ist.

## Entwicklung

```sh
uv sync                                   # Dev-Tools (Ruff, pre-commit)
uv run pre-commit install                 # Hooks für jeden Commit
uv run ruff format . && uv run ruff check .
uv sync --directory services/ingestion    # Umgebung eines Service, z. B. für die IDE
uv run --directory services/ingestion pytest   # Unit-Tests eines Service
```

Formatierung und Linting laufen zentral über Ruff (Konfiguration in [pyproject.toml](pyproject.toml)), dazu `.editorconfig` und LF-Zeilenenden über `.gitattributes`. Jeder Service ist ein eigenes uv-Projekt mit eigenem `uv.lock` und eigenen Tests unter `services/<name>/tests/`; pytest und Test-Hilfen stehen in der `dev`-Gruppe des Service und landen nicht im Image. Der Workflow [tests.yml](.github/workflows/tests.yml) führt die Unit-Tests bei jedem Pull Request aus, mit einem Matrix-Job pro Service. Der abschließende Job `tests-passed` ist nur grün, wenn alle Matrix-Jobs erfolgreich waren; er ist der Pflicht-Check für Merges nach `develop`.
