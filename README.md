# Batch-Datenarchitektur für Taxinachfrage

Portfolio im Kurs Projekt: Data Engineering an der IU Internationale Hochschule.

- Name: Rahil Chughtai
- Kurs: Projekt: Data Engineering
- Kursnummer: DLMDWWDE02
- Aufgabe: 1.1, batch-basierte Datenarchitektur für eine datenintensive Applikation
- Datenquelle: [NYC TLC Yellow Taxi Trip Records](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

## Konzept

Die Pipeline stellt einmal pro Quartal aufbereitete Trainingsdaten zur Taxinachfrage für eine
ML-Anwendung bereit. Sie folgt der Medallion-Architektur: Die Rohdaten der TLC landen unverändert
in **Bronze**, werden in **Silver** bereinigt und validiert und in **Gold** zur stündlichen
Nachfrage je Pickup-Zone aggregiert. Jeder Schritt ist ein eigener Container; `docker compose up`
führt einen vollständigen Lauf aus.

![Architekturdiagramm der Batch-Pipeline](figures/architektur.png)

Die editierbare Quelle des Diagramms liegt in [figures/architektur.drawio](figures/architektur.drawio).

## Datenfluss

| Schritt (Compose-Service) | Code | Liest | Schreibt |
|---|---|---|---|
| `lake-init` | [seaweedfs/init-buckets.py](seaweedfs/init-buckets.py) | – | Buckets `bronze`, `silver`, `gold` |
| `ingestion` | [services/ingestion/ingest.py](services/ingestion/ingest.py) | TLC-Monatsdateien | `bronze/yellow_tripdata/year=YYYY/month=MM/` |
| `silver-job` | [services/spark/jobs/bronze_to_silver.py](services/spark/jobs/bronze_to_silver.py) | Bronze | `silver/yellow_tripdata/…`, abgelehnte Zeilen in `silver/_quarantine/yellow_tripdata/…` |
| `gold-job` | [services/spark/jobs/silver_to_gold.py](services/spark/jobs/silver_to_gold.py) | Silver | `gold/yellow_tripdata/zone_hourly/…` |
| `report` | [services/spark/jobs/report.py](services/spark/jobs/report.py) | alle Schichten | `gold/_reports/<INGEST_PERIOD>/` |

Jeder Schritt startet erst, wenn der vorige erfolgreich beendet ist (`depends_on` mit
`service_completed_successfully`); ein Fehler hält die Kette an. Dauerhaft laufen der Data Lake
`seaweedfs` (S3-kompatibler Objektspeicher) und der Spark-Cluster aus `spark-master` und zwei
`spark-worker`. Die Spark-Jobs reichen ihren Job per `spark-submit` beim Cluster ein.

Alle drei Schichten sind nach `year=YYYY/month=MM` partitioniert. Den Zeitraum gibt
`INGEST_PERIOD` vor: ein Quartal (`2024Q1`) für den regulären Lauf oder ein Monat (`2024-03`), um
nur diesen neu zu verarbeiten. Das Parsen des Zeitraums und das Format der Partitionen teilen sich
Ingestion und Spark über die kleine Bibliothek [libs/pipeline_common](libs/pipeline_common/src/pipeline_common), zusammen mit dem gemeinsamen Log-Format.

**Silver** übernimmt nur die für Nachfrage-Features nötigen Spalten, castet sie auf feste Typen
(die TLC ändert ihr Schema über die Jahre), entfernt exakte Duplikate und prüft zehn
Qualitätsregeln: fehlende Zeitstempel, Abholung außerhalb des Monats, negative oder über 24 Stunden
lange Fahrten, ungültige Distanzen und Beträge, Abholungen in den Zonen 264/265 („unbekannt“ und
„außerhalb NYC“, für die sich keine Nachfrage je Zone bilden lässt), ungültige Zielzonen und
Fahrgastzahlen. Abgelehnte Zeilen landen mit dem Namen der ersten verletzten Regel
(`reject_reason`) in der Quarantäne. `_source_file` und `_processed_at` halten fest, woher und
wann jede Zeile stammt.

**Gold** enthält eine Zeile je Pickup-Zone (1 bis 263) und Stunde des Monats: Anzahl
(`trip_count`), mittlere Distanz, Dauer und Beträge sowie Stunde, Wochentag und Wochenende als
Kalendermerkmale. Stunden ohne Fahrten stehen mit `trip_count = 0` und leeren Mittelwerten drin,
damit das Modell auch fehlende Nachfrage sieht.

**Der Laufbericht** zählt nach Gold je Monat die Zeilen in allen Schichten (`bronze_rows`,
`silver_rows`, `quarantined_rows` mit `rejected_by_reason`, `duplicates`, `gold_rows`,
`gold_trips`). Er entsteht aus den Daten selbst, deshalb muss immer
`bronze_rows = silver_rows + quarantined_rows + duplicates` und `gold_trips = silver_rows` gelten.
Dazu hält er je Monat fest, wann die Stufen fertig waren (`bronze_loaded_at`,
`silver_processed_at`, `gold_processed_at`); die Abstände zeigen die Latenz jeder Stufe, vom
Ablegen der Quelldatei bis zu fertigen Features.

## Start

Voraussetzungen: Docker mit `docker compose` oder [Podman](https://podman.io/) mit
`podman compose` und mindestens 8 GB RAM für die Container (Master, zwei Worker mit je 2 GiB und
der Driver). Bei weniger RAM lassen sich `SPARK_WORKER_MEMORY` und `SPARK_EXECUTOR_MEMORY` in
`.env` senken.

```sh
cp .env.example .env                      # Beispielwerte reichen für eine lokale Demo
docker compose up --build                 # Lauf für INGEST_PERIOD aus .env (Standard 2024Q1)
INGEST_PERIOD=2024-03 docker compose up   # nur einen Monat neu verarbeiten
docker compose up --scale spark-worker=3  # mit weiteren Workern
docker compose down                       # beenden; der Data Lake bleibt im Volume erhalten
```

`--build` sorgt dafür, dass ein Lauf den aktuellen Code verwendet, weil die Image-Tags gleich
bleiben. Nach dem Lauf laufen SeaweedFS (`http://127.0.0.1:8333`) und der Spark-Cluster (UI unter
`http://127.0.0.1:8080`) weiter, damit die ML-Anwendung Gold lesen kann.

Zum Ansehen des Data Lakes gibt es die Admin-Oberfläche von SeaweedFS (Cluster, Volumes,
Dateibrowser über alle Buckets). Sie startet nur auf Anfrage und verlangt die Admin-Identität aus
`.env` (`S3_ADMIN_ACCESS_KEY` als Benutzer, `S3_ADMIN_SECRET_KEY` als Passwort):

```sh
docker compose --profile admin up -d seaweedfs-admin   # danach http://127.0.0.1:12535
docker compose --profile admin stop seaweedfs-admin
```

### Gold lesen (ML-Anwendung)

[ml_reader/gold_analysis.ipynb](ml_reader/gold_analysis.ipynb) zeigt, wie die ML-Anwendung die
Features mit der Identität `ml_reader` per pandas und pyarrow aus `gold/yellow_tripdata/zone_hourly/` liest,
die Aktualität im Laufbericht prüft und eine einfache Analyse macht. Der letzte Schritt belegt,
dass `ml_reader` Silver nicht lesen darf.

```sh
uv sync --directory ml_reader
uv run --directory ml_reader --env-file ../.env jupyter nbconvert --execute --to notebook --inplace gold_analysis.ipynb
```

## Umsetzung der Anforderungen

**Verlässlichkeit**

- Idempotent: Jeder Spark-Job überschreibt genau die Monats-Partition, die er verarbeitet; ein
  erneuter Lauf ersetzt sie, statt Zeilen zu verdoppeln. Die Ingestion überspringt Monate, die
  schon in Bronze liegen, und legt ein Objekt erst nach vollständigem Upload an.
- Wiederholungen bei Verbindungsfehlern und HTTP 429/5xx beim Download.
- Healthchecks für SeaweedFS und den Spark-Cluster; ein fehlgeschlagener Schritt beendet sich mit
  einem Fehler und hält die Kette an, statt neu zu starten (`restart: "no"`).
- Der Laufbericht macht die Vollständigkeit jedes Laufs prüfbar.

**Skalierbarkeit**

- Spark-Standalone-Cluster, horizontal skalierbar über weitere Worker
  (`docker compose up --scale spark-worker=N`).
- Monatsweise Partitionierung: Jeder Monat wird unabhängig verarbeitet und gelesen.

**Wartbarkeit**

- Ein Container je Aufgabe, ein kurzes Skript je Schicht.
- Versionierte Images, gesperrte Python-Abhängigkeiten je Service (`uv.lock`), Infrastruktur als
  Code in [docker-compose.yml](docker-compose.yml).
- Unit-Tests, End-to-End-Test und Linting in CI.

**Datensicherheit, Governance und Datenschutz**

- Least Privilege: Jeder Service bekommt nur die Zugangsdaten seiner eigenen Identität
  ([seaweedfs/s3.template.json](seaweedfs/s3.template.json)): Die Ingestion schreibt nur
  `bronze`, Spark liest `bronze` und schreibt `silver` und `gold`, die ML-Anwendung (`ml_reader`)
  liest nur `gold`. Die Spark-Worker selbst haben keine Zugangsdaten. Der End-to-End-Test prüft
  diese Rechte.
- Zugangsdaten stehen nur in `.env` (nicht im Repository). SeaweedFS startet nicht ohne
  Signing-Key, statt ohne Authentifizierung zu laufen; die gerenderte Schlüsseldatei ist nur für
  den Besitzer lesbar. Alle Container laufen ohne Root-Rechte.
- Datenminimierung in Silver, Quarantäne mit Ablehnungsgrund statt stillem Verwerfen, Herkunft
  jeder Zeile über `_source_file` und `_processed_at`.

## Bekannte Grenzen

- **Keine Historie der Datenstände.** Ein Lauf überschreibt die Monate in Silver und Gold; ein
  früherer Stand lässt sich nicht wiederherstellen.
- **Kein atomarer Commit.** Wird ein Job mitten im Schreiben beendet, kann ein Monat unvollständig
  sein, bis ein erneuter Lauf ihn ersetzt.
- **Geänderte Quelldateien werden nicht erneut geladen.** Die Ingestion überspringt jeden Monat,
  der schon in Bronze liegt. Um einen Monat neu zu laden, das Objekt in `bronze` löschen.
- **Kein eingebauter Zeitplan.** Den quartalsweisen Lauf stößt das Betriebssystem an, z. B. per
  cron mit `INGEST_PERIOD=<Vorquartal> docker compose up -d`.
- **Beispielschlüssel.** Die Werte aus `.env.example` reichen für eine lokale Demo; für jeden
  anderen Betrieb müssen sie durch eigene Zufallswerte ersetzt werden.

## Entwicklung

```sh
uv sync && uv run pre-commit install           # Ruff und pre-commit-Hooks
uv run --directory services/ingestion pytest   # Unit-Tests der Ingestion
uv run --directory services/spark pytest       # Unit-Tests der Spark-Jobs (braucht Java 17)
uv run --directory libs/pipeline_common pytest # Unit-Tests der Bibliothek
docker compose -f docker-compose.yml -f docker-compose.test.yml --env-file .env.example run --build --rm e2e-tests
docker compose -f docker-compose.yml -f docker-compose.test.yml --env-file .env.example down -v
```

Der End-to-End-Test ([tests/e2e](tests/e2e/test_pipeline.py)) startet dieselbe Pipeline mit einer
Mock-Quelle, die gekürzte echte TLC-Dateien (2.000 Fahrten je Monat, 2024Q1) ausliefert. Danach
prüft er: Bronze ist byte-gleich zur Quelle, der Laufbericht geht auf, jede Silver-Fahrt liegt in
ihrem Monat, jede Partition enthält genau eine Datei, und die Zugriffsrechte jeder Identität
entsprechen der Policy.

Der Workflow [tests.yml](.github/workflows/tests.yml) führt bei jedem Pull Request die Unit-Tests
und das Linting aus, den End-to-End-Test nach jedem Merge nach `develop` und `main` sowie beim
Release-PR nach `main`. Dort läuft er zweimal auf demselben Lake: Dieselben Prüfungen zeigen dann,
dass eine Wiederholung nichts verdoppelt.

## Troubleshooting

### Podman unter Windows: veröffentlichte Ports sind vom Host nicht erreichbar

Die Pipeline läuft durch, aber `http://127.0.0.1:8333` (SeaweedFS) und `http://127.0.0.1:8080` (Spark-UI) verweigern unter Windows die Verbindung. Den End-to-End-Test betrifft das nicht, er erreicht SeaweedFS im Compose-Netz. In der Machine selbst antwortet der Port (`podman machine ssh curl -s -o /dev/null -w "%{http_code}" http://127.0.0.1:8333/healthz` liefert `200`).

Ursache ist ein bekannter Fehler im Zusammenspiel von Podman 6 und der WSL-Weiterleitung im NAT-Modus ([microsoft/WSL#41204](https://github.com/microsoft/WSL/issues/41204)): Podman 6 reserviert veröffentlichte Ports nur per `bind()`, WSL leitet aber nur TCP-Sockets im Listen-Zustand nach Windows weiter. Neuere Images von `podman machine` bringen dafür eine Konfiguration mit ([podman-machine-os](https://github.com/podman-container-tools/podman-machine-os/blob/main/podman-image/999-podman-machine-wsl-rootful.conf)), ältere Machines nicht. `podman machine set --user-mode-networking=true` hilft nicht.

Abhilfe für eine rootful Machine (`podman machine inspect --format "{{.Rootful}}"` zeigt `true`): dieselbe Konfiguration in der Machine anlegen. Sie liegt im Dateisystem der Machine und übersteht Neustarts; nach einem `podman machine rm` und `init` ist sie neu anzulegen, sofern das neue Image sie nicht schon enthält.

```sh
podman machine ssh
mkdir -p /etc/containers/containers.rootful.conf.d
cat > /etc/containers/containers.rootful.conf.d/999-wsl-port-forwarding.conf <<'CONF'
[engine]
force_port_listen = true

[network]
default_host_ips = ["0.0.0.0"]
CONF
exit
podman machine stop
podman machine start
```

Nicht `/etc/containers/containers.conf.d/` verwenden: Das Verzeichnis ist mit `%APPDATA%\containers\containers.conf.d` unter Windows verbunden, dort liest auch der Podman-Client die Dateien. Für eine rootless Machine gilt die [rootless-Variante](https://github.com/podman-container-tools/podman-machine-os/blob/main/podman-image/999-podman-machine-wsl-rootless.conf) unter `/etc/containers/containers.rootless.conf.d/`.

### Podman unter Windows: `failed to connect to the docker API at npipe`

`podman compose` bricht sofort mit `failed to connect to the docker API at npipe:////./pipe/podman-machine-default ... The system cannot find the file specified` ab, während `podman run` und `podman ps` funktionieren. Das tritt typischerweise auf, nachdem Windows neu gestartet oder heruntergefahren wurde, während die Machine lief. `podman machine start` meldet dabei `win-sshproxy.exe failed to start, exit code: 0` und trotzdem Erfolg.

Ursache ist ein Fehler in Podman 6.1.4 und 5.8.9 ([podman#29971](https://github.com/podman-container-tools/podman/issues/29971)): Der mitgelieferte gvproxy v0.9.0 löscht eine liegengebliebene Socket-Datei `%TEMP%\podman\podman-machine-default-api.sock` vor dem Start nicht mehr. `win-sshproxy` kann sie nicht binden (Windows-Ereignisprotokoll, Anwendung, Quelle `.NET Runtime`: `bind: Only one usage of each socket address`), beendet sich und nimmt die Named Pipe mit, die `podman compose` an `docker-compose.exe` weiterreicht. Ein sauberes `podman machine stop` räumt die Datei auf, ein Windows-Neustart nicht.

Behoben ist das ab Podman 6.2.0 ([#30007](https://github.com/podman-container-tools/podman/pull/30007)), das die Datei beim Start selbst entfernt. Bis zum Update hilft, die Datei bei gestoppter Machine zu löschen:

```powershell
podman machine stop
Remove-Item $env:TEMP\podman\podman-machine-default-api.sock
podman machine start
Test-Path \\.\pipe\podman-machine-default   # True
```

Lässt sich die Datei nicht löschen (`The file cannot be accessed by the system`), stattdessen den Ordner `%TEMP%\podman` umbenennen und die Machine starten.
