# Orchestrator

Der zentrale Einstieg für einen vollständigen Pipeline-Lauf: Ingestion, Spark-Job 1 (Bronze → Silver) und Spark-Job 2 (Silver → Gold), nacheinander für ein Quartal und jeweils als Compose-Service. Das Skript läuft auf dem Host und nicht im Container. Ein Container bräuchte dafür den Socket der Container-Engine, also Rechte, die root auf dem Host gleichkommen, und dessen Pfad unterscheidet sich je nach Engine und Betriebssystem. Es nutzt nur die Standardbibliothek.

## Voraussetzung

Der Orchestrator macht keine Annahmen über die Container-Engine. Er ruft nur den angegebenen Compose-Befehl auf, und dieser muss lauffähig sein. Für eine funktionierende Engine mit Compose-Unterstützung ist der Nutzer zuständig. Der Befehl kommt aus `--compose`, sonst aus der Umgebungsvariable `COMPOSE_CMD`. Es gibt keinen Standardwert und keine automatische Erkennung. Fehlt beides, bricht der Orchestrator vor dem ersten Schritt mit Exit-Code `2` ab.

## Aufruf

Im Repo-Root, damit Compose `docker-compose.yml` und `.env` findet. `<compose>` steht für den Compose-Befehl der eigenen Engine:

```sh
export COMPOSE_CMD="<compose>"                                # einmalig, z. B. in der Shell-Konfiguration
uv run --project services/orchestrator orchestrator --quarter 2024Q1
uv run --project services/orchestrator orchestrator          # neuestes veröffentlichtes Quartal
uv run --project services/orchestrator orchestrator --compose "<compose>"   # statt COMPOSE_CMD
```

Für reproduzierbare Demos ein historisches Quartal explizit angeben. Ohne `--quarter` gilt das letzte Quartal, dessen drei Monate nach der Veröffentlichungsverzögerung der TLC (etwa zwei Monate nach Monatsende) vorliegen sollten, am 29.10.2026 also 2026Q2; 2026Q3 erst ab dem 1.12.2026, wenn auch der September vorliegen sollte. Der Orchestrator prüft das Format vor dem ersten Schritt und bricht bei einem ungültigen Wert sofort mit Exit-Code `2` ab, ohne Build und ohne Bericht. Erlaubt sind dieselben Schreibweisen wie bei den Services (`2024Q1`, `2024q1`, `2024-Q1`), weitergegeben und protokolliert wird stets `YYYYQn`. `INGEST_PERIOD` aus `.env` beeinflusst den Orchestrator nicht; es ist nur ein Fallback für manuelle Einzelläufe.

## Ablauf

| Schritt | Befehl |
|---|---|
| `build` | `compose --profile jobs build`, baut alle Images |
| `ingestion` | `compose run --rm -e PIPELINE_RUN_ID=<run_id> ingestion --quarter Q`, startet SeaweedFS und `lake-init` mit |
| `silver-job` | `compose run --rm -e PIPELINE_RUN_ID=<run_id> silver-job --quarter Q`, startet und wartet zusätzlich auf den Spark-Cluster |
| `gold-job` | `compose run --rm -e PIPELINE_RUN_ID=<run_id> gold-job --quarter Q` |

Über `PIPELINE_RUN_ID` übernehmen alle drei Jobs die Run-ID des Orchestrators. Ein Lauf hat damit genau eine Run-ID: in `runs/<run_id>.json`, in den Reports unter `_runs/` aller drei Buckets und in der Spalte `_run_id` von Silver und Gold. Ein ungültiger Wert gilt im Job als Konfigurationsfehler (Exit-Code `2`). Ohne die Variable, etwa bei einem manuellen `compose run`, erzeugt jeder Job eine eigene Run-ID.

Die Datenjobs liegen im Compose-Profil `jobs` und hängen nur von der Infrastruktur ab, nicht voneinander; die Reihenfolge legt allein der Orchestrator fest. Die Startzeit des Spark-Clusters zählt daher zum Schritt `silver-job`. Alle Schritte sind idempotent, ein erneuter Lauf desselben Quartals überschreibt dieselben Partitionen. Der Orchestrator wiederholt keine Schritte und bricht beim ersten Fehler ab. SeaweedFS und der Spark-Cluster laufen danach weiter (`compose down` beendet sie).

**Frist je Schritt:** Jeder Schritt darf höchstens `--step-timeout` Sekunden dauern (Standard `1200`, also 20 Minuten; `0` schaltet die Frist ab). Danach beendet der Orchestrator den Compose-Client (SIGTERM, nach 30 Sekunden SIGKILL), und der Schritt erhält den Status `timeout`. Die Container verwaltet allein Compose. Bleibt ein Job-Container zurück, etwa nach SIGKILL, entfernt ihn `compose down --remove-orphans`. Ein langer Erstbuild oder ein langsamer Download kann die Frist überschreiten; dann `--step-timeout` erhöhen.

**Ein Lauf zur Zeit:** Parallele Läufe für überlappende Zeiträume werden nicht verhindert und würden dieselben Partitionen überschreiben. Die Run-ID (`YYYYMMDDTHHMMSSZ-xxxxxx`) ist eindeutig, Berichte kollidieren also nicht.

## Exit-Code und Bericht

Der Exit-Code ist der des fehlgeschlagenen Schritts, sonst `0`: `1` Fehler in einem Monat, `2` Konfiguration bzw. ungültiges Quartal (beim Orchestrator selbst schon vor dem ersten Schritt), `3` Quelldaten unvollständig (bei der Ingestion: Monat noch nicht veröffentlicht), `124` Frist überschritten, `127` Compose-Befehl nicht gefunden. Jeder Lauf schreibt `runs/<run_id>.json` mit Quartal, Compose-Befehl, Frist (`step_timeout_seconds`), Status und der Wandzeit jedes Schritts inklusive Containerstart, also der End-to-End-Latenz. Die Berichte der Services selbst liegen unter `_runs/` im jeweiligen Bucket, unter derselben Run-ID wie der Bericht des Orchestrators.

## Quartalsweise Planung

Kein eigener Scheduler: Der Lauf wird vom Betriebssystem angestoßen, z. B. per cron am 1. März, Juni, September und Dezember, wenn das Vorquartal veröffentlicht sein sollte:

```cron
0 6 1 3,6,9,12 *  cd /pfad/zum/repo && COMPOSE_CMD="<compose>" uv run --project services/orchestrator orchestrator
```

Unter Windows leistet die Aufgabenplanung dasselbe.

## Tests

Im Repo-Root: `uv run --directory services/orchestrator pytest`. Neben Ablauf, Fehlerabbruch und Berichten prüfen die Tests die Compose-Konfiguration: Ohne Profil sind nur Infrastruktur-Services aktiv, Datenjobs hängen nur von Infrastruktur ab. Diese Prüfungen nutzen den Compose-Befehl aus `COMPOSE_CMD`, brauchen aber keinen laufenden Container-Dienst. Fehlt die Variable oder das Programm, werden sie lokal übersprungen. In CI (`CI=true`) schlagen sie dann fehl, damit die Prüfung nicht unbemerkt entfällt.
