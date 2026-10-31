# Code-Review: Wiederholungen, Architektur, Anti-Patterns

Stand: Branch `refactor/refactor-duplications`, geprüft gegen `develop` und die gesamte Codebasis.
Drei getrennte Durchgänge: Wiederholungen, Architektur und Anti-Patterns. Doppelte Befunde der
Durchgänge sind zusammengeführt. Jeder Befund ist am Code nachgeprüft. Die Bugs in Abschnitt 1
sind zusätzlich reproduziert.

Status: **behoben** in diesem Branch, oder **offen** mit Begründung.

## 1. Bugs

| # | Befund | Ort | Status |
|---|---|---|---|
| B1 | Ein unbekanntes `LOG_LEVEL` (z. B. `VERBOSE`) bricht mit `ValueError` ab, bevor die Behandlung für Konfigurationsfehler greift. Exit-Code 1 mit Traceback statt einer Meldung. | `pipeline_common/logs.py` | **behoben**: Rückfall auf `INFO` mit Warnung |
| B2 | Dateien unter 4 Byte (z. B. eine leere 200-Antwort) scheitern in `validate_parquet` mit `OSError` (`seek(-4, 2)`) statt mit `InvalidFileError` | `ingestion/download.py` | **behoben** und getestet |
| B3 | Größenprüfung und sha256 beziehen sich auf entpackte Bytes, `Content-Length` auf die übertragenen. requests fordert standardmäßig `gzip, deflate` an. Komprimiert die Quelle oder ein Proxy, scheitert jeder Versuch. | `ingestion/download.py`, `job.py` | **behoben**: `Accept-Encoding: identity`; die eigene Größenprüfung entfällt, urllib3 2 prüft `Content-Length` selbst |
| B4 | `init-buckets.sh` prüft mit `grep -qw`; `-` gilt als Wortgrenze, ein vorhandenes `gold-archive` lässt `gold` als vorhanden gelten. Fehler beim Anlegen bleiben unbemerkt. | `seaweedfs/init-buckets.sh` | **behoben**: exakter Namensvergleich (Format aus dem SeaweedFS-Quellcode geprüft), Nachprüfung nach dem Anlegen |

## 2. Wiederholungen

| # | Befund | Status |
|---|---|---|
| W1 | Status-Strings `"failed"`, `"processed"`, `"not_published"` stehen als Literale in mehreren Modulen; ein Tippfehler ergibt Exit-Code 0 | **behoben**: `reports.FAILED`/`PROCESSED`, `ingestion.job.NOT_PUBLISHED`, `job.MISSING` |
| W2 | Der Eintrag für einen fehlgeschlagenen Monat ist in Ingestion und Spark jeweils selbst gebaut | **behoben**: `reports.failed_entry` |
| W3 | `pipeline_report.main` wiederholt den Start von `job.main` (Logging, `ValueError` → Exit 2, `spark.stop()`) | **behoben**: gemeinsames `job.start` |
| W4 | Das Bronze-Layout `{dataset}/{year=…/month=…}` ist in Ingestion und Spark je einmal gebaut; Abweichungen fallen erst im E2E-Test auf | **behoben**: `pipeline_common.layout.month_prefix` |
| W5 | Dateiname der Monatsdatei doppelt in `ingestion/job.py` und `storage.object_key` | **behoben**: `storage.file_name` |
| W6 | Zweistellige Formatierung von Jahr/Monat in `Month` und `lake.with_lineage` doppelt | **behoben**: `Month.partition_values` |
| W7 | Die Spark-Tests haben den `paths`-Fixture dreimal, `Path(x.removeprefix("file://"))` etwa achtmal und fest eingetragene Partitionen | **behoben**: `paths` in `conftest.py`, `tests/support.py` mit `local()` und Run-IDs |
| W8 | `partitionOverwriteMode` steht global in `spark-defaults.conf` und bei jedem Schreiben in `lake.write_partition` | **behoben**: globale Einstellung entfernt, das Schreiben setzt sie selbst |
| W9 | Laufzeiten werden außerhalb von `timing.elapsed` von Hand gerundet (`download_seconds`) | **behoben** |
| W10 | Compose: One-Shot-Einstellungen (`restart`, `logging`) und Image-Tags mehrfach; doppelte Overrides in `docker-compose.test.yml` | offen: geringer Nutzen; ohne lauffähigen E2E-Test hier nicht prüfbar |
| W11 | Master-URL in drei Spark-Skripten; uv- und Python-Versionen in Dockerfiles und Workflow mehrfach | offen: Pins bewusst explizit, Änderung an Images hier nicht testbar |
| W12 | Ingestion-Tests setzen die Umgebung doppelt (`test_job.py` und `test_config.py`); `"us-east-1"` als Default zweimal | offen: geringe Auswirkung |

## 3. Architektur

| # | Befund | Status |
|---|---|---|
| A1 | `guarded_month` reicht Parameter über ein untypisiertes `*args` durch, und die Jobs lesen ihre Quelle aus dem Report-Eintrag (`entry["source"]`) | **behoben**: `process: Callable[[str], Entry]` per `functools.partial`; `_process_month` bekommt `source` als Parameter und liefert nur seine Felder |
| A2 | Spaltennamen für Lineage und Partitionen als Literale in `lake` und `GOLD_COLUMNS` | **behoben**: `lake.PARTITION_COLUMNS`/`LINEAGE_COLUMNS` |
| A3 | Job 2 importiert das Qualitätsmodul nur wegen `month_bounds` | **behoben**: `month_bounds` liegt in `schema.py`, `quality` importiert es von dort |
| A4 | `pipeline_report` behandelt nur fehlende Reports; ein kaputter Report endet mit `JSONDecodeError`/`KeyError` ohne Schrittnamen | **behoben**: eine Fehlerklasse `StepReportError` für fehlende und unbrauchbare Reports, Exit 1 mit Meldung |
| A5 | Scheitert das Schreiben des Run-Reports, gehen die Ergebnisse je Monat verloren | offen: jeder Monat loggt sein Ergebnis bereits einzeln; ein zusätzlicher Dump wäre doppelt |
| A6 | Ingestion: `download` und `storage` hängen am ganzen `Config`, `storage` importiert Datentypen aus `download` | offen: lohnt erst bei weiterem Ausbau; `Protocol`/`models.py` wären der nächste Schritt |
| A7 | `lake.write_partition(..., has_rows)` überlässt die Leer-Erkennung dem Aufrufer | offen: beide Aufrufer leiten das Flag aus gezählten Zeilen ab; ein `isEmpty()` kostete eine zusätzliche Spark-Aktion |

## 4. Anti-Patterns

| # | Befund | Status |
|---|---|---|
| P1 | `"error": str(err)` verliert den Fehlertyp (leere Meldung bei `TimeoutError()`) und übernimmt komplette Java-Stacktraces in den Report | **behoben**: `failed_entry` mit `Typ: Meldung`, gekürzt auf 1000 Zeichen |
| P2 | Ingestion verliert bei einem Fehler `source_url` und `key` im Report | **behoben**: `month_entry` vor dem `try` |
| P3 | `main()` der Ingestion ist nur für Konfigurationsfehler getestet; die Exit-Codes 1/3 und der Report fehlen in den Tests | **behoben**: Test für `failed` und `not_published` |
| P4 | Run-IDs in den Spark-Tests ohne das Hex-Suffix, das `RUN_ID_PATTERN` verlangt | **behoben** |
| P5 | `entrypoint.sh` schreibt die Zugangsdaten zunächst mit Modus 644 | **behoben**: `umask 077` beim Rendern |
| P6 | Secrets als `sed`-Argument (sichtbar in `/proc/*/cmdline` im Container) | offen: nur innerhalb des Containers sichtbar, Werte auf `[A-Za-z0-9_-]` beschränkt |
| P7 | `dict[str, Any]` und `Any` für Report-Einträge und S3-Client | offen: Typprüfung (mypy) ist nicht eingerichtet; `TypedDict` brächte ohne sie wenig |
| P8 | Healthcheck des Workers prüft nicht die Registrierung am Master; `SPARK_EXECUTOR_MEMORY` > `SPARK_WORKER_MEMORY` lässt den Job ohne Fehler warten | offen: braucht einen lauffähigen Cluster zum Testen |
| P9 | `requests.ConnectionError` umfasst auch `SSLError`/`ProxyError`, dauerhafte Fehler werden wiederholt | offen: nur zusätzliche Wartezeit, kein falsches Ergebnis |

## Zweiter Durchgang: weniger Code

Vorrang hatte, Code zu entfernen statt neue Abstraktionen einzuführen (netto rund −150 Zeilen):

- `parse_month`, `quarter_months` und `count_statuses` wurden nur von Tests genutzt und gehen in
  `parse_period` bzw. `run_report` auf.
- `SourceInfo.content_length` wurde nie gelesen; die eigene `Content-Length`-Prüfung beim Download
  doppelte die von urllib3.
- `BUSINESS_COLUMNS`, `RULE_NAMES`, `valid_rows`/`rejected_rows` und die Regel-Wrapper
  `_amount_invalid`/`_location_invalid` waren Einzeiler mit nur einem Aufrufer.
- `pipeline_report` hat eine statt drei Fehlerklassen; die Prüfung eines Step-Reports ist ein
  einziger `try`-Block.
- Tests, die nur wiederholten, was andere Tests schon abdecken (`test_lake.py`,
  `test_object_key_is_deterministic`), sind entfernt.

## Prüfung

- Unit-Tests: `libs/pipeline_common` 53, `services/ingestion` 37, `services/spark` 84, alle grün.
- `pre-commit run --all-files` (Ruff, YAML, Whitespace) grün; `sh -n` für die geänderten Skripte.
- E2E (`docker-compose.test.yml`) ist in dieser Umgebung nicht gelaufen, weil der Proxy die
  Basis-Images (ghcr.io, Docker Hub) blockiert. Er läuft in CI beim PR nach `main`.
