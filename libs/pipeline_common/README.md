# pipeline_common

Gemeinsamer Code der Pipeline-Services, ohne Abhängigkeiten und lauffähig ab Python 3.10, weil der Spark-Container diese Version hat.

| Modul | Inhalt |
| --- | --- |
| `periods` | `Month` (mit `next()`, `partition` und `partition_values`), `parse_period` und `months_from_env` für `INGEST_PERIOD` (Quartal `2024Q1` oder Monat `2024-01`) |
| `run_id` | `read_run_id`: liest und prüft die Run-ID, die `run-init` ins Volume schreibt; `started_at`: Startzeit aus der Run-ID |
| `env` | `required`, `optional` und `ConfigError` für Umgebungsvariablen |
| `logs` | `setup_logging` mit dem gemeinsamen Format; ein unbekanntes `LOG_LEVEL` fällt mit Warnung auf `INFO` zurück |
| `reports` | `run_report` (Aufbau des JSON-Reports eines Schritts), `failed_entry` (Eintrag eines fehlgeschlagenen Monats), die Status `FAILED`/`PROCESSED`, `exit_code` und das Enum `ExitCode` (0 `OK`, 1 `FAILED` bei Fehlern, 2 `CONFIG` bei Konfigurationsfehlern, 3 `INCOMPLETE` bei fehlenden Quelldaten) |
| `layout` | Ablage im Lake, die mehrere Services kennen: `month_prefix` (Bronze-Präfix eines Monats) und `runs_key` (Ablageort der Reports) |
| `timing` | `now_utc` (UTC auf ganze Sekunden) und `elapsed` (gerundete Laufzeit seit `time.monotonic()`) |

Ingestion bindet die Bibliothek als uv-Pfadabhängigkeit ein (`[tool.uv.sources]`), das Spark-Image installiert sie per `pip` ins Python des Images. Deshalb ist der Build-Kontext beider Images das Repo-Root.

```sh
uv run --directory libs/pipeline_common pytest
```
