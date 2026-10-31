# pipeline_common

Gemeinsamer Code der Pipeline-Services, ohne Abhängigkeiten und lauffähig ab Python 3.10, weil der Spark-Container diese Version hat.

| Modul | Inhalt |
| --- | --- |
| `periods` | `Month` (mit `next()` und `partition`), `parse_period` und `months_from_env` für `INGEST_PERIOD` (Quartal `2024Q1` oder Monat `2024-01`) |
| `run_id` | `read_run_id`: liest und prüft die Run-ID, die `run-init` ins Volume schreibt; `started_at`: Startzeit aus der Run-ID |
| `env` | `required`, `optional` und `ConfigError` für Umgebungsvariablen |
| `logs` | `setup_logging` mit dem gemeinsamen Format |
| `reports` | `run_report` (Aufbau des JSON-Reports eines Schritts), `runs_key` (Ablageort des Reports), `exit_code` und die Konstanten `EXIT_*` (0, 1 bei Fehlern, 2 bei Konfigurationsfehlern, 3 bei fehlenden Quelldaten) |
| `timing` | `now_utc` (UTC auf ganze Sekunden) und `elapsed` (gerundete Laufzeit seit `time.monotonic()`) |

Ingestion bindet die Bibliothek als uv-Pfadabhängigkeit ein (`[tool.uv.sources]`), das Spark-Image installiert sie per `pip` ins Python des Images. Deshalb ist der Build-Kontext beider Images das Repo-Root.

```sh
uv run --directory libs/pipeline_common pytest
```
