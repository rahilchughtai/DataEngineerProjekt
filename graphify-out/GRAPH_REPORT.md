# Graph Report - DataEngineerProjekt  (2026-10-31)

## Corpus Check
- 30 files · ~7,009 words
- Verdict: corpus is large enough that graph structure adds value.
- Unclassified: 24 file(s) not represented in the graph (top: (none) 10, .lock 6, .parquet 3)

## Summary
- 390 nodes · 599 edges · 29 communities (18 shown, 11 thin omitted)
- Extraction: 98% EXTRACTED · 2% INFERRED · 0% AMBIGUOUS · INFERRED: 9 edges (avg confidence: 0.85)
- Token cost: 0 input · 0 output

## Graph Freshness
- Built from commit: `1776b0cc`
- Run `git rev-parse HEAD` and compare to check if the graph is stale.
- Run `graphify update .` after code changes (no API cost).

## Community Hubs (Navigation)
- e2e/conftest.py
- bronze_to_silver.py
- pytest
- test_pipeline.py
- README: Batch-Datenarchitektur für Taxinachfrage
- test_bronze_to_silver.py
- silver-job
- services
- report
- __init__.py
- environment
- INGEST_PERIOD (Quartal YYYYQn oder Monat YYYY-MM)
- tests.yml
- INGEST_PERIOD (Quartal YYYYQn oder Monat YYYY-MM)
- with
- silver-job
- lake-init
- ingestion
- steps
- Project Packages
- SeaweedFS Entrypoint
- E2E Project
- Root Project

## God Nodes (most connected - your core abstractions)
1. `ingest_month()` - 14 edges
2. `environment` - 10 edges
3. `FakeS3` - 10 edges
4. `to_silver()` - 10 edges
5. `start()` - 10 edges
6. `month_path()` - 10 edges
7. `services` - 9 edges
8. `spark-worker` - 9 edges
9. `http_session()` - 9 edges
10. `write_month()` - 9 edges

## Surprising Connections (you probably didn't know these)
- `main()` --calls--> `setup_logging()`  [INFERRED]
  services/ingestion/ingest.py → libs/pipeline_common/src/pipeline_common/logs.py
- `main()` --calls--> `months()`  [EXTRACTED]
  services/ingestion/ingest.py → libs/pipeline_common/src/pipeline_common/periods.py
- `period_months()` --calls--> `months()`  [EXTRACTED]
  services/spark/jobs/common.py → libs/pipeline_common/src/pipeline_common/periods.py
- `object_key()` --calls--> `partition()`  [EXTRACTED]
  services/ingestion/ingest.py → libs/pipeline_common/src/pipeline_common/periods.py
- `month_path()` --calls--> `partition()`  [EXTRACTED]
  services/spark/jobs/common.py → libs/pipeline_common/src/pipeline_common/periods.py

## Import Cycles
- None detected.

## Hyperedges (group relationships)
- **Medallion data flow Bronze -> Silver -> Gold via Spark jobs** — figures_architektur_ingestion_service, figures_architektur_bronze, figures_architektur_spark_job_1, figures_architektur_silver, figures_architektur_spark_job_2, figures_architektur_gold [EXTRACTED 1.00]

## Communities (29 total, 11 thin omitted)

### Community 0 - "e2e/conftest.py"
Cohesion: 0.11
Nodes (15): download_to_bronze(), file_name(), http_session(), ingest_month(), main(), object_exists(), object_key(), FakeS3 (+7 more)

### Community 1 - "bronze_to_silver.py"
Cohesion: 0.11
Nodes (15): setup_logging(), test_quiet_loggers_only_warn(), first_instant(), main(), out_of_range(), reject_reason(), month_path(), period_months() (+7 more)

### Community 2 - "pytest"
Cohesion: 0.22
Nodes (5): months(), partition(), test_months(), test_months_rejects_invalid(), test_partition_is_zero_padded()

### Community 3 - "test_pipeline.py"
Cohesion: 0.14
Nodes (8): allowed(), distinct_rows(), fixture_file(), test_access_matches_policy(), test_bronze_stores_source_files_unchanged(), test_one_file_per_month(), test_report_adds_up(), test_silver_pickups_lie_in_their_month()

### Community 4 - "README: Batch-Datenarchitektur für Taxinachfrage"
Cohesion: 0.15
Nodes (11): README: Batch-Datenarchitektur für Taxinachfrage, CI-Workflow tests.yml (unit-tests, lint, e2e, tests-passed), lake-init (legt Buckets an), Data Lake Bronze/Silver/Gold (SeaweedFS S3), Identität ml_reader (liest Gold), NYC TLC Yellow Taxi Trip Records, Laufbericht gold/_runs/pipeline/<run_id>.json, Troubleshooting: Podman npipe fehlt (stale api.sock, podman#29971) (+3 more)

### Community 5 - "test_bronze_to_silver.py"
Cohesion: 0.09
Nodes (15): harmonize(), to_silver(), bronze(), spark(), reason(), test_december_bounds_roll_over_into_next_year(), test_exact_duplicates_are_removed(), test_rules_reject() (+7 more)

### Community 6 - "silver-job"
Cohesion: 0.16
Nodes (10): bucket_exists(), log(), main(), s3_client(), credentials(), keys(), lake_fs(), report() (+2 more)

### Community 7 - "services"
Cohesion: 0.13
Nodes (21): spark-master, replicas, SPARK_WORKER_CORES, SPARK_WORKER_MEMORY, interval, retries, start_period, test (+13 more)

### Community 8 - "report"
Cohesion: 0.25
Nodes (8): gold-job, condition, command, depends_on, image, pull_policy, restart, report

### Community 9 - "__init__.py"
Cohesion: 0.25
Nodes (8): silver-job, command, depends_on, image, pull_policy, restart, gold-job, condition

### Community 10 - "environment"
Cohesion: 0.13
Nodes (25): INGEST_PERIOD, LAKE_BUCKETS, S3_ADMIN_ACCESS_KEY, S3_ADMIN_SECRET_KEY, S3_ENDPOINT, S3_INGESTION_ACCESS_KEY, S3_INGESTION_SECRET_KEY, S3_ML_ACCESS_KEY (+17 more)

### Community 12 - "INGEST_PERIOD (Quartal YYYYQn oder Monat YYYY-MM)"
Cohesion: 0.06
Nodes (37): context, dockerfile, lake-init, seaweedfs, build, depends_on, image, restart (+29 more)

### Community 14 - "tests.yml"
Cohesion: 0.27
Nodes (13): Bronze Layer (Rohdaten, unveraendert), Data Lake SeaweedFS (S3), Architecture Diagram: Batch-Datenarchitektur Trainingsdaten zur Taxinachfrage, Docker Compose (container boundary), Gold Layer (aggregierte Features, Parquet), Ingestion-Service (Python, Download der Monatsdaten), ML-Anwendung (Modell zur Taxinachfrage, liest Gold 1x pro Quartal), NYC TLC Yellow Taxi Trip Records (monthly Parquet via HTTPS) (+5 more)

### Community 16 - "with"
Cohesion: 0.05
Nodes (46): concurrency, cancel-in-progress, group, env, if, runs-on, steps, timeout-minutes (+38 more)

### Community 17 - "silver-job"
Cohesion: 0.20
Nodes (10): ingestion, spark-worker, condition, silver-job, command, depends_on, image, pull_policy (+2 more)

### Community 19 - "ingestion"
Cohesion: 0.17
Nodes (12): run, unit-tests, project, working-directory, fail-fast, matrix, defaults, if (+4 more)

### Community 30 - "steps"
Cohesion: 0.25
Nodes (7): exclude, args, id, repos, hooks, repo, rev

### Community 33 - "Project Packages"
Cohesion: 0.67
Nodes (3): ingestion, pipeline-common, spark-jobs

## Knowledge Gaps
- **12 isolated node(s):** `lake-init`, `ingestion`, `spark-jobs`, `e2e`, `taxi-batch-pipeline` (+7 more)
  These have ≤1 connection - possible missing edges or undocumented components. (Counts symbols only; 162 node(s) total have ≤1 connection when file, concept and rationale nodes are included.)
- **11 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `services` connect `INGEST_PERIOD (Quartal YYYYQn oder Monat YYYY-MM)` to `report`, `__init__.py`, `silver-job`, `services`?**
  _High betweenness centrality (0.055) - this node is a cross-community bridge._
- **Are the 4 inferred relationships involving `to_silver()` (e.g. with `reason()` and `test_exact_duplicates_are_removed()`) actually correct?**
  _`to_silver()` has 4 INFERRED edges - model-reasoned connections that need verification._
- **What connects `lake-init`, `ingestion`, `spark-jobs` to the rest of the system?**
  _12 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `e2e/conftest.py` be split into smaller, more focused modules?**
  _Cohesion score 0.1111111111111111 - nodes in this community are weakly interconnected._
- **Why does `spark-worker` connect `services` to `INGEST_PERIOD (Quartal YYYYQn oder Monat YYYY-MM)`?**
  _High betweenness centrality (0.018) - this node is a cross-community bridge._
- **Should `bronze_to_silver.py` be split into smaller, more focused modules?**
  _Cohesion score 0.10810810810810811 - nodes in this community are weakly interconnected._
- **Why does `seaweedfs` connect `INGEST_PERIOD (Quartal YYYYQn oder Monat YYYY-MM)` to `environment`, `services`?**
  _High betweenness centrality (0.014) - this node is a cross-community bridge._