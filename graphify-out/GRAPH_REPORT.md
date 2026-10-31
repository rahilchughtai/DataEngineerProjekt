# Graph Report - DataEngineerProjekt  (2026-10-31)

## Corpus Check
- 59 files · ~18,119 words
- Verdict: corpus is large enough that graph structure adds value.
- Unclassified: 25 file(s) not represented in the graph (top: (none) 11, .lock 5, .parquet 3)

## Summary
- 811 nodes · 1822 edges · 48 communities (37 shown, 11 thin omitted)
- Extraction: 88% EXTRACTED · 12% INFERRED · 0% AMBIGUOUS · INFERRED: 215 edges (avg confidence: 0.91)
- Token cost: 0 input · 0 output

## Graph Freshness
- Built from commit: `f28a50d3`
- Run `git rev-parse HEAD` and compare to check if the graph is stale.
- Run `graphify update .` after code changes (no API cost).

## Community Hubs (Navigation)
- Ingestion Download & Storage
- pipeline_report.py
- Bronze to Silver Job
- E2E Test Container
- Architecture Docs & Concepts
- harmonize
- json
- spark-worker
- Month
- silver_to_gold.py
- environment
- bronze_to_silver.py
- Compose Networks & Anchors
- Compose Logging Config
- Architecture Diagram
- Lake Init Service
- Spark Job Env Anchor
- CI Workflow Settings
- test_quality.py
- CI Unit Test Matrix
- Init Chain Dependencies
- CI Job Steps
- test_main_rejects_invalid_run_id_before_spark_starts
- seaweedfs
- run-report
- Run Init Service
- healthcheck
- tests-passed
- CI Action Inputs
- Spark Job Anchor
- Pre-commit Hooks
- spark-master
- e2e
- Project Packages
- SeaweedFS Entrypoint
- Bucket Init Script
- Spark Master Routing
- Spark Worker Start
- Spark Job Submit
- collections_abc
- E2E Project
- Root Project
- networks
- quality.py
- lint
- logs.py
- on

## God Nodes (most connected - your core abstractions)
1. `LakePaths` - 60 edges
2. `Month` - 47 edges
3. `Config` - 27 edges
4. `Lake` - 22 edges
5. `process_month()` - 17 edges
6. `run()` - 17 edges
7. `put_bronze()` - 17 edges
8. `download()` - 16 edges
9. `main()` - 16 edges
10. `ingest_month()` - 15 edges

## Surprising Connections (you probably didn't know these)
- `Abgleich sum(trip_count) = Silver-Zeilen, genau eine Silver-Run-ID` --semantically_similar_to--> `Invarianten-Prüfungen (Laufbericht, Bronze, Silver, Gold, Lineage, Zugriffsrechte)`  [INFERRED] [semantically similar]
  services/spark/README.md → tests/e2e/README.md
- `test_object_key_is_deterministic()` --uses--> `Month`  [INFERRED]
  services/ingestion/tests/test_storage.py → libs/pipeline_common/src/pipeline_common/periods.py
- `test_object_key_is_partitioned_by_time()` --uses--> `Month`  [INFERRED]
  services/ingestion/tests/test_storage.py → libs/pipeline_common/src/pipeline_common/periods.py
- `ingest_month()` --uses--> `Month`  [INFERRED]
  services/ingestion/src/ingestion/job.py → libs/pipeline_common/src/pipeline_common/periods.py
- `object_key()` --uses--> `Month`  [INFERRED]
  services/ingestion/src/ingestion/storage.py → libs/pipeline_common/src/pipeline_common/periods.py

## Import Cycles
- None detected.

## Hyperedges (group relationships)
- **Medallion data flow Bronze -> Silver -> Gold via Spark jobs** — figures_architektur_ingestion_service, figures_architektur_bronze, figures_architektur_spark_job_1, figures_architektur_silver, figures_architektur_spark_job_2, figures_architektur_gold [EXTRACTED 1.00]
- **Batch-Kette per docker compose up: run-init/lake-init → Ingestion → Job 1 → Job 2 → Laufbericht** — readme_run_init, readme_lake_init, services_ingestion_readme, services_spark_readme_job1_bronze_to_silver, services_spark_readme_job2_silver_to_gold, services_spark_readme_pipeline_report [EXTRACTED 1.00]
- **Lineage über Run-ID und Reports von Bronze über Silver bis Gold** — readme_run_id_lineage, services_ingestion_readme_integrity_metadata, services_spark_readme_silver_report, services_spark_readme_gold_report, readme_pipeline_run_report [INFERRED 0.85]
- **Idempotenz-Mechanismen der Pipeline** — readme_idempotent_partition_overwrite, services_ingestion_readme_etag_idempotency, services_spark_readme_dynamic_partition_overwrite, services_spark_readme_lake_py [INFERRED 0.85]

## Communities (48 total, 11 thin omitted)

### Community 0 - "Ingestion Download & Storage"
Cohesion: 0.05
Nodes (41): count_statuses(), run_report(), test_count_statuses(), test_exit_code(), test_run_report_with_and_without_job(), Config, _check_status(), download() (+33 more)

### Community 1 - "pipeline_report.py"
Cohesion: 0.08
Nodes (21): delete(), exists(), _fs(), read_text(), write_partition(), write_text(), build_report(), MissingReportError (+13 more)

### Community 2 - "Bronze to Silver Job"
Cohesion: 0.20
Nodes (22): process_month(), _process_month(), run(), write_month(), LakePaths, partition(), january_sample(), paths() (+14 more)

### Community 3 - "E2E Test Container"
Cohesion: 0.08
Nodes (22): lake(), run_id(), settings(), tables(), Lake, Settings, sha256(), by_month() (+14 more)

### Community 4 - "Architecture Docs & Concepts"
Cohesion: 0.06
Nodes (42): pipeline_common README, env (required, optional, ConfigError), logs (setup_logging), periods (Month, parse_period, months_from_env), reports (run_report, exit_code 0/1/3), run_id (read_run_id), README: Batch-Datenarchitektur für Taxinachfrage, CI-Workflow tests.yml (unit-tests, lint, e2e, tests-passed) (+34 more)

### Community 5 - "harmonize"
Cohesion: 0.21
Nodes (6): harmonize(), SchemaError, test_harmonize_casts_type_drift(), test_harmonize_keeps_only_needed_columns(), test_harmonize_reports_missing_columns(), test_harmonize_turns_unconvertible_values_into_null()

### Community 6 - "json"
Cohesion: 0.15
Nodes (9): FakeS3, test_is_current(), test_object_key_is_deterministic(), test_object_key_is_partitioned_by_time(), test_stored_metadata_missing_object(), test_stored_metadata_reraises_other_errors(), test_stored_metadata_returns_metadata(), test_upload_sets_lineage_metadata() (+1 more)

### Community 7 - "spark-worker"
Cohesion: 0.14
Nodes (14): spark-master, replicas, SPARK_WORKER_CORES, SPARK_WORKER_MEMORY, spark-worker, condition, command, depends_on (+6 more)

### Community 8 - "Month"
Cohesion: 0.06
Nodes (42): ConfigError, optional(), required(), Month, months_from_env(), parse_month(), parse_period(), period_from_env() (+34 more)

### Community 9 - "silver_to_gold.py"
Cohesion: 0.11
Nodes (28): add_calendar_features(), aggregate(), build_gold(), check_month(), hour_grid(), process_month(), _process_month(), ReconciliationError (+20 more)

### Community 10 - "environment"
Cohesion: 0.07
Nodes (49): ingestion, silver-job, spark-worker, BRONZE_BUCKET, GOLD_BUCKET, INGEST_PERIOD, LOG_LEVEL, PIPELINE_RUN_ID_FILE (+41 more)

### Community 11 - "bronze_to_silver.py"
Cohesion: 0.17
Nodes (7): deduplicate(), files_for(), prepare(), rejected_rows(), valid_rows(), classify(), test_files_for()

### Community 12 - "Compose Networks & Anchors"
Cohesion: 0.13
Nodes (14): name, name, name, services, volumes, pipeline-run, seaweedfs-data, x-healthcheck (+6 more)

### Community 13 - "Compose Logging Config"
Cohesion: 0.23
Nodes (15): logging, logging, logging, driver, options, max-file, max-size, logging (+7 more)

### Community 14 - "Architecture Diagram"
Cohesion: 0.27
Nodes (13): Bronze Layer (Rohdaten, unveraendert), Data Lake SeaweedFS (S3), Architecture Diagram: Batch-Datenarchitektur Trainingsdaten zur Taxinachfrage, Docker Compose (container boundary), Gold Layer (aggregierte Features, Parquet), Ingestion-Service (Python, Download der Monatsdaten), ML-Anwendung (Modell zur Taxinachfrage, liest Gold 1x pro Quartal), NYC TLC Yellow Taxi Trip Records (monthly Parquet via HTTPS) (+5 more)

### Community 15 - "Lake Init Service"
Cohesion: 0.17
Nodes (12): seaweedfs, LAKE_BUCKETS, SEAWEEDFS_MASTER, depends_on, entrypoint, environment, image, networks (+4 more)

### Community 16 - "Spark Job Env Anchor"
Cohesion: 0.17
Nodes (12): x-spark-job-env, BRONZE_BUCKET, GOLD_BUCKET, INGEST_PERIOD, LOG_LEVEL, PIPELINE_RUN_ID_FILE, S3_ENDPOINT, S3_SPARK_ACCESS_KEY (+4 more)

### Community 17 - "CI Workflow Settings"
Cohesion: 0.20
Nodes (9): concurrency, cancel-in-progress, group, env, UV_VERSION, jobs, name, permissions (+1 more)

### Community 18 - "test_quality.py"
Cohesion: 0.37
Nodes (8): reason(), test_december_bounds_roll_over_year(), test_first_violated_rule_wins(), test_good_trip_passes(), test_large_finite_amount_passes(), test_missing_passenger_count_is_allowed(), test_month_bounds(), test_rule_rejects()

### Community 19 - "CI Unit Test Matrix"
Cohesion: 0.17
Nodes (12): run, unit-tests, project, working-directory, fail-fast, matrix, defaults, if (+4 more)

### Community 20 - "Init Chain Dependencies"
Cohesion: 0.20
Nodes (10): lake-init, run-init, depends_on, image, networks, restart, volumes, condition (+2 more)

### Community 21 - "CI Job Steps"
Cohesion: 0.29
Nodes (10): steps, NEEDS, steps, env, if, name, run, uses (+2 more)

### Community 23 - "seaweedfs"
Cohesion: 0.20
Nodes (11): context, dockerfile, build, build, image, networks, ports, restart (+3 more)

### Community 24 - "run-report"
Cohesion: 0.18
Nodes (11): gold-job, condition, command, depends_on, entrypoint, image, networks, pull_policy (+3 more)

### Community 25 - "Run Init Service"
Cohesion: 0.25
Nodes (8): command, entrypoint, image, network_mode, pull_policy, restart, volumes, run-init

### Community 26 - "healthcheck"
Cohesion: 0.54
Nodes (8): interval, retries, start_period, test, timeout, healthcheck, healthcheck, healthcheck

### Community 27 - "tests-passed"
Cohesion: 0.50
Nodes (4): tests-passed, if, needs, runs-on

### Community 28 - "CI Action Inputs"
Cohesion: 0.25
Nodes (8): with, cache-dependency-glob, distribution, enable-cache, java-version, key, path, version

### Community 29 - "Spark Job Anchor"
Cohesion: 0.29
Nodes (7): x-spark-job, command, image, networks, pull_policy, restart, volumes

### Community 30 - "Pre-commit Hooks"
Cohesion: 0.29
Nodes (6): args, id, repos, hooks, repo, rev

### Community 31 - "spark-master"
Cohesion: 0.33
Nodes (6): spark-master, command, image, networks, ports, restart

### Community 32 - "e2e"
Cohesion: 0.33
Nodes (6): env, if, runs-on, timeout-minutes, COMPOSE, e2e

### Community 33 - "Project Packages"
Cohesion: 0.67
Nodes (3): ingestion, pipeline-common, spark-jobs

### Community 42 - "networks"
Cohesion: 0.40
Nodes (5): name, networks, lakenet, sparknet, name

### Community 43 - "quality.py"
Cohesion: 0.31
Nodes (6): _amount_invalid(), _duration_seconds(), _location_invalid(), month_bounds(), _out_of_range(), _pickup_outside_month()

### Community 44 - "lint"
Cohesion: 0.50
Nodes (4): lint, if, runs-on, timeout-minutes

### Community 46 - "on"
Cohesion: 0.40
Nodes (5): on, pull_request, push, workflow_dispatch, branches

## Knowledge Gaps
- **14 isolated node(s):** `taxi-batch-pipeline`, `ingestion`, `spark-jobs`, `e2e`, `env (required, optional, ConfigError)` (+9 more)
  These have ≤1 connection - possible missing edges or undocumented components. (Counts symbols only; 278 node(s) total have ≤1 connection when file, concept and rationale nodes are included.)
- **11 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `Month` connect `Month` to `Ingestion Download & Storage`, `pipeline_report.py`, `Bronze to Silver Job`, `json`, `silver_to_gold.py`, `bronze_to_silver.py`, `quality.py`, `test_quality.py`?**
  _High betweenness centrality (0.040) - this node is a cross-community bridge._
- **Are the 46 inferred relationships involving `LakePaths` (e.g. with `main()` and `process_month()`) actually correct?**
  _`LakePaths` has 46 INFERRED edges - model-reasoned connections that need verification._
- **What connects `taxi-batch-pipeline`, `ingestion`, `spark-jobs` to the rest of the system?**
  _14 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `Ingestion Download & Storage` be split into smaller, more focused modules?**
  _Cohesion score 0.05263157894736842 - nodes in this community are weakly interconnected._
- **Why does `LakePaths` connect `Bronze to Silver Job` to `Month`, `pipeline_report.py`, `bronze_to_silver.py`, `silver_to_gold.py`?**
  _High betweenness centrality (0.035) - this node is a cross-community bridge._
- **Are the 37 inferred relationships involving `Month` (e.g. with `test_month_str_is_zero_padded()` and `test_month_validates_range()`) actually correct?**
  _`Month` has 37 INFERRED edges - model-reasoned connections that need verification._
- **Should `pipeline_report.py` be split into smaller, more focused modules?**
  _Cohesion score 0.08244680851063829 - nodes in this community are weakly interconnected._