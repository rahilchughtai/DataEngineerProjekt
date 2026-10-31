# Graph Report - DataEngineerProjekt  (2026-10-31)

## Corpus Check
- 30 files · ~6,721 words
- Verdict: corpus is large enough that graph structure adds value.
- Unclassified: 25 file(s) not represented in the graph (top: (none) 11, .lock 6, .parquet 3)

## Summary
- 376 nodes · 578 edges · 24 communities (17 shown, 7 thin omitted)
- Extraction: 99% EXTRACTED · 1% INFERRED · 0% AMBIGUOUS · INFERRED: 8 edges (avg confidence: 0.85)
- Token cost: 0 input · 0 output

## Graph Freshness
- Built from commit: `dd439d97`
- Run `git rev-parse HEAD` and compare to check if the graph is stale.
- Run `graphify update .` after code changes (no API cost).

## Community Hubs (Navigation)
- ingest.py
- bronze_to_silver.py
- silver-job
- test_pipeline.py
- README: Batch-Datenarchitektur für Taxinachfrage
- test_bronze_to_silver.py
- e2e/conftest.py
- services
- tests/conftest.py
- gold-job
- environment
- services
- tests.yml
- INGEST_PERIOD (Quartal YYYYQn oder Monat YYYY-MM)
- with
- lake-init
- ingestion
- steps
- Project Packages
- SeaweedFS Entrypoint
- E2E Project
- Root Project

## God Nodes (most connected - your core abstractions)
1. `environment` - 10 edges
2. `to_silver()` - 10 edges
3. `start()` - 10 edges
4. `month_path()` - 10 edges
5. `with` - 9 edges
6. `services` - 9 edges
7. `spark-worker` - 9 edges
8. `ingest_month()` - 9 edges
9. `write_month()` - 9 edges
10. `unit-tests` - 8 edges

## Surprising Connections (you probably didn't know these)
- `main()` --calls--> `setup_logging()`  [EXTRACTED]
  services/ingestion/ingest.py → libs/pipeline_common/src/pipeline_common/logs.py
- `period_months()` --calls--> `months()`  [EXTRACTED]
  services/spark/jobs/common.py → libs/pipeline_common/src/pipeline_common/periods.py
- `month_path()` --calls--> `partition()`  [EXTRACTED]
  services/spark/jobs/common.py → libs/pipeline_common/src/pipeline_common/periods.py
- `ingestion` --depends_on--> `pipeline-common`  [EXTRACTED]
  services/ingestion/pyproject.toml → libs/pipeline_common/pyproject.toml
- `spark-jobs` --depends_on--> `pipeline-common`  [EXTRACTED]
  services/spark/pyproject.toml → libs/pipeline_common/pyproject.toml

## Import Cycles
- None detected.

## Hyperedges (group relationships)
- **Medallion data flow Bronze -> Silver -> Gold via Spark jobs** — figures_architektur_ingestion_service, figures_architektur_bronze, figures_architektur_spark_job_1, figures_architektur_silver, figures_architektur_spark_job_2, figures_architektur_gold [EXTRACTED 1.00]

## Communities (24 total, 7 thin omitted)

### Community 0 - "ingest.py"
Cohesion: 0.09
Nodes (16): months(), partition(), test_months(), test_months_rejects_invalid(), test_partition_is_zero_padded(), exists(), file_name(), http_session() (+8 more)

### Community 1 - "bronze_to_silver.py"
Cohesion: 0.12
Nodes (14): setup_logging(), test_quiet_loggers_only_warn(), first_instant(), main(), out_of_range(), reject_reason(), month_path(), period_months() (+6 more)

### Community 2 - "silver-job"
Cohesion: 0.20
Nodes (10): ingestion, spark-worker, condition, silver-job, command, depends_on, image, pull_policy (+2 more)

### Community 3 - "test_pipeline.py"
Cohesion: 0.14
Nodes (8): allowed(), distinct_rows(), fixture_file(), test_access_matches_policy(), test_bronze_stores_source_files_unchanged(), test_one_file_per_month(), test_report_adds_up(), test_silver_pickups_lie_in_their_month()

### Community 4 - "README: Batch-Datenarchitektur für Taxinachfrage"
Cohesion: 0.15
Nodes (11): README: Batch-Datenarchitektur für Taxinachfrage, CI-Workflow tests.yml (unit-tests, lint, e2e, tests-passed), lake-init (legt Buckets an), Data Lake Bronze/Silver/Gold (SeaweedFS S3), Identität ml_reader (liest Gold), NYC TLC Yellow Taxi Trip Records, Laufbericht gold/_runs/pipeline/<run_id>.json, Troubleshooting: Podman npipe fehlt (stale api.sock, podman#29971) (+3 more)

### Community 5 - "test_bronze_to_silver.py"
Cohesion: 0.20
Nodes (10): harmonize(), to_silver(), reason(), test_december_bounds_roll_over_into_next_year(), test_exact_duplicates_are_removed(), test_rules_reject(), test_silver_keeps_only_needed_columns(), test_type_drift_is_cast_and_unconvertible_values_become_null() (+2 more)

### Community 6 - "e2e/conftest.py"
Cohesion: 0.16
Nodes (10): bucket_exists(), log(), main(), s3_client(), credentials(), keys(), lake_fs(), report() (+2 more)

### Community 7 - "services"
Cohesion: 0.13
Nodes (21): spark-master, replicas, SPARK_WORKER_CORES, SPARK_WORKER_MEMORY, interval, retries, start_period, test (+13 more)

### Community 8 - "tests/conftest.py"
Cohesion: 0.13
Nodes (6): hourly_demand(), bronze(), spark(), test_calendar_features(), test_counts_and_averages_per_zone_and_hour(), trip()

### Community 9 - "gold-job"
Cohesion: 0.25
Nodes (8): silver-job, command, depends_on, image, pull_policy, restart, gold-job, condition

### Community 10 - "environment"
Cohesion: 0.09
Nodes (33): gold-job, INGEST_PERIOD, LAKE_BUCKETS, S3_ADMIN_ACCESS_KEY, S3_ADMIN_SECRET_KEY, S3_ENDPOINT, S3_INGESTION_ACCESS_KEY, S3_INGESTION_SECRET_KEY (+25 more)

### Community 12 - "services"
Cohesion: 0.06
Nodes (37): context, dockerfile, lake-init, seaweedfs, build, depends_on, image, restart (+29 more)

### Community 14 - "tests.yml"
Cohesion: 0.27
Nodes (13): Bronze Layer (Rohdaten, unveraendert), Data Lake SeaweedFS (S3), Architecture Diagram: Batch-Datenarchitektur Trainingsdaten zur Taxinachfrage, Docker Compose (container boundary), Gold Layer (aggregierte Features, Parquet), Ingestion-Service (Python, Download der Monatsdaten), ML-Anwendung (Modell zur Taxinachfrage, liest Gold 1x pro Quartal), NYC TLC Yellow Taxi Trip Records (monthly Parquet via HTTPS) (+5 more)

### Community 17 - "with"
Cohesion: 0.05
Nodes (46): concurrency, cancel-in-progress, group, env, if, runs-on, steps, timeout-minutes (+38 more)

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
- **12 isolated node(s):** `taxi-batch-pipeline`, `lake-init`, `ingestion`, `spark-jobs`, `e2e` (+7 more)
  These have ≤1 connection - possible missing edges or undocumented components. (Counts symbols only; 153 node(s) total have ≤1 connection when file, concept and rationale nodes are included.)
- **7 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `services` connect `services` to `gold-job`, `environment`, `silver-job`, `services`?**
  _High betweenness centrality (0.059) - this node is a cross-community bridge._
- **Are the 4 inferred relationships involving `to_silver()` (e.g. with `reason()` and `test_exact_duplicates_are_removed()`) actually correct?**
  _`to_silver()` has 4 INFERRED edges - model-reasoned connections that need verification._
- **What connects `taxi-batch-pipeline`, `lake-init`, `ingestion` to the rest of the system?**
  _12 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `ingest.py` be split into smaller, more focused modules?**
  _Cohesion score 0.09487179487179487 - nodes in this community are weakly interconnected._
- **Why does `spark-worker` connect `services` to `services`?**
  _High betweenness centrality (0.020) - this node is a cross-community bridge._
- **Should `bronze_to_silver.py` be split into smaller, more focused modules?**
  _Cohesion score 0.12063492063492064 - nodes in this community are weakly interconnected._
- **Why does `seaweedfs` connect `services` to `environment`, `services`?**
  _High betweenness centrality (0.015) - this node is a cross-community bridge._