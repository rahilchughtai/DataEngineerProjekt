# Graph Report - DataEngineerProjekt  (2026-11-03)

## Corpus Check
- 31 files · ~7,501 words
- Verdict: corpus is large enough that graph structure adds value.
- Unclassified: 26 file(s) not represented in the graph (top: (none) 10, .lock 7, .parquet 3)

## Summary
- 402 nodes · 624 edges · 35 communities (25 shown, 10 thin omitted)
- Extraction: 98% EXTRACTED · 2% INFERRED · 0% AMBIGUOUS · INFERRED: 12 edges (avg confidence: 0.85)
- Token cost: 0 input · 0 output

## Graph Freshness
- Built from commit: `680daaba`
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
- tests/conftest.py
- __init__.py
- environment
- services
- tests.yml
- INGEST_PERIOD (Quartal YYYYQn oder Monat YYYY-MM)
- tests.yml
- silver-job
- lake-init
- ingestion
- ml-reader-example
- report
- hourly_demand
- steps
- with
- e2e
- on
- lint
- tests-passed
- steps
- Project Packages
- SeaweedFS Entrypoint
- E2E Project
- Root Project

## God Nodes (most connected - your core abstractions)
1. `ingest_month()` - 14 edges
2. `services` - 10 edges
3. `environment` - 10 edges
4. `to_silver()` - 10 edges
5. `start()` - 10 edges
6. `month_path()` - 10 edges
7. `FakeS3` - 10 edges
8. `seaweedfs-admin` - 9 edges
9. `spark-worker` - 9 edges
10. `write_month()` - 9 edges

## Surprising Connections (you probably didn't know these)
- `period_months()` --calls--> `months()`  [INFERRED]
  services/spark/jobs/common.py → libs/pipeline_common/src/pipeline_common/periods.py
- `month_path()` --calls--> `partition()`  [INFERRED]
  services/spark/jobs/common.py → libs/pipeline_common/src/pipeline_common/periods.py
- `start()` --calls--> `setup_logging()`  [INFERRED]
  services/spark/jobs/common.py → libs/pipeline_common/src/pipeline_common/logs.py
- `main()` --calls--> `setup_logging()`  [EXTRACTED]
  services/ingestion/ingest.py → libs/pipeline_common/src/pipeline_common/logs.py
- `main()` --calls--> `months()`  [EXTRACTED]
  services/ingestion/ingest.py → libs/pipeline_common/src/pipeline_common/periods.py

## Import Cycles
- None detected.

## Hyperedges (group relationships)
- **Medallion data flow Bronze -> Silver -> Gold via Spark jobs** — figures_architektur_ingestion_service, figures_architektur_bronze, figures_architektur_spark_job_1, figures_architektur_silver, figures_architektur_spark_job_2, figures_architektur_gold [EXTRACTED 1.00]

## Communities (35 total, 10 thin omitted)

### Community 0 - "e2e/conftest.py"
Cohesion: 0.09
Nodes (19): bucket_exists(), log(), main(), s3_client(), download_to_bronze(), file_name(), http_session(), ingest_month() (+11 more)

### Community 1 - "bronze_to_silver.py"
Cohesion: 0.11
Nodes (14): setup_logging(), test_quiet_loggers_only_warn(), main(), out_of_range(), reject_reason(), month_bounds(), month_path(), period_months() (+6 more)

### Community 2 - "pytest"
Cohesion: 0.22
Nodes (5): months(), partition(), test_months(), test_months_rejects_invalid(), test_partition_is_zero_padded()

### Community 3 - "test_pipeline.py"
Cohesion: 0.15
Nodes (8): allowed(), distinct_rows(), fixture_file(), test_access_matches_policy(), test_bronze_stores_source_files_unchanged(), test_one_file_per_month(), test_report_adds_up(), test_silver_pickups_lie_in_their_month()

### Community 4 - "README: Batch-Datenarchitektur für Taxinachfrage"
Cohesion: 0.15
Nodes (11): README: Batch-Datenarchitektur für Taxinachfrage, CI-Workflow tests.yml (unit-tests, lint, e2e, tests-passed), lake-init (legt Buckets an), Data Lake Bronze/Silver/Gold (SeaweedFS S3), Identität ml_reader (liest Gold), NYC TLC Yellow Taxi Trip Records, Laufbericht gold/_runs/pipeline/<run_id>.json, Troubleshooting: Podman npipe fehlt (stale api.sock, podman#29971) (+3 more)

### Community 5 - "test_bronze_to_silver.py"
Cohesion: 0.20
Nodes (10): harmonize(), to_silver(), reason(), test_december_bounds_roll_over_into_next_year(), test_exact_duplicates_are_removed(), test_rules_reject(), test_silver_keeps_only_needed_columns(), test_type_drift_is_cast_and_unconvertible_values_become_null() (+2 more)

### Community 6 - "silver-job"
Cohesion: 0.23
Nodes (6): credentials(), keys(), lake_fs(), report(), s3(), s3_client()

### Community 7 - "services"
Cohesion: 0.14
Nodes (22): spark-master, replicas, SPARK_WORKER_CORES, SPARK_WORKER_MEMORY, interval, retries, start_period, test (+14 more)

### Community 9 - "__init__.py"
Cohesion: 0.25
Nodes (8): silver-job, command, depends_on, image, pull_policy, restart, gold-job, condition

### Community 10 - "environment"
Cohesion: 0.09
Nodes (31): INGEST_PERIOD, LAKE_BUCKETS, S3_ADMIN_ACCESS_KEY, S3_ADMIN_SECRET_KEY, S3_ENDPOINT, S3_INGESTION_ACCESS_KEY, S3_INGESTION_SECRET_KEY, S3_ML_ACCESS_KEY (+23 more)

### Community 12 - "services"
Cohesion: 0.06
Nodes (39): context, dockerfile, lake-init, seaweedfs, build, depends_on, image, restart (+31 more)

### Community 14 - "tests.yml"
Cohesion: 0.27
Nodes (13): Bronze Layer (Rohdaten, unveraendert), Data Lake SeaweedFS (S3), Architecture Diagram: Batch-Datenarchitektur Trainingsdaten zur Taxinachfrage, Docker Compose (container boundary), Gold Layer (aggregierte Features, Parquet), Ingestion-Service (Python, Download der Monatsdaten), ML-Anwendung (Modell zur Taxinachfrage, liest Gold 1x pro Quartal), NYC TLC Yellow Taxi Trip Records (monthly Parquet via HTTPS) (+5 more)

### Community 16 - "tests.yml"
Cohesion: 0.20
Nodes (9): concurrency, cancel-in-progress, group, env, UV_VERSION, jobs, name, permissions (+1 more)

### Community 17 - "silver-job"
Cohesion: 0.20
Nodes (10): ingestion, spark-worker, condition, silver-job, command, depends_on, image, pull_policy (+2 more)

### Community 19 - "ingestion"
Cohesion: 0.17
Nodes (12): run, unit-tests, project, working-directory, fail-fast, matrix, defaults, if (+4 more)

### Community 21 - "report"
Cohesion: 0.25
Nodes (8): gold-job, condition, command, depends_on, image, pull_policy, restart, report

### Community 22 - "hourly_demand"
Cohesion: 0.35
Nodes (5): hourly_demand(), test_calendar_features(), test_counts_and_averages_per_zone_and_hour(), test_every_zone_and_hour_of_the_month_has_a_row(), trip()

### Community 23 - "steps"
Cohesion: 0.29
Nodes (10): steps, NEEDS, steps, env, if, name, run, uses (+2 more)

### Community 24 - "with"
Cohesion: 0.25
Nodes (8): with, cache-dependency-glob, distribution, enable-cache, java-version, key, path, version

### Community 25 - "e2e"
Cohesion: 0.33
Nodes (6): env, if, runs-on, timeout-minutes, COMPOSE, e2e

### Community 26 - "on"
Cohesion: 0.40
Nodes (5): on, pull_request, push, workflow_dispatch, branches

### Community 27 - "lint"
Cohesion: 0.50
Nodes (4): lint, if, runs-on, timeout-minutes

### Community 28 - "tests-passed"
Cohesion: 0.50
Nodes (4): tests-passed, if, needs, runs-on

### Community 30 - "steps"
Cohesion: 0.25
Nodes (7): exclude, args, id, repos, hooks, repo, rev

### Community 33 - "Project Packages"
Cohesion: 0.67
Nodes (3): ingestion, pipeline-common, spark-jobs

## Knowledge Gaps
- **13 isolated node(s):** `ml-reader-example`, `lake-init`, `ingestion`, `spark-jobs`, `e2e` (+8 more)
  These have ≤1 connection - possible missing edges or undocumented components. (Counts symbols only; 170 node(s) total have ≤1 connection when file, concept and rationale nodes are included.)
- **10 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `services` connect `services` to `services`, `__init__.py`, `environment`, `silver-job`, `report`?**
  _High betweenness centrality (0.062) - this node is a cross-community bridge._
- **Are the 4 inferred relationships involving `to_silver()` (e.g. with `reason()` and `test_exact_duplicates_are_removed()`) actually correct?**
  _`to_silver()` has 4 INFERRED edges - model-reasoned connections that need verification._
- **What connects `ml-reader-example`, `lake-init`, `ingestion` to the rest of the system?**
  _13 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `e2e/conftest.py` be split into smaller, more focused modules?**
  _Cohesion score 0.08792270531400966 - nodes in this community are weakly interconnected._
- **Why does `spark-worker` connect `services` to `services`?**
  _High betweenness centrality (0.018) - this node is a cross-community bridge._
- **Should `bronze_to_silver.py` be split into smaller, more focused modules?**
  _Cohesion score 0.10661268556005399 - nodes in this community are weakly interconnected._
- **Why does `seaweedfs` connect `environment` to `services`, `services`?**
  _High betweenness centrality (0.014) - this node is a cross-community bridge._