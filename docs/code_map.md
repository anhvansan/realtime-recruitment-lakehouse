# Code Map — Tra cứu Khái niệm sang Mã nguồn

Bảng ánh xạ trực tiếp từ các khái niệm lý thuyết / kiến trúc sang vị trí cài đặt cụ thể trong mã nguồn theo quy tắc **Phụ lục A.2**.

| Khái niệm | Module | File:dòng | Symbol | Cập nhật ở task |
| :--- | :--- | :--- | :--- | :--- |
| S3 Decoupling & DuckDB Host:Port | T09, K1 | `src/common/config.py:127` | `AppConfig.s3_endpoint_host_port` | 1.1 |
| Configuration Loader | T09 | `src/common/config.py:152` | `load` | 1.1 |
| CSV Inventory & Triage Profiler | T05, F12 | `scripts/profile_tracking.py:27` | `run_phase_1` | 2.1 |
| Deep Hypotheses Verification (U1-U9, H1, H4) | T05, F13 | `scripts/profile_tracking.py:114` | `run_phase_2` | 2.1 |
| Canonical Event & Funnel Mapping Contract | F13 | `docs/data_dictionary.md:98` | `Event Type Mapping` | 2.1 |
| JSON Schema Event Contract (`v1`) | F13, K14 | `src/common/schemas/cpc_event.v1.json:1` | `CPC Recruitment AdTech Canonical Event v1` | 2.2 |
| Canonical Event Pydantic Model | F06, F13 | `src/common/schemas/cpc_event.py:77` | `CpcEventV1` | 2.2 |
| Pure Event Contract Validator | F13 | `src/common/schemas/cpc_event.py:231` | `validate_event` | 2.2 |
| Spark/Python Validation Parity Spec | F13 | `src/common/schemas/validation_rules.md:1` | `Validation Rules Specification` | 2.2 |
| Monetary DECIMAL(18,4) & UTC Standard | F13, R9 | `docs/adr/007-decimal-and-utc.md:49` | `ADR-007` | 2.2 |
| Spend Calculation Rule (`spend = bid`) | F13, R9 | `docs/adr/009-spend-rule.md:43` | `Quyết định chọn Phương án A` | 2.1 |
| S3 MinIO Object Store Core Service | F08, T04 | `docker-compose.yml:15` | `services.minio` | 1.2, 1.3 |
| Redpanda Message Broker Core Service | F07, T01 | `docker-compose.yml:59` | `services.redpanda` | 1.3 |
| Partition Key Skew Analyzer | F07, T05 | `scripts/analyze_partition_skew.py:73` | `analyze_candidates` | 2.3 |
| Kafka Partition Key Decision (`campaign_id`) | F07 | `docs/adr/005-partition-key.md:55` | `ADR-005` | 2.3 |
| Idempotent Redpanda Topic Provisioner | T01, F07 | `infra/redpanda/create_topics.sh:17` | `ensure_topic` | 2.4 |
| Simulation Clock & Deadline Time Mapper | F02, F01 | `src/producer/time_mapper.py:30` | `TimeMapper` | 2.5 |
| Chunked Parquet Stream Reader | F01 | `src/producer/replay_producer.py:302` | `iter_parquet_chunks` | 2.5 |
| Idempotent Kafka Replay Producer & Graceful SIGINT | T01, T09, F06 | `src/producer/replay_producer.py:642` | `run_replay` | 2.5, 2.6 |
| Seeded Fault Injector (2% Dup, 2% Late, 1% Corrupt) | F04, F06, F13 | `src/producer/fault_injector.py:149` | `FaultInjector` | 2.6 |
| Ground-Truth Manifest & Late IDs Writer | F04, F13 | `src/producer/replay_producer.py:611` | `_write_manifest_files` | 2.6 |
| Fault Rate & Determinism Test Suite (100k events) | F04, F06, F13 | `tests/unit/test_fault_injector.py:55` | `test_fault_rates_converge_on_100k_events` | 2.7 |
| Lab L2: Replay Overlap Duplicate Experiment | F06 | `docs/lessons/labs.md:7` | `Lab L2` | 2.7 |
| S3 API Conformance & Performance Suite | T04, F08 | `scripts/s3_conformance.py:76` | `S3ConformanceRunner` | 1.2 |
| Redpanda Log Retention Policy | T01, F07 | `docs/progress/1.4.md:14` | `log_retention_ms=3600000` | 1.4 |
| Disk Governance & Report Script | T08, F08 | `scripts/disk_report.sh:1` | `disk_report.sh` | 1.5 |
| WSL2 & Drive D Storage Strategy | R5, R6 | `docs/adr/008-disk-strategy.md:43` | `ADR-008` | 1.5 |
| Docker Resource Baseline Profiler | F14, R14 | `scripts/bench/resource_snapshot.sh:1` | `resource_snapshot.sh` | 1.6 |
| Resource Baseline Report | F14, R14 | `docs/benchmarks/baseline.md:12` | `Resource Baseline` | 1.6 |
| Spark + Delta + S3A Baked Image | T02, R4 | `infra/spark/Dockerfile:5` | `FROM apache/spark:3.5.3` | 3.1 |
| Deterministic Spark JAR Downloader | T02, R4 | `infra/spark/download_jars.py:37` | `jar_paths_by_group` | 3.1 |
| Spark App Container Service | T02, R5 | `docker-compose.yml:119` | `services.spark` | 3.1 |
| Spark <-> Delta <-> DuckDB Interop Test | T02, T03, T05, F10 | `tests/integration/smoke_delta_interop.py:298` | `main` | 3.1 |
| Delta Lake Table Format Decision | F10, T03 | `docs/adr/003-delta.md:49` | `ADR-003` | 3.1 |
| Centralized Pipeline & Spark YAML Config | T02, T08, R5 | `config/pipeline.yaml:11` | `streaming`, `spark`, `storage` | 3.2 |
| Spark & Checkpoint Pydantic Config + S3A URI | T02, T09 | `src/common/config.py:53` | `SparkConfig`, `AppConfig.s3a_uri` | 3.2 |
| SparkSession Factory (16GB RAM, Delta, S3A, UTC) | T02, T08, R6 | `src/streaming/spark_session.py:109` | `build_spark` | 3.2 |
| Spark Config Builder & Credential Redactor | T02, R6 | `src/streaming/spark_session.py:48` | `build_spark_conf`, `redact_spark_conf` | 3.2 |
| Hadoop S3A Bucket/Prefix Lister | T02, T04 | `src/streaming/spark_session.py:179` | `list_s3a_paths` | 3.2 |
| Local SparkSession + Delta Roundtrip Test | T02 | `tests/unit/test_spark_session.py:137` | `test_build_spark_local_delta_roundtrip` | 3.2 |
| PySpark PERMISSIVE JSON & Contract Validator | F13, T02 | `src/streaming/validation.py:132` | `validate_df` | 3.3 |
| Q1 Ingest Kafka Raw Stream Reader | T01, T02 | `src/streaming/ingest.py:31` | `read_kafka_raw_stream` | 3.3 |
| Q1 Idempotent Bronze/DLQ Batch Router | F06, T03 | `src/streaming/ingest.py:86` | `make_process_batch` | 3.3 |
| Q1 Kafka DLQ At-Least-Once Publisher | F06, F13 | `src/streaming/ingest.py:60` | `publish_to_kafka_dlq` | 3.3 |
| Q1 Streaming Query Runner & Checkpoint | T02 | `src/streaming/ingest.py:142` | `start_ingest_query` | 3.3 |
| Python <-> PySpark Validation Parity Suite | F13, T02 | `tests/unit/test_validation_parity.py:212` | `test_python_and_spark_validation_parity` | 3.3 |
| Q2 Refine Bronze Delta Stream Reader | T02, T03 | `src/streaming/refine.py:50` | `read_bronze_delta_stream` | 3.4 |
| Bronze -> Silver Type Normalizer & Silver DLQ | F12, F13 | `src/streaming/refine.py:65` | `transform_bronze_to_silver` | 3.4 |
| 2-Layer Dedup & Partition-Pruned Delta MERGE | F03, F04, F05, F06, T03 | `src/streaming/refine.py:192` | `make_upsert_silver` | 3.4 |
| Q2 Refine Watermark Streaming Runner | F03, F04, F05, T02 | `src/streaming/refine.py:260` | `start_refine_query` | 3.4 |
| Silver Idempotent MERGE & Dedup Test | F06, T03 | `tests/unit/test_refine.py:223` | `test_upsert_silver_idempotent_and_deduplicates` | 3.4 |
| DuckDB Lakehouse Snapshot Fetcher | F14, T05 | `tests/integration/reconcile.py:255` | `fetch_lakehouse_snapshot` | 3.5 |
| End-to-End Reconciliation Evaluator (a)-(e) | F13, F14 | `tests/integration/reconcile.py:130` | `evaluate_reconciliation` | 3.5 |
| Reconciliation Unit Test Suite | F14 | `tests/unit/test_reconcile.py:55` | `test_reconcile_clean_pass` | 3.5 |
| Empty Bronze Delta Initializer | T02, T03 | `src/streaming/run_all.py:33` | `ensure_bronze_table_exists` | 3.6 |
| Single-JVM Q1 + Q2 Pipeline Runner | F16, T02, R5 | `src/streaming/run_all.py:78` | `run_streaming_pipeline` | 3.6 |
| Streaming Failure Models & Recovery Guide | F16 | `docs/lessons/F16_failure_models_recovery.md:1` | `F16` | 3.6 |
| QueryProgress Parser (State, Watermark, Rates) | F14, T02 | `src/streaming/listener.py:51` | `parse_query_progress` | 3.9 |
| Live JSON & 60s Buffered Delta Metrics Listener | F14, T02, R6 | `src/streaming/listener.py:157` | `MetricsListener` | 3.9 |
| StreamingQueryListener Unit Test Suite | F14 | `tests/unit/test_listener.py:82` | `test_metrics_listener_buffers_until_interval_and_flushes_on_demand` | 3.9 |
| Watermark vs P3 MERGE-Only Experiment Runner | F04, F05 | `scripts/bench/exp_watermark.py:227` | `main` | 3.7 |
| Watermark & Late-Data Policy ADR | F04, F05 | `docs/adr/006-late-data.md:54` | `ADR-006` | 3.7 |
| Delta Compaction & OCC Conflict Retry | F10, T03 | `src/streaming/maintenance.py:150` | `compact_table`, `run_with_conflict_retry` | 3.8 |
| Safe Delta VACUUM Retention Guard (>= 1h / 168h) | F10, T03 | `src/streaming/maintenance.py:197` | `vacuum_table`, `validate_vacuum_retention` | 3.8 |
| Small Files Benchmark (DuckDB vs OPTIMIZE) | F10, T03, T05 | `scripts/bench/bench_small_files.py:132` | `main` | 3.8 |
| Maintenance Retry & Safety Unit Tests | T03 | `tests/unit/test_maintenance.py:31` | `test_run_with_conflict_retry_succeeds_after_transient_conflicts` | 3.8 |
| dbt Project Configuration | T06 | `dbt_cpc/dbt_project.yml:1` | `dbt_cpc` | 4.1 |
| dbt-duckdb Profile & S3 Secret Config (K1, R6, R7) | T05, T06, K1 | `dbt_cpc/profiles.yml:1` | `dbt_cpc.outputs.dev` | 4.1 |
| Silver Delta Source (`delta_scan` external_location) | T06, K6 | `dbt_cpc/models/staging/sources.yml:3` | `lakehouse_silver.cpc_events` | 4.1 |
| Staging CPC Events View (`event_hour`, explicit casts) | T06, F12 | `dbt_cpc/models/staging/stg_cpc_events.sql:1` | `stg_cpc_events` | 4.3 |
| Staging Generic Data Tests (`data_tests`) | T06, F13 | `dbt_cpc/models/staging/schema.yml:3` | `stg_cpc_events` | 4.3 |
| dbt Config & Contract Offline Unit Tests | T06, F13 | `tests/unit/test_dbt_config.py:14` | `test_dbt_profiles_enforces_env_vars_and_k1_endpoint_rule` | 4.1, 4.3 |
| Deterministic Dimension Seed Normalizer | T06, F12, R1 | `scripts/prepare_dbt_seeds.py:229` | `prepare_all_seeds` | 4.2 |
| Campaign Budget & Dimension Views (5 dims) | T06, F12, R13 | `dbt_cpc/models/dimensions/dim_campaign_budget.sql:1` | `dim_campaign_budget` | 4.2 |
| Dimension PK & Staging Referential Integrity Tests | T06, F13 | `dbt_cpc/models/dimensions/schema.yml:3` | `dim_campaign_budget` | 4.2 |
| Real Budget & Linear Pacing Curve ADR | F12, R9, R13 | `docs/adr/010-campaign-budget-and-pacing.md:34` | `ADR-010` | 4.2 |
| Seed Determinism & PII Exclusion Unit Tests | T06, R6 | `tests/unit/test_prepare_seeds.py:210` | `test_prepare_all_seeds_is_deterministic` | 4.2 |
| Gold Hourly Campaign Pacing (Hour Spine, P1) | F12, T06, R9 | `dbt_cpc/models/gold/fct_campaign_hourly_pacing.sql:1` | `fct_campaign_hourly_pacing` | 4.4 |
| Gold Publisher Quality & Fraud Flags R1/R2 (P2) | F12, T06, R9 | `dbt_cpc/models/gold/fct_publisher_quality.sql:1` | `fct_publisher_quality` | 4.4 |
| Gold Recruitment Efficiency CPC/CPA/CPQA (P3) | F12, T06, R9 | `dbt_cpc/models/gold/fct_recruitment_efficiency.sql:1` | `fct_recruitment_efficiency` | 4.4 |
| Gold Marts Schema & Non-Null Tests | F13, T06 | `dbt_cpc/models/gold/schema.yml:3` | `fct_campaign_hourly_pacing` | 4.4 |
| Gold Metric Catalog Section 2.3 Unit Tests | F12, R9 | `tests/unit/test_gold_metrics.py:124` | `test_fct_campaign_hourly_pacing_section_2_3_closed_and_in_progress_hours` | 4.4 |
| Singular Quality Tests (Spend, Pacing, SimRange, Funnel, Reconcile) | F13, T06 | `dbt_cpc/tests/assert_non_negative_spend.sql:1` | `assert_non_negative_spend` | 4.5 |
| Silver Source Freshness (`processed_at` 2m warn) | F13, F14, T06 | `dbt_cpc/models/staging/sources.yml:9` | `lakehouse_silver.cpc_events` | 4.5 |
| Lightweight dbt Scheduler (`flock` + `_last_run.json`) | T06, R5 | `scripts/dbt_loop.sh:1` | `run_one_cycle` | 4.6 |
| Gold `_last_run.json` MinIO Metadata Writer | F14, T04, R6 | `scripts/write_gold_last_run.py:34` | `build_last_run_payload`, `upload_last_run_metadata` | 4.6 |
| Singular & Negative (`spend < 0`) Quality Unit Tests | F13 | `tests/unit/test_dbt_quality_tests.py:114` | `test_assert_non_negative_spend_passes_clean_and_fails_on_negative_spend` | 4.5, 4.6 |
| Zero-Copy DuckDB Serving Architecture ADR | F11, F15 | `docs/adr/004-serving.md:45` | `ADR-004` | 5.1 |
| Dashboard DuckDB `:memory:` Root Connection & K1/R6 | F11, T05, T07 | `src/dashboard/db.py:91` | `create_root_connection`, `get_root_connection` | 5.1 |
| Thread-Safe Per-Query Cursor & Retry Runner | F11, F15, F16 | `src/dashboard/db.py:157` | `run_query`, `DatabaseConnectionError` | 5.1 |
| Dashboard DB Concurrency, Delta & Secret Redaction Tests | F11, R6 | `tests/unit/test_dashboard_db.py:55` | `test_run_query_five_concurrent_threads_no_lock_or_race` | 5.1 |
| Dashboard Parameterized SQL Queries (5 panels) | T05, R9 | `src/dashboard/queries/pacing.sql:1` | `pacing`, `sim_now`, `publisher_quality`, `funnel`, `health` | 5.2 |
| Dashboard & Alerting Query Repository | T05, T07, R9 | `src/dashboard/repository.py:98` | `DashboardRepository`, `DataSourcePaths` | 5.2 |
| Query Layer Unit Tests (Section 2.3 & DLQ) | F13, R9 | `tests/unit/test_dashboard_queries.py:217` | `test_all_five_dashboard_queries_against_section_2_3_examples` | 5.2 |
| Fast Path vs Deep Path Metric Parity Test (`<= 1e-6`) | F12, R9 | `tests/integration/test_metric_parity.py:72` | `test_fast_path_and_deep_path_metric_parity` | 4.7 |
| Shared Panel Helpers & `@st.cache_data(ttl=5)` | T07, R9 | `src/dashboard/panels/common.py:21` | `format_metric_or_na`, `fetch_pacing`, `sanitize_dlq_payload` | 5.3-5.7 |
| Panel 1: Budget Pacing Monitor (`campaign.csv`) | T07, R9, R13 | `src/dashboard/panels/pacing.py:22` | `render_pacing_panel` | 5.3 |
| Panel 2: Publisher Fraud & Quality Radar | T07, R9 | `src/dashboard/panels/publisher_quality.py:20` | `render_publisher_quality_panel` | 5.4 |
| Panel 3: Hiring Funnel & CPA (Gold Parquet) | T07, R9 | `src/dashboard/panels/funnel.py:42` | `render_funnel_panel` | 5.5 |
| Panel 4: Pipeline Health & DLQ Inspector | T07, F14 | `src/dashboard/panels/health.py:24` | `render_health_panel` | 5.6 |
| Streamlit App (`@st.fragment(run_every="10s")`) | T07, F15 | `src/dashboard/app.py:34` | `_pacing_fragment`, `main` | 5.7 |
| Dashboard Panel Helpers & S3 Cache Unit Tests | T07, R9 | `tests/unit/test_dashboard_panels.py:22` | `test_format_metric_and_percent_return_na_for_missing_or_infinite` | 5.3-5.7 |
