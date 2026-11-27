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
