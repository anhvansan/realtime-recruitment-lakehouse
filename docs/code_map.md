# Code Map — Tra cứu Khái niệm sang Mã nguồn

Bảng ánh xạ trực tiếp từ các khái niệm lý thuyết / kiến trúc sang vị trí cài đặt cụ thể trong mã nguồn theo quy tắc **Phụ lục A.2**.

| Khái niệm | Module | File:dòng | Symbol | Cập nhật ở task |
| :--- | :--- | :--- | :--- | :--- |
| S3 Decoupling & DuckDB Host:Port | T09, K1 | `src/common/config.py:127` | `AppConfig.s3_endpoint_host_port` | 1.1 |
| Configuration Loader | T09 | `src/common/config.py:152` | `load` | 1.1 |
| CSV Inventory & Triage Profiler | T05, F12 | `scripts/profile_tracking.py:27` | `run_phase_1` | 2.1 |
| Deep Hypotheses Verification (U1-U9, H1, H4) | T05, F13 | `scripts/profile_tracking.py:114` | `run_phase_2` | 2.1 |
| Canonical Event & Funnel Mapping Contract | F13 | `docs/data_dictionary.md:73` | `Event Type Mapping` | 2.1 |
| Spend Calculation Rule (`spend = bid`) | F13, R9 | `docs/adr/009-spend-rule.md:43` | `Quyết định chọn Phương án A` | 2.1 |
| S3 MinIO Object Store Core Service | F08, T04 | `docker-compose.yml:15` | `services.minio` | 1.2, 1.3 |
| Redpanda Message Broker Core Service | F07, T01 | `docker-compose.yml:59` | `services.redpanda` | 1.3 |
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
