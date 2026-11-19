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
