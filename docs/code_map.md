# Code Map — Tra cứu Khái niệm sang Mã nguồn

Bảng ánh xạ trực tiếp từ các khái niệm lý thuyết / kiến trúc sang vị trí cài đặt cụ thể trong mã nguồn theo quy tắc **Phụ lục A.2**.

| Khái niệm | Module | File:dòng | Symbol | Cập nhật ở task |
| :--- | :--- | :--- | :--- | :--- |
| S3 Decoupling & DuckDB Host:Port | T09, K1 | `src/common/config.py:127` | `AppConfig.s3_endpoint_host_port` | 1.1 |
| Configuration Loader | T09 | `src/common/config.py:152` | `load` | 1.1 |
