# Version Registry & Compatibility Matrix

Tài liệu này lưu trữ toàn bộ các phiên bản phần mềm, Docker images, thư viện được "pin" chính thức trong dự án theo quy tắc **R4 — Pin phiên bản** và **R10 — Tài liệu đúng phiên bản thắng backlog**.

## 1. Thành phần phần mềm & Thư viện

| Thành phần | Phiên bản đã pin | Ngày chốt | Ghi chú / Mục đích |
| :--- | :--- | :--- | :--- |
| Python | 3.13.11 | 2026-11-19 | Runtime Python trên máy host |
| Ruff | 0.17.5 | 2026-11-19 | Linter & Formatter cho toàn bộ code Python |
| Pytest | 9.1.1 | 2026-11-19 | Test framework |
| Pydantic | 2.12.4 | 2026-11-19 | Data validation & settings management |
| Pydantic-Settings | 2.12.0 | 2026-11-19 | Nạp biến môi trường .env |
| PyYAML | 6.0.3 | 2026-11-19 | Parser cho config/*.yaml |
| GNU Make | 4.1 | 2026-11-19 | Build tool & workflow orchestration |
| MinIO Server | *Chốt ở Task 1.2* | - | S3-compatible Object Storage |
| MinIO MC Client | *Chốt ở Task 1.2* | - | CLI khởi tạo bucket cho MinIO |
| Redpanda | *Chốt ở Task 1.3* | - | Kafka-compatible streaming broker |
| Redpanda Console | *Chốt ở Task 1.3* | - | Web UI giám sát Kafka topics & messages |
| Apache Spark | *Chốt ở Task 3.1* | - | Structured Streaming engine |
| Delta Lake | *Chốt ở Task 3.1* | - | ACID Lakehouse table format |
| DuckDB | *Chốt ở Task 3.1* | - | In-memory OLAP & Delta Lake direct query engine |
| dbt-duckdb | *Chốt ở Task 4.1* | - | Data build tool for Gold marts |
| Streamlit | *Chốt ở Task 5.1* | - | Dashboard UI |

## 2. Ma trận tương thích (Compatibility Matrix)

Ma trận xác nhận tính liên thông giữa các thành phần chính (sẽ được kiểm chứng chi tiết tại Task 3.1):

| Spark Engine | Scala Version | Delta Lake | hadoop-aws | DuckDB | MinIO | Trạng thái xác nhận |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| *TBD* | *TBD* | *TBD* | *TBD* | *TBD* | *TBD* | Chưa kiểm chứng (chờ Task 3.1) |

## 3. Các điểm lệch so với Backlog (Deviations)

Ghi nhận các điểm điều chỉnh kỹ thuật khi đối chiếu thực tế với tài liệu `PROJECT_BACKLOG.md`:

| Ngày | Task | Điểm lệch so với Backlog | Lý do & Giải pháp thay thế |
| :--- | :--- | :--- | :--- |
| 2026-11-19 | 1.1 | Sử dụng GNU Make 4.1 (Msys2 / Conda) trên Windows | Môi trường Windows host cần make để chạy các target chuẩn Makefile; các lệnh trong Makefile được thiết kế tương thích cả Windows shell và Linux/WSL2. |
