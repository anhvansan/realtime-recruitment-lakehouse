# Version Registry & Compatibility Matrix

Tài liệu này lưu trữ toàn bộ các phiên bản phần mềm, Docker images, thư viện được "pin" chính thức trong dự án theo quy tắc **R4 — Pin phiên bản** và **R10 — Tài liệu đúng phiên bản thắng backlog**.

## 1. Thành phần phần mềm & Thư viện

| Thành phần | Phiên bản đã pin | Ngày chốt | Ghi chú / Mục đích |
| :--- | :--- | :--- | :--- |
| Python | 3.13.11 | 2026-11-19 | Runtime Python trên máy host / WSL2 |
| Ruff | 0.17.5 | 2026-11-19 | Linter & Formatter cho toàn bộ code Python |
| Pytest | 9.1.1 | 2026-11-19 | Test framework |
| Pydantic | 2.12.4 | 2026-11-19 | Data validation & settings management |
| Pydantic-Settings | 2.12.0 | 2026-11-19 | Nạp biến môi trường .env |
| PyYAML | 6.0.3 | 2026-11-19 | Parser cho config/*.yaml |
| GNU Make | 4.1 | 2026-11-19 | Build tool & workflow orchestration |
| DuckDB | 2.0.0 | 2026-11-19 | In-memory OLAP & Delta direct scan engine |
| Boto3 | 1.44.6 | 2026-11-19 | AWS SDK Python kiểm tra S3 API |
| MinIO Server | `pgsty/minio:RELEASE.2026-08-04T00-00-00Z` | 2026-11-19 | S3-compatible Object Storage (Task 1.2, 1.3) |
| MinIO MC Client | `pgsty/mc:RELEASE.2026-10-26T00-00-00Z` | 2026-11-19 | CLI khởi tạo bucket `lakehouse` (Task 1.3) |
| Redpanda | `redpandadata/redpanda:v24.2.20` | 2026-11-19 | Kafka-compatible streaming broker (Task 1.3) |
| Redpanda Console | `redpandadata/console:v2.7.2` | 2026-11-19 | Web UI giám sát Kafka topics & messages |
| Apache Spark | *Chốt ở Task 3.1* | - | Structured Streaming engine |
| Delta Lake | *Chốt ở Task 3.1* | - | ACID Lakehouse table format |
| dbt-duckdb | *Chốt ở Task 4.1* | - | Data build tool for Gold marts |
| Streamlit | *Chốt ở Task 5.1* | - | Dashboard UI |

## 2. Ma trận tương thích (Compatibility Matrix)

Ma trận xác nhận tính liên thông giữa các thành phần chính (sẽ được kiểm chứng chi tiết tại Task 3.1):

| Spark Engine | Scala Version | Delta Lake | hadoop-aws | DuckDB | MinIO | Trạng thái xác nhận |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| *TBD* | *TBD* | *TBD* | *TBD* | 2.0.0 | RELEASE.2026-08-04 | Chờ hoàn tất Task 3.1 |

## 3. Các điểm lệch so với Backlog (Deviations)

Ghi nhận các điểm điều chỉnh kỹ thuật khi đối chiếu thực tế với tài liệu `PROJECT_BACKLOG.md`:

| Ngày | Task | Điểm lệch so với Backlog | Lý do & Giải pháp thay thế |
| :--- | :--- | :--- | :--- |
| 2026-11-19 | 1.1 | Sử dụng GNU Make 4.1 (Msys2 / Conda) trên Windows | Môi trường Windows host cần make để chạy các target chuẩn Makefile; các lệnh trong Makefile được thiết kế tương thích cả Windows shell và Linux/WSL2. |
| 2026-11-19 | 1.2/1.3 | DATA_ROOT mặc định trỏ `/mnt/d/docker_data/cpc_lakehouse` | Môi trường chạy Docker Engine trực tiếp trong Ubuntu WSL2 truy cập ổ D qua đường dẫn `/mnt/d`. |
| 2026-11-19 | 1.2/1.3 | Dùng image `pgsty/minio` & `pgsty/mc` thay thế `minio/*` | MinIO upstream đã đóng quyền pull ẩn danh cho repo `minio/minio` và `minio/mc` trên Docker Hub (`pull access denied`). Image `pgsty/minio` và `pgsty/mc` là bản phân phối drop-in 1-1 từ mã nguồn mở MinIO chính thức, giữ nguyên các tham số và S3 API. |
| 2026-11-19 | 1.3 | Bỏ thuộc tính `version: "3.8"` trong `docker-compose.yml` | Docker Compose v2 cảnh báo thuộc tính `version` đã lỗi thời (obsolete). |
| 2026-11-19 | 2.1 | Dùng ngân sách thật từ `campaign.csv`, bỏ `impressions`/`CTR` | Dữ liệu gốc đã có sẵn `budget_total` thật; không có log impressions chuẩn. |
