# ADR-001 — Sử dụng MinIO làm S3-Compatible Object Store với Cấu hình Tách rời (Decoupling)

- **Trạng thái:** Accepted
- **Ngày:** 2026-11-19
- **Tác giả:** Chủ dự án & AI Agent

## 1. Bối cảnh
Hệ thống Near Real-Time Lakehouse yêu cầu một tầng lưu trữ đối tượng tương thích chuẩn AWS S3 API để lưu trữ các bảng Delta Lake (Bronze, Silver, DLQ) và Gold Data Marts (Parquet). Môi trường triển khai là máy tính cá nhân 16GB RAM chạy WSL2/Ubuntu. Cần một giải pháp lưu trữ nhẹ, ổn định, hỗ trợ đầy đủ các tính năng S3 cốt lõi (Path-style, Multipart Upload, ListObjectsV2) và có giao diện quan sát trực quan mà không phụ thuộc vào hạ tầng đám mây công cộng đắt đỏ.

## 2. Các phương án
- **Phương án A (Khuyến nghị): MinIO (Drop-in open-source image `pgsty/minio:RELEASE.2026-08-04T00-00-00Z`)**
  - *Ưu điểm:* Là chuẩn mực S3-compatible phổ biến nhất; tương thích 100% với hadoop-aws (Spark), DuckDB `httpfs/delta` và boto3; có sẵn MinIO Web Console (cổng 9001) giúp quan sát trực quan cây thư mục bucket và file Delta; mức tiêu thụ RAM tĩnh cực thấp (< 200MB, ngân sách cấp 448MB). Sử dụng image drop-in công khai từ cộng đồng `pgsty` sau khi upstream MinIO hạn chế pull ẩn danh trên Docker Hub.
- **Phương án B: SeaweedFS / Garage**
  - *Ưu điểm:* Tiêu thụ RAM siêu nhẹ (< 100MB), hỗ trợ phân tán tốt.
  - *Nhược điểm:* Web UI quan sát hạn chế; một số ngữ nghĩa S3 nâng cao (etag multipart upload, header tương thích) đôi khi phát sinh lỗi lệch chuẩn với các thư viện Spark cũ.
- **Phương án C: Local Filesystem (POSIX Path thay vì S3 API)**
  - *Ưu điểm:* Không cần chạy container lưu trữ.
  - *Nhược điểm:* Đánh mất tính chân thực của kiến trúc Cloud Lakehouse; cú pháp kết nối của DuckDB, dbt và Spark sẽ khác hoàn toàn so với khi lên Production; không kiểm thử được cạm bẫy S3 endpoint.

## 3. Bằng chứng
1. Ngân sách tài nguyên (Mục 4.4): MinIO được cấp phát giới hạn `mem_limit: 448m`, thực tế chạy ổn định ở mức ~150MB RAM.
2. Bộ kiểm chuẩn `scripts/s3_conformance.py` kiểm tra toàn bộ 6 tiêu chí kỹ thuật: Path-style addressing, CRUD, ListObjectsV2 phân trang, Multipart upload 32MB, Overwrite và Concurrent writes.

## 4. Quyết định
**Chọn Phương án A (MinIO)** cho môi trường local, đồng thời thực thi nghiêm ngặt **nguyên lý Decoupling qua biến môi trường S3**:
- Mọi thành phần (Spark, DuckDB, dbt, Boto3) **tuyệt đối không hard-code** URL hay credential của MinIO.
- Toàn bộ kết nối được cấu hình thông qua các biến môi trường chuẩn S3: `S3_ENDPOINT`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`, `S3_BUCKET`, `S3_REGION`.
- Lớp cấu hình `src/common/config.py` tự động xử lý tách scheme (`s3_endpoint_host_port`) để phục vụ riêng cho DuckDB Secret.
- Nhờ thiết kế tách rời này, hệ thống có thể chuyển đổi sang AWS S3 thật, Cloudflare R2, hoặc SeaweedFS bất kỳ lúc nào chỉ bằng việc thay đổi file `.env` mà không phải sửa một dòng mã nguồn nào.

## 5. Hệ quả
- *Được:*
  - Có môi trường Object Store cục bộ hoàn chỉnh, chạy offline không tốn chi phí.
  - Có Web UI (cổng 9001) để thanh tra trực tiếp các file Parquet và thư mục `_delta_log`.
  - Đảm bảo mã nguồn sẵn sàng 100% cho Cloud Production.
- *Mất / Đánh đổi:*
  - Cần khởi chạy một container Docker nền (`minio`) chiếm ~200MB RAM.

## 6. Điều kiện xem xét lại
Quyết định này được xem xét lại khi dự án chuyển lên môi trường Cloud Production (sẽ chuyển `S3_ENDPOINT` sang AWS S3 hoặc Google Cloud Storage via S3 API interoperability).
