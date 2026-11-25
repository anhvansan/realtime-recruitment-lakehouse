# ADR-003 — Chọn Delta Lake làm Table Format cho Lakehouse

- **Trạng thái:** Accepted
- **Ngày:** 2026-11-25
- **Tác giả:** Chủ dự án & AI Assistant
- **Liên quan:** Task 3.1, R4 (Pin phiên bản), K2 & K7 (Rủi ro tương thích Spark ↔ Delta ↔ DuckDB)

---

## 1. Bối cảnh

Hệ thống CPC AdTech Lakehouse chạy trên MinIO (S3-compatible Object Storage) theo kiến trúc Medallion (Bronze ➔ Silver ➔ Gold) với hai yêu cầu cốt lõi:
1. **Ghi streaming ACID và khử trùng (Idempotent Write & MERGE):** Spark Structured Streaming ghi micro-batch liên tục (mỗi 10 giây) vào Bronze (dùng `txnAppId`/`txnVersion`) và thực hiện `MERGE` insert-if-not-matched vào Silver để đảm bảo mỗi `event_id` chỉ xuất hiện đúng 1 lần, không nhân đôi `spend`.
2. **Đọc đồng thời không khoá (Zero-Copy Snapshot Isolation):** DuckDB (Fast Path cho Dashboard Streamlit và dbt-duckdb) phải đọc trực tiếp các bảng trên MinIO trong lúc Spark vẫn đang ghi liên tục mà không gặp lỗi đọc trạng thái nửa vời (torn reads) hay `FileNotFound`.
3. **Ràng buộc phần cứng local (16GB RAM, Docker/WSL2):** Cần triển khai gọn nhẹ, không dựng thêm một dịch vụ Metastore/Catalog server nặng (như Hive Metastore, Nessie hay Polaris) tốn thêm hàng trăm MB RAM.

---

## 2. Các phương án

### Phương án A: Delta Lake (`delta-spark==3.2.1` trên Spark `3.5.3`)
- Lưu toàn bộ metadata giao dịch ngay trong thư mục `_delta_log/` trên MinIO (JSON commit logs + Parquet checkpoints), không bắt buộc phải có Catalog server riêng.
- *Ưu điểm:*
  - Tích hợp sâu và trưởng thành nhất với PySpark Structured Streaming (`txnAppId`/`txnVersion` idempotent writes, `DeltaTable.merge()`, `optimize()`, `vacuum()`).
  - Không cần dựng thêm container Metastore/Catalog ➔ tiết kiệm RAM quý giá trong ngân sách < 4.5GB.
  - DuckDB `2.0.0` có extension `delta` chính thức xây trên **Delta Kernel**, đọc trực tiếp `delta_scan('s3://...')` kèm hỗ trợ pushdown predicate và Deletion Vectors.
- *Nhược điểm:*
  - Các tính năng bảng mới (Table Features như Deletion Vectors, Column Mapping) nâng protocol version (Reader v3 / Writer v7), đòi hỏi reader (DuckDB) phải đủ mới mới đọc được.
  - Micro-batch tạo nhiều file nhỏ (small files), đòi hỏi cơ chế compaction (`OPTIMIZE`) định kỳ có kỷ luật (Task 3.8).

### Phương án B: Apache Iceberg
- Table format đa-engine phổ biến trong hệ sinh thái lớn (Spark, Trino, Flink, Snowflake).
- *Ưu điểm:*
  - Hỗ trợ Hidden Partitioning và Schema Evolution mạnh mẽ; trung lập về nhà cung cấp.
- *Nhược điểm:*
  - Để đảm bảo atomic commit an toàn trên S3/MinIO (không dùng HadoopCatalog vốn bị khuyến cáo không an toàn cho production/concurrent writers), Iceberg yêu cầu thêm một **REST Catalog / JDBC Catalog / Nessie** ➔ tăng thêm container, phức tạp hoá hạ tầng local và tốn thêm RAM.
  - Extension `iceberg` trên DuckDB lịch sử hỗ trợ ghi/table features chậm hơn khi không qua REST Catalog.

### Phương án C: Apache Hudi
- Tối ưu cho workload CDC / upsert mật độ cao (Merge-on-Read).
- *Ưu điểm:*
  - Đánh chỉ mục (indexing) bản ghi tốt cho bài toán update dày đặc.
- *Nhược điểm:*
  - Kiến trúc cấu hình phức tạp hơn cho phạm vi dự án học tập; hệ sinh thái đọc trực tiếp không qua Spark (như DuckDB native extension) yếu hơn rõ rệt so với Delta Lake và Iceberg.

---

## 3. Bằng chứng

- Ma trận phiên bản đã chốt và kiểm chứng trong [docs/versions.md](file:///d:/realtime-recruitment-lakehouse/docs/versions.md): `apache/spark:3.5.3-scala2.12-java17-python3-ubuntu` + `delta-spark==3.2.1` + `hadoop-aws:3.3.4` + `aws-java-sdk-bundle:1.12.262` + `duckdb==1.5.6` (container) / `2.0.0` (Windows host).
- Kết quả chạy thực tế `make smoke-interop` ([tests/integration/smoke_delta_interop.py](file:///d:/realtime-recruitment-lakehouse/tests/integration/smoke_delta_interop.py), chi tiết tại [docs/progress/3.1.md](file:///d:/realtime-recruitment-lakehouse/docs/progress/3.1.md)) đạt **4/4 hạng mục PASS trong 91.64s**:
  - **Test A:** 3 commit (`append`, `append`, `merge`) hợp lệ trong `_delta_log` (`versions=[0, 1, 2]`).
  - **Test B:** DuckDB `delta_scan` đọc chính xác 6 dòng và `SUM(spend)=23.0000` (`DECIMAL(18,4)`).
  - **Test C:** 20 commit Spark liên tiếp song song với **108 lượt đọc liên tục từ DuckDB** — 0 lỗi, 0 trạng thái nửa vời (`final rows=106`).
  - **Test D:** DuckDB `1.5.6` đọc bản địa bảng Delta có bật **Deletion Vectors** (`minReaderVersion=3, minWriterVersion=7, tableFeatures=['appendOnly', 'deletionVectors', 'invariants']`).

---

## 4. Quyết định

**Chọn Phương án A — Delta Lake `3.2.1`** làm table format thống nhất cho toàn bộ các tầng dữ liệu Lakehouse (Bronze, Silver, DLQ) trên MinIO.

---

## 5. Hệ quả

- **Được:**
  - Hạ tầng tối giản (không cần dựng Hive Metastore / Nessie / REST Catalog), giữ tổng RAM hệ thống gọn trong ngân sách 4.5GB.
  - Có sẵn cơ chế ghi idempotent (`txnAppId`/`txnVersion`) và `MERGE` nguyên tử cho tầng Bronze/Silver.
  - DuckDB đọc trực tiếp zero-copy qua `delta_scan()` với Snapshot Isolation đầy đủ.
- **Mất / Đánh đổi (Giới hạn của DuckDB `2.0.0` + `delta` extension đã pin):**
  - DuckDB chỉ đóng vai trò **Reader** đối với bảng Delta trên MinIO; mọi thao tác `MERGE`, `UPDATE`, `DELETE`, `OPTIMIZE`, `VACUUM` bắt buộc do Spark đảm nhiệm.
  - Phải kiểm soát chặt Table Features (ví dụ kiểm chứng Deletion Vectors ở Task 3.1) và không bao giờ chạy `VACUUM(0)` khi DuckDB đang đọc (Task 3.8).

---

## 6. Điều kiện xem xét lại

Quyết định này cần được xem xét lại (chuyển sang Apache Iceberg + REST Catalog) khi:
1. Hệ thống mở rộng thành đa-writer từ nhiều engine khác nhau (ví dụ Flink + Spark + Trino cùng ghi vào một bảng) đòi hỏi một Catalog tập trung chuẩn mở.
2. Cần tính năng Hidden Partitioning / Partition Evolution mà không muốn viết lại dữ liệu lịch sử.
