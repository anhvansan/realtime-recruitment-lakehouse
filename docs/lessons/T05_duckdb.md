# T05 — DuckDB: Công cụ Phân tích Nhúng Siêu tốc và Cầu nối Lakehouse

## 1. Câu hỏi gốc
Làm sao để thực hiện các truy vấn phân tích phức tạp (Aggregation, Window Functions, Joins) trên các tệp dữ liệu lớn (CSV, Parquet, Delta Lake) trực tiếp trên máy tính cá nhân hoặc ứng dụng Web (Streamlit) với tốc độ tức thì (sub-second) mà không cần cài đặt và vận hành một hệ thống Data Warehouse cồng kềnh (như Snowflake, BigQuery hay Postgres)?

## 2. Trực giác
Nếu như SQLite được mệnh danh là "con dao bấm Thụy Sĩ" cho các ứng dụng lưu trữ giao dịch hàng ngày (OLTP) trong điện thoại và trình duyệt, thì DuckDB chính là một "động cơ phản lực bỏ túi" chuyên dụng cho các tác vụ phân tích dữ liệu lớn (OLAP). Bạn không cần bật server, không cần tạo database; chỉ cần trỏ DuckDB vào bất kỳ tệp CSV, Parquet hay thư mục Delta Lake nào, nó sẽ đọc và tính toán tức thì với sức mạnh tận dụng tối đa đa nhân CPU và vector hóa bộ nhớ.

## 3. Cơ chế
Kiến trúc bên trong của DuckDB:

```
[SQL Query / Python DataFrame]
              │
              ▼
    [Hyper-optimized Parser & Cost-based Optimizer]
              │
              ▼
    [Vectorized Execution Engine] ──► Xử lý dữ liệu theo từng khối mảng (vector 2048 phần tử)
              │
    ┌─────────┴──────────────────────────────────┐
    │ Extensions Hub                             │
    │  ├── httpfs: Đọc ghi trực tiếp từ S3/MinIO │
    │  └── delta: Đọc Delta Log và Parquet files │
    └────────────────────────────────────────────┘
              │
              ▼
[Direct File Scan: CSV / Parquet / Delta / S3 / :memory:]
```

**Các cơ chế nổi bật:**
1. **Zero-Copy In-Process Querying:** DuckDB chạy cùng tiến trình bộ nhớ với Python (hoặc Streamlit), trao đổi dữ liệu với Arrow/Pandas/Polars mà không tốn chi phí sao chép qua mạng (IPC).
2. **Pushdown Predicate & Column Pruning:** Khi đọc Delta Lake/Parquet trên S3 qua `delta_scan()`, DuckDB chỉ tải đúng các cột và các partition cần thiết, bỏ qua các khối dữ liệu không khớp điều kiện `WHERE`.
3. **Cơ chế Secret Manager:** Quản lý credential S3/MinIO bảo mật, tách biệt với logic truy vấn.

## 4. Ví dụ số
- Khảo sát 18 file CSV thô với tổng cộng hàng trăm nghìn dòng dữ liệu:
  - DuckDB chỉ mất **0.32 giây** để quét toàn bộ 18 file, tính toán tỷ lệ null, thống kê min/max thời gian và phân tích phân phối giá thầu `bid`.
  - Không cần nạp toàn bộ file vào RAM: DuckDB tự động streaming và đọc song song trên nhiều lõi CPU.

## 5. Điểm mạnh / Điểm yếu / Khi nào không dùng
- **Điểm mạnh:** Cực kỳ nhanh cho phân tích; cú pháp SQL mở rộng hiện đại (`GROUP BY ALL`, `FILTER`, `COLUMNS(*)`); nạp extension động; không tốn chi phí hạ tầng.
- **Điểm yếu:** Không hỗ trợ ghi đồng thời từ nhiều tiến trình độc lập vào cùng một file `.duckdb` (Single-writer constraint).
- **Khi nào không dùng:** Làm cơ sở dữ liệu giao dịch với hàng triệu lệnh `INSERT`/`UPDATE` từng dòng một từ hàng nghìn người dùng đồng thời (dùng PostgreSQL / MySQL).

## 6. Hiểu lầm phổ biến
1. *Hiểu lầm:* DuckDB chỉ đọc được các bảng đã được nạp (import) vào cơ sở dữ liệu nội bộ của nó.  
   *Sự thật:* DuckDB có thể chạy SQL trực tiếp trên các đường dẫn tệp ngoài: `SELECT * FROM read_csv_auto('path/*.csv')` hoặc `SELECT * FROM delta_scan('s3://...')`.
2. *Hiểu lầm:* S3 endpoint truyền vào secret của DuckDB phải có tiền tố `http://` như các client khác.  
   *Sự thật:* DuckDB yêu cầu trường `ENDPOINT` chỉ chứa `host:port` (ví dụ `minio:9000`), kèm theo cờ `USE_SSL false`.

## 7. Trong repo này

| File:dòng | Symbol | Đoạn code | Giải thích dòng nào quan trọng |
| :--- | :--- | :--- | :--- |
| `scripts/profile_tracking.py:46-52` | `read_csv_auto` | Đọc CSV trực tiếp | DuckDB tự động suy diễn kiểu dữ liệu và đọc trực tiếp file CSV mà không cần định nghĩa bảng trước. |
| `tests/integration/smoke_delta_interop.py:89-118` | `build_duckdb_connection` | `LOAD httpfs`, `LOAD delta`, `CREATE SECRET` | Dùng `cfg.s3_endpoint_host_port` (không có `http://`), `URL_STYLE 'path'`, giới hạn `memory_limit='256MB'`, không log secret (R6). |
| `tests/integration/smoke_delta_interop.py:170-190` | `run_test_b` | `delta_scan('s3://...')` | Quét trực tiếp bảng Delta do Spark ghi trên MinIO và kiểm chứng chính xác kiểu `DECIMAL(18,4)`. |
| `src/dashboard/db.py:91-199` | `create_root_connection`, `run_query` | `:memory:`, `cursor()`, retry backoff | Lớp kết nối phục vụ đa luồng cho Dashboard (`memory_limit='256MB'`, không log secret, bọc `DatabaseConnectionError`). |

```python
            # Row count
            row_count = con.execute(f"SELECT count(*) FROM read_csv_auto('{safe_path}')").fetchone()[0]

            # Schema
            desc = con.execute(f"DESCRIBE SELECT * FROM read_csv_auto('{safe_path}')").fetchall()
            cols_info = [(r[0], r[1]) for r in desc]
```

## 8. Tự kiểm tra
1. *Câu hỏi:* Tại sao trong dự án này DuckDB được chọn làm engine phục vụ cho Đường nhanh (Fast Path) của Dashboard Streamlit?  
   *Đáp án:* Vì DuckDB chạy nhúng ngay trong tiến trình Streamlit, có thể gọi `delta_scan()` trực tiếp trên MinIO với độ trễ dưới 1 giây mà không cần dựng thêm cụm server OLAP.
2. *Câu hỏi:* Để giới hạn lượng RAM DuckDB được phép sử dụng nhằm tuân thủ quy tắc kỷ luật tài nguyên (R5), ta dùng câu lệnh nào?  
   *Đáp án:* `SET memory_limit = '256MB';`.
3. *Câu hỏi:* Trong bài toán profiling Task 2.1, tính năng nào của DuckDB giúp kiểm tra phân phối 24 giờ một cách ngắn gọn?  
   *Đáp án:* Hàm trích xuất thời gian `EXTRACT(hour FROM ts)` kết hợp `GROUP BY 1 ORDER BY 1`.

## 9. Đọc thêm
- DuckDB Official Documentation: https://duckdb.org/docs/
- DuckDB S3 & Delta Lake Integration: https://duckdb.org/docs/extensions/httpfs/s3_api.html
