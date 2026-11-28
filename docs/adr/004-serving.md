# ADR-004 — Kiến trúc Serving Zero-Copy (DuckDB Nhúng `:memory:` trên MinIO vs OLAP Server)

- **Trạng thái:** Accepted
- **Ngày:** 2026-11-28
- **Tác giả:** Chủ dự án & AI Agent

## 1. Bối cảnh
Dashboard Streamlit (Epic 5) cần phục vụ đồng thời hai luồng truy vấn:
1. **Đường nhanh (Fast Path — P1, P2, P4):** Đọc trực tiếp bảng Silver Delta trên MinIO mỗi ~10 giây với SLO độ trễ truy vấn `p95 <= 1.5s`.
2. **Đường sâu (Deep Path — P3):** Đọc 3 file Gold Parquet (`fct_*.parquet`) do dbt xuất lên MinIO.

Hệ thống chạy trên máy cá nhân **16 GB RAM** (ngân sách container mục 4.4 là `<= 4.5 GB`), nên mọi quyết định về tầng serving phải cân bằng giữa độ trễ, tính nhất quán và chi phí RAM/vận hành.

## 2. Các phương án
- **Phương án A (Khuyến nghị): Pull / Zero-Copy Serving bằng DuckDB `:memory:` nhúng trong Streamlit**  
  Dùng một root connection DuckDB `:memory:` duy nhất cache qua `@st.cache_resource` (`memory_limit='256MB'`, `threads=2`, `TimeZone='UTC'`), mở `cursor()` riêng cho mỗi truy vấn, nạp `httpfs` + `delta` để đọc thẳng Silver Delta (`delta_scan`) và Gold Parquet từ MinIO; kết hợp `st.cache_data(ttl=5)` để chia sẻ kết quả giữa nhiều tab trình duyệt.
  - *Ưu điểm:* Không tốn RAM dựng thêm server OLAP riêng; không sao chép dữ liệu (Zero-Copy); không bao giờ bị lỗi single-writer lock như tệp `.duckdb` bền; hưởng trọn *snapshot isolation* của Delta Lake khi Spark đang ghi đồng thời.
  - *Nhược điểm:* Mỗi lần cache hết TTL, DuckDB phải quét trực tiếp MinIO (chịu ảnh hưởng nếu nhiều file nhỏ chưa nén hoặc lượng người dùng đồng thời tăng cao).
- **Phương án B: Push / Dedicated OLAP Server (ClickHouse + Materialized View)**  
  Chạy thêm container ClickHouse đọc từ Redpanda hoặc nạp định kỳ, tạo Materialized View theo phút cho Dashboard truy vấn.
  - *Ưu điểm:* Độ trễ rất thấp và ổn định khi có hàng chục đến hàng trăm người dùng đồng thời; hỗ trợ tổng hợp liên tục khi ghi.
  - *Nhược điểm:* Tốn thêm đáng kể RAM (vượt ngân sách 4.5 GB của máy local), tăng độ phức tạp vận hành và xuất hiện thêm bản sao dữ liệu ngoài Lakehouse.
- **Phương án C: File `.duckdb` bền làm trung gian giữa Writer và Streamlit**  
  Một tiến trình nền cập nhật file `.duckdb` trên đĩa, Streamlit mở cùng file đó để đọc.
  - *Ưu điểm:* Giảm chi phí quét S3 lặp lại.
  - *Nhược điểm:* Vi phạm giới hạn **single-writer lock** của DuckDB (sẽ gây lỗi khóa file ngay khi vừa ghi vừa đọc ở Lab L6).

## 3. Bằng chứng
Các số đo thực tế đã thu thập trong dự án:
1. **Hiệu năng quét trực tiếp (`docs/benchmarks/small_files.md`):** Sau khi chạy `OPTIMIZE`, DuckDB đọc bảng Delta trên MinIO đạt median **`54.45 ms`** (`p95 = 67.31 ms`), thấp hơn rất nhiều so với ngưỡng SLO `1,500 ms` (`1.5s`).
2. **Ngân sách RAM (`docs/progress/3.8.md`):** Toàn bộ stack hiện tại (`core` + `spark`) chỉ tiêu thụ **`~721.3 MiB`** RAM; giữ serving ở dạng nhúng `:memory:` với `memory_limit='256MB'` giúp Dashboard nằm gọn trong ngưỡng dưới `4.5 GB`.
3. **Tính cô lập đồng thời (`docs/progress/3.1.md`):** Bài kiểm thử `make smoke-interop` xác nhận DuckDB đọc liên tục trong lúc Spark thực hiện 20 commit Delta nối tiếp không xảy ra lỗi hay đọc dữ liệu rách (torn read).

## 4. Quyết định
**Chọn Phương án A (Pull / Zero-Copy Serving với DuckDB `:memory:`):**
1. Hiện thực lớp kết nối tại `src/dashboard/db.py`: `get_root_connection()` (`@st.cache_resource`) với `:memory:`, `memory_limit='256MB'`, `threads=2`, `TimeZone='UTC'`, `extension_directory` cấu hình được qua biến môi trường để cài sẵn extension khi đóng gói Docker (Task 5.9).
2. Mỗi truy vấn qua `run_query()` bắt buộc tạo `conn.cursor()` riêng và đóng trong `finally`, ghi log `query_ms` (không bao giờ log credential hay SQL `CREATE SECRET`), và có **retry + exponential backoff** trước khi ném `DatabaseConnectionError` để UI hiển thị thông báo thân thiện thay vì sập trang.
3. Giảm tải cho MinIO bằng cách lọc partition `event_date` trước khi quét (Task 5.2), kết hợp `OPTIMIZE` định kỳ (Task 3.8) và `st.cache_data(ttl=5)` (Task 5.7).

## 5. Hệ quả
- *Được:*
  - Kiến trúc Zero-Copy tinh gọn, 0 bản sao dữ liệu dư thừa, tổng RAM thấp và triển khai nhanh.
  - Hoàn toàn an toàn đa luồng giữa các phiên Streamlit khi mỗi query dùng `cursor()` riêng trên `:memory:`.
- *Mất / Đánh đổi:*
  - Khả năng chịu tải đồng thời bị giới hạn bởi số lõi CPU local và băng thông `httpfs` tới MinIO nếu dữ liệu tăng mạnh.

## 6. Điều kiện xem xét lại (Ngưỡng chuyển sang Push / ClickHouse — F15 & Task 7.3)
Quyết định này cần được đánh giá lại và chuyển sang **ClickHouse (Phương án B)** khi xuất hiện một trong các ngưỡng sau:
1. Số người dùng mở Dashboard đồng thời vượt **15–20 người** khiến `p95` độ trễ truy vấn vượt **1.5 giây** liên tục dù đã bật `st.cache_data(ttl=5)` và `OPTIMIZE`.
2. Tổng dung lượng phân vùng Silver cần quét cho cửa sổ ngày vượt **50–100 GB** (làm quét S3 mỗi 10s quá chậm).
3. Hệ thống phát sinh yêu cầu tổng hợp liên tục dưới 1 giây khi ghi hoặc phân quyền truy cập dữ liệu đa đối tượng (multi-tenant RBAC).
