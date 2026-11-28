# F11 — OLTP vs OLAP, Engine Vector hoá, Nhúng (`:memory:`) và Server

## 1. Câu hỏi gốc
Vì sao hệ thống không dùng chung một cơ sở dữ liệu giao dịch (như PostgreSQL/MySQL) để vừa ghi đơn hàng vừa chạy truy vấn tổng hợp cho Dashboard, và vì sao DuckDB chạy trong tiến trình ứng dụng lại chọn chế độ `:memory:` thay vì file `.duckdb` bền?

## 2. Trực giác
- **OLTP (Postgres):** Giống quầy thu ngân siêu thị — xử lý hàng nghìn giao dịch nhỏ mỗi giây, tìm đúng 1 dòng hoá đơn để cập nhật thật nhanh (lưu theo dòng).
- **OLAP (DuckDB/ClickHouse):** Giống phòng kiểm toán cuối ngày — đọc ít truy vấn hơn nhưng mỗi lần quét hàng triệu dòng trên vài cột (`spend`, `campaign_id`) để tính tổng (lưu theo cột).
- **Nhúng (`:memory:`) vs File `.duckdb`:** Mở file `.duckdb` có chế độ *single-writer lock* (ai ghi là người khác chờ/văng lỗi). Khi để DuckDB chạy `:memory:` và đọc thẳng Delta/Parquet trên MinIO, không hề có tệp khóa nào trên đĩa để các thread tranh chấp.

## 3. Cơ chế

```
[Streamlit Threads (5 phiên người dùng song song)]
         │           │           │
         ▼           ▼           ▼
   cur = root.cursor()   (Mỗi truy vấn 1 cursor riêng, đóng trong finally)
         │
         ▼
[DuckDB :memory: Root Connection (@st.cache_resource)]
   ├── memory_limit = '256MB' | threads = 2 | TimeZone = 'UTC'
   ├── Vectorized Engine (lô ~2048 phần tử vừa L1/L2 CPU cache)
   └── Extensions: httpfs + delta ──► delta_scan('s3://lakehouse/silver/cpc_events')
```

1. **Thực thi vector hoá (Vectorized Execution):** Thay vì xử lý từng dòng một (*tuple-at-a-time*) làm tràn lệnh nhánh CPU, DuckDB truyền các mảng cột nhỏ (vector ~2048 giá trị) qua từng toán tử, tận dụng tối đa CPU cache và SIMD.
2. **Embedded vs Server:** DuckDB chạy cùng tiến trình Python của Dashboard (không tốn RAM cho daemon riêng), nhưng bị giới hạn bởi single-writer khi dùng file `.duckdb`. Dùng `:memory:` + `cursor()` riêng cho từng luồng giúp đọc đa luồng an toàn tuyệt đối trên dữ liệu Delta bất biến.

## 4. Ví dụ số
- Giả sử bảng Silver có `20` cột, Dashboard Panel 1 chỉ đọc `3` cột (`campaign_id`, `event_time`, `spend`):
  - **Row store (OLTP):** phải đọc đủ 100% dung lượng cả 20 cột từ trang đĩa.
  - **Columnar OLAP (DuckDB trên Parquet):** chỉ đọc `~15%` cột cần thiết và tính tổng bằng vector hoá ➔ đạt median **`54.45 ms`** trên dữ liệu đã `OPTIMIZE` (`docs/benchmarks/small_files.md`).

## 5. Hiểu lầm phổ biến
1. *Hiểu lầm:* Có thể share chung 1 `duckdb.Connection` cho nhiều thread Streamlit gọi `.execute()` trực tiếp.  
   *Sự thật:* Một connection DuckDB không an toàn khi nhiều thread dùng chung một trạng thái kết quả; bắt buộc tạo `conn.cursor()` riêng cho mỗi truy vấn và đóng trong `finally`.
2. *Hiểu lầm:* Ghi kết quả trung gian vào file `dashboard.duckdb` rồi đọc sẽ luôn tốt hơn `:memory:`.  
   *Sự thật:* File `.duckdb` áp đặt khóa single-writer; nếu vừa có worker ghi vừa có UI đọc sẽ văng lỗi khóa file ngay (Lab L6).

## 6. Trong repo này

| File:dòng | Symbol | Đoạn code | Giải thích dòng nào quan trọng |
| :--- | :--- | :--- | :--- |
| `src/dashboard/db.py:104-110` | `create_root_connection` | `:memory:`, `memory_limit='256MB'`, `UTC` | Khởi tạo DuckDB nhúng không file khóa, giới hạn RAM 256MB và chuẩn múi giờ UTC. |
| `src/dashboard/db.py:171-198` | `run_query` | `cur = conn.cursor()`, `finally: cur.close()` | Cô lập cursor theo từng truy vấn đa luồng và đo `query_ms`. |
| `docs/adr/004-serving.md:34-43` | `ADR-004` | Quyết định Zero-Copy | Giải thích ngưỡng chuyển sang OLAP server (ClickHouse). |

```python
# Trích nguyên văn từ src/dashboard/db.py:171-178
    for attempt in range(1, attempts + 1):
        cur = conn.cursor()
        t0 = time.perf_counter()
        try:
            df = cur.execute(sql, bound_params).df()
            elapsed_ms = (time.perf_counter() - t0) * 1000.0
            logger.info("query_ms=%.1f rows=%d attempt=%d", elapsed_ms, len(df), attempt)
            return df
```

## 7. Tự kiểm tra
1. *Câu hỏi:* Vì sao dùng `:memory:` với `delta_scan()` tránh được lỗi khóa file trong Lab L6?  
   *Đáp án:* Không mở tệp `.duckdb` nào trên đĩa nên không tồn tại single-writer file lock; mọi lần đọc qua `httpfs`/`delta` đều đọc snapshot bất biến trên MinIO.
2. *Câu hỏi:* Hai điểm mà OLAP server (ClickHouse) vượt trội DuckDB nhúng là gì?  
   *Đáp án:* Chịu tải nhiều người dùng đọc đồng thời với độ trễ ổn định và tự động tổng hợp liên tục khi dữ liệu đang chảy vào (Materialized View).
3. *Câu hỏi:* Vì sao `src/dashboard/db.py` bắt buộc gọi `conn.cursor()` cho mỗi truy vấn thay vì dùng trực tiếp `conn.execute()`?  
   *Đáp án:* Streamlit chạy mỗi phiên người dùng trên một thread riêng; `cursor()` cô lập trạng thái thực thi và kết quả giữa các thread.

## 8. Đọc thêm
- DuckDB Concurrency & Python Multithreading: https://duckdb.org/docs/api/python/overview#using-connections-in-parallel-python-programs
- DuckDB Vectorized Execution Architecture: https://duckdb.org/why_duckdb
