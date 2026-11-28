# F15 — Mẫu Phục vụ Dữ liệu: Push (OLAP Server) và Pull (Zero-Copy Lakehouse)

## 1. Câu hỏi gốc
Khi đưa số liệu từ Lakehouse lên Dashboard, ta nên **"đẩy" (Push)** dữ liệu vào một máy chủ OLAP chuyên dụng (như ClickHouse) trước, hay để Dashboard **"kéo" (Pull)** trực tiếp từ MinIO khi có người xem?

## 2. Trực giác
- **Push (ClickHouse):** Giống nhà hàng nấu sẵn các nồi súp theo công thức cố định ở bếp nóng — khách gọi là múc ra ngay trong vài mili-giây, phục vụ trăm bàn cùng lúc rất khỏe, nhưng luôn phải nuôi bếp nóng tốn điện và RAM cả ngày.
- **Pull (DuckDB đọc Lakehouse):** Giống đầu bếp gọn nhẹ chỉ đi lấy đúng nguyên liệu trong kho lạnh (MinIO) khi có khách gọi món, kết hợp giữ ấm 5 giây (`st.cache_data(ttl=5)`) để nhiều khách gọi cùng món không phải đi kho lại nhiều lần.

## 3. Cơ chế

| Tiêu chí | **Push** (ClickHouse + Materialized View) | **Pull** (DuckDB `:memory:` + Streamlit) |
| :--- | :--- | :--- |
| Độ trễ truy vấn | Rất thấp, ổn định | Phụ thuộc quét S3 & số lượng file Delta (`OPTIMIZE`) |
| Nhiều người dùng đồng thời | Rất tốt | Tăng tuyến tính nếu bỏ cache; giảm mạnh với `ttl=5` |
| Độ phức tạp & RAM vận hành | Cao (thêm server, pipeline nạp, bản sao dữ liệu) | Rất nhẹ (`memory_limit='256MB'`, không sao chép dữ liệu) |
| Khi nào phù hợp | > 15–20 người dùng đồng thời, tải quét lớn | Nhóm nhỏ, máy cá nhân 16 GB RAM, Zero-Copy Lakehouse |

## 4. Ví dụ số
- Giả sử `20` người cùng mở Panel 1 làm tươi mỗi `10s`:
  - **Không có cache:** `20` truy vấn/10s quét MinIO lặp lại cùng một snapshot Silver.
  - **Có `st.cache_data(ttl=5)` + `get_root_connection()` (`@st.cache_resource`):** Trong cửa sổ `5` giây chỉ có tối đa **`1` lần** quét MinIO cho cùng tham số, cắt giảm **`95%`** lưu lượng đọc S3.
- Ngưỡng chuyển sang Push (ADR-004): khi người dùng đồng thời `> 15–20` khiến `p95` vượt **`1.5s`** hoặc dữ liệu quét ngày vượt `50–100 GB`.

## 5. Hiểu lầm phổ biến
1. *Hiểu lầm:* Pull luôn chậm hơn Push ở mọi quy mô.  
   *Sự thật:* Ở quy mô vài GB sau compaction (`54.45 ms` median), Pull đạt thừa SLO `<= 1.5s` mà không tốn thêm GB RAM hay đồng bộ bản sao thứ hai.
2. *Hiểu lầm:* Pull không cần xử lý lỗi mạng S3.  
   *Sự thật:* Vì mỗi lượt kéo đọc trực tiếp qua HTTP (`httpfs`), `run_query()` bắt buộc phải có **retry + exponential backoff** và ném `DatabaseConnectionError` để UI không sập khi MinIO chập chờn.

## 6. Trong repo này

| File:dòng | Symbol | Đoạn code | Giải thích dòng nào quan trọng |
| :--- | :--- | :--- | :--- |
| `docs/adr/004-serving.md:34-60` | `ADR-004` | Ngưỡng chuyển sang ClickHouse | Chốt kiến trúc Pull hiện tại và tiêu chí định lượng cho Task 7.3. |
| `src/dashboard/db.py:171-198` | `run_query` | Retry + `DatabaseConnectionError` | Bảo vệ luồng Pull trước lỗi mạng/HTTP 503 tạm thời từ S3/MinIO. |

```python
# Trích nguyên văn từ src/dashboard/db.py:192-196
            if attempt >= attempts:
                raise DatabaseConnectionError(attempts=attempt, cause=exc) from exc

            delay = base_backoff_sec * (2 ** (attempt - 1))
            sleep_fn(delay)
```

## 7. Tự kiểm tra
1. *Câu hỏi:* Cache `ttl=5` trên lớp repository giúp ích thế nào khi nhiều người cùng mở Dashboard?  
   *Đáp án:* Mọi người dùng cùng tham số truy vấn trong 5 giây dùng chung 1 DataFrame kết quả thay vì mỗi tab kích hoạt một lượt quét `delta_scan()` mới lên MinIO.
2. *Câu hỏi:* Dấu hiệu định lượng đầu tiên cho thấy hệ thống cần chuyển từ Pull (DuckDB) sang Push (ClickHouse)?  
   *Đáp án:* Độ trễ `p95` vượt SLO `1.5s` liên tục dù đã bật `st.cache_data(ttl=5)` và chạy `OPTIMIZE` trên bảng Delta.
3. *Câu hỏi:* Nếu chuyển sang Push (ClickHouse), dự án đánh đổi điều gì?  
   *Đáp án:* Mất tính thuần Zero-Copy (phát sinh thêm một kho dữ liệu sao chép phải đồng bộ và ngốn thêm RAM vận hành).

## 8. Đọc thêm
- `docs/adr/004-serving.md` (Kiến trúc Serving Zero-Copy trong repo)
- DuckDB HTTPFS & S3 Remote Scanning Best Practices: https://duckdb.org/docs/extensions/httpfs/s3_api
