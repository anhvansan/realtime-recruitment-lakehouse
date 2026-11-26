# T09 — Bộ Công cụ Python: Pydantic, Ruff, Pytest và Decoupling

## 1. Câu hỏi gốc
Làm thế nào để đảm bảo chất lượng code Python nhất quán, tự động phát hiện lỗi cú pháp/logic ngay từ đầu, và quản lý các tham số cấu hình phức tạp (S3, Kafka, database) mà không bị hard-code hoặc gây xung đột cú pháp giữa các công cụ khác nhau (như Spark vs DuckDB)?

## 2. Trực giác
Viết code không có công cụ kiểm chuẩn giống như thợ mộc đóng tủ mà không dùng thước đo và dưỡng mẫu: mỗi thanh gỗ sẽ lệch vài milimet, đến khi ghép lại thì cánh tủ không đóng được. `Ruff` đóng vai trò là chiếc thước đo laser quét siêu nhanh; `Pytest` là bài kiểm tra độ chịu lực trước khi xuất xưởng; còn `Pydantic` giống như nhân viên kiểm định nguyên liệu đầu vào — kiểm tra từng biến môi trường xem có đúng quy cách và định dạng trước khi cho phép hệ thống khởi động.

## 3. Cơ chế
Hệ thống công cụ quản trị code trong dự án được tổ chức theo các lớp bảo vệ:

```
[Developer Code / AI Agent]
            │
            ▼
     [Ruff Check & Format] ──── (Lint tĩnh: style, unused imports, bugbear, line-length 100)
            │
            ▼
     [Pytest Test Suite]   ──── (Test động: unit tests, mock environment, assertions)
            │
            ▼
  [Pydantic BaseSettings]  ──── (Nạp .env + pipeline.yaml, type casting, validation)
            │
            ├─► Spark / Boto3 Endpoint : "http://minio:9000" (Cần cả protocol scheme)
            └─► DuckDB S3 Secret       : "minio:9000" (Bắt buộc CẮT BỎ protocol scheme)
```

**Nguyên lý Decoupling và cạm bẫy S3 Host:Port (K1):**
DuckDB khi cấu hình Secret S3 yêu cầu thuộc tính `ENDPOINT` chỉ chứa `host:port` (ví dụ `minio:9000`), không được có tiền tố `http://` hay `https://`. Ngược lại, PySpark và AWS SDK (`boto3`) lại yêu cầu đầy đủ URL bao gồm cả scheme. Lớp cấu hình `src/common/config.py` đóng vai trò trung gian chuyển đổi tự động thuộc tính này.

## 4. Ví dụ số
- Nếu biến môi trường là `S3_ENDPOINT="http://minio:9000/"`:
  - `s3_endpoint` trả về `"http://minio:9000/"` (phục vụ Spark/Boto3).
  - `s3_endpoint_host_port` tự động cắt bỏ `http://` và dấu `/` ở cuối, trả về chính xác `"minio:9000"` (phục vụ DuckDB Secret).
  - `s3_use_ssl` trả về `False`.
- Nếu biến môi trường là `S3_ENDPOINT="https://s3.amazonaws.com"`:
  - `s3_endpoint_host_port` trả về `"s3.amazonaws.com"`.
  - `s3_use_ssl` trả về `True`.

## 5. Điểm mạnh / Điểm yếu / Khi nào không dùng
- **Điểm mạnh:**
  - `Ruff` viết bằng Rust, tốc độ thực thi nhanh hơn Flake8/Black từ 10–100 lần.
  - `Pydantic` ngăn chặn lỗi `TypeError` hoặc thiếu biến môi trường ngay khi ứng dụng khởi chạy (`fail-fast`).
  - `Pytest` cho phép viết các fixture cô lập để test nghiệp vụ mà không cần bật cụm container Docker.
- **Điểm yếu:** Cần thiết lập cấu hình nghiêm ngặt ban đầu; Pydantic v2 có thể làm chậm thời gian import một chút so với dataclass thuần túy.
- **Khi nào không dùng:** Các đoạn script một lần (one-off scratchpad) cực ngắn không tái sử dụng.

## 6. Hiểu lầm phổ biến
1. *Hiểu lầm:* Truyền nguyên chuỗi `http://minio:9000` vào câu lệnh `CREATE SECRET ... (TYPE S3, ENDPOINT 'http://minio:9000')` trong DuckDB vẫn chạy được.  
   *Sự thật:* DuckDB sẽ báo lỗi kết nối hoặc phân giải domain không hợp lệ do DuckDB mong đợi `ENDPOINT 'minio:9000'` kèm `USE_SSL false`.
2. *Hiểu lầm:* Chỉ cần viết unit test khi hệ thống đã hoàn thiện toàn bộ.  
   *Sự thật:* TDD và test cấu hình từ Task 1.1 giúp phát hiện ngay sai sót nền tảng trước khi xây dựng các pipeline phức tạp.

## 7. Trong repo này

| File:dòng | Symbol | Đoạn code | Giải thích dòng nào quan trọng |
| :--- | :--- | :--- | :--- |
| `src/common/config.py:130-139` | `AppConfig.s3_endpoint_host_port` | Trích đoạn nguyên văn từ `src/common/config.py`: | Tách bỏ tiền tố `http://` hoặc `https://` và dấu gạch chéo cuối để phục vụ DuckDB Secret. |
| `src/producer/replay_producer.py:642-803` | `run_replay` | Bắt `SIGINT` & `flush()` sạch | Dùng `threading.Event` kết hợp `signal.signal(signal.SIGINT, ...)` đánh thức vòng đợi deadline ngay lập tức và `flush()` trước khi thoát. |

```python
    @property
    def s3_endpoint_host_port(self) -> str:
        """Strip scheme (http:// or https://) and trailing slashes for DuckDB / dbt-duckdb.

        Example:
            'http://minio:9000' -> 'minio:9000'
            'https://s3.amazonaws.com' -> 's3.amazonaws.com'
        """
        clean_url = re.sub(r"^https?://", "", self.s3_endpoint.strip())
        return clean_url.rstrip("/")
```

## 8. Tự kiểm tra
1. *Câu hỏi:* Tại sao `ruff check .` là lệnh bắt buộc trong quy trình CI/CD và pre-commit hook?  
   *Đáp án:* Để ngăn chặn các lỗi mã nguồn cơ bản (import thừa, biến chưa khai báo, vi phạm quy ước độ dài dòng) ngay trước khi code được đẩy lên repository.
2. *Câu hỏi:* Làm thế nào để chạy pytest mà bỏ qua các test nặng đòi hỏi Spark hoặc Docker đang chạy?  
   *Đáp án:* Sử dụng marker lọc: `pytest -q -m "not spark and not integration"`.
3. *Câu hỏi:* Biến `s3_use_ssl` được xác định như thế nào trong `config.py`?  
   *Đáp án:* Kiểm tra xem chuỗi `s3_endpoint` có bắt đầu bằng tiền tố `"https://"` hay không.

## 9. Đọc thêm
- Ruff Documentation: https://docs.astral.sh/ruff/
- Pydantic Settings: https://docs.pydantic.dev/latest/concepts/pydantic_settings/
- DuckDB S3 Secret Configuration: https://duckdb.org/docs/extensions/httpfs/s3_api.html
