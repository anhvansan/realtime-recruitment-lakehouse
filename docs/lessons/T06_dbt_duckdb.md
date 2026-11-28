# T06 — dbt với Adapter `dbt-duckdb` trên Delta Lake

## 1. Câu hỏi gốc
Làm sao tách toàn bộ logic biến đổi nghiệp vụ (Staging ➔ Gold marts) và kiểm định chất lượng dữ liệu (Data Quality Tests) ra khỏi luồng Spark Structured Streaming thành các câu SQL có quản lý phiên bản (Git), tự động kiểm tra DAG phụ thuộc và chạy kiểm thử tự động trên dữ liệu Delta Lake ở MinIO mà không cần dựng thêm Data Warehouse server riêng?

## 2. Trực giác
Hãy hình dung Spark Streaming là dây chuyền đóng gói thô và làm sạch ở nhà máy (Bronze ➔ Silver), còn **dbt** giống như bản thiết kế bếp trưởng: dbt **không** tự nấu hay chứa dữ liệu, mà biên dịch các công thức SQL + Jinja (`ref()`, `source()`) theo đúng thứ tự phụ thuộc rồi gửi thẳng cho động cơ **DuckDB** thực thi. Nhờ chạy `:memory:` kèm `httpfs` và `delta`, DuckDB đọc thẳng bảng Silver trên MinIO qua `delta_scan()` mà không cần sao chép dữ liệu đi đâu cả (Zero-Copy Lakehouse).

## 3. Cơ chế

```
[dbt_project.yml + profiles.yml]
   │  (path: ':memory:', extensions: [httpfs, delta],
   │   settings: {memory_limit: '512MB', TimeZone: 'UTC'},
   │   secrets: S3 từ env_var, endpoint không có 'http://')
   ▼
[Parse & Build DAG] ◄── sources.yml (external_location: delta_scan('s3://.../silver/cpc_events'))
   │
   ▼
[Jinja Compile] ───────► target/compiled/.../stg_cpc_events.sql (VIEW, chọn cột tường minh)
   │
   ├──► dbt run  ──► Tạo VIEW stg_cpc_events trong DuckDB :memory:
   └──► dbt test ──► Chạy các câu SELECT tìm dòng vi phạm (not_null, unique, accepted_values)
                     (Trả về 0 dòng = PASS | >= 1 dòng = FAIL)
```

**Các thành phần chính trong luồng thực thi:**
1. **`profiles.yml` (Secret & Extension Bootstrap):** Mỗi lần dbt khởi tạo kết nối, `dbt-duckdb` mở DuckDB `:memory:`, nạp `httpfs` + `delta`, áp dụng `TimeZone='UTC'` (Rule R7) và đăng ký S3 Secret từ biến môi trường (Rule R6) với endpoint dạng `host:port` (bẫy K1).
2. **`sources.yml` (`external_location`):** Khai báo `lakehouse_silver.cpc_events` với `config.external_location: "delta_scan('s3://.../silver/cpc_events')"`. Khi biên dịch, `{{ source('lakehouse_silver', 'cpc_events') }}` được `dbt-duckdb` thay thế trực tiếp bằng lời gọi hàm `delta_scan(...)`.
3. **Staging Model (`stg_cpc_events.sql`):** Materialize dạng `view`, chỉ chọn tường minh 13 cột chuẩn từ Silver, ép kiểu chuẩn DuckDB (`VARCHAR`, `TIMESTAMP`, `DATE`, `DECIMAL(18,4)`, `BIGINT`) và dẫn xuất `event_hour = date_trunc('hour', event_time)` — **không** chứa công thức kinh doanh P1–P3.
4. **Generic Data Tests (`schema.yml`):** Trong `dbt-core` 1.11+, generic tests khai báo dưới `data_tests:`, tham số macro như `values` của `accepted_values` đặt lồng trong `arguments:` để tuân thủ chuẩn không deprecation (Rule R10).

## 4. Ví dụ số
Giả sử bảng Silver trên MinIO hiện có **`1,958` dòng** sạch (`event_id` duy nhất):
- Test `unique` trên `event_id` biên dịch thành câu truy vấn đếm nhóm trùng:
  `SELECT event_id AS unique_field, COUNT(*) AS n_records FROM stg_cpc_events WHERE event_id IS NOT NULL GROUP BY event_id HAVING COUNT(*) > 1`.
  - Khi dữ liệu sạch: truy vấn trả về **`0` dòng vi phạm** ➔ **`PASS`**.
  - Nếu có 1 cặp trùng sót lại (`1,959` dòng): truy vấn trả về **`1` dòng vi phạm** (`n_records = 2`) ➔ **`FAIL`** ngay ở bước `dbt build`, chặn dữ liệu sai chảy xuống tầng Gold.
- Test `accepted_values` kiểm tra cột `event_type` ngoài 4 giá trị `['click', 'apply', 'qualified', 'unqualified']`: trả về `0` dòng lạ ➔ **`PASS`**.

## 5. Điểm mạnh / Điểm yếu / Khi nào không dùng
- **Điểm mạnh:** Tách biệt hoàn toàn logic SQL khỏi code streaming PySpark; tự động dựng DAG và kiểm tra contract bằng `dbt build`; chạy nhẹ trên DuckDB `:memory:` mà không cần server DB riêng.
- **Điểm yếu:** Thực thi theo lô (batch/periodic), không phải event-at-a-time; tùy chọn `external_location` với `delta_scan` phụ thuộc phiên bản `dbt-duckdb`.
- **Khi nào không dùng:** Tính toán stateful từng sự kiện trong luồng hoặc cần cảnh báo độ trễ dưới vài giây (giữ ở Spark Streaming hoặc Fast Path DuckDB SQL).

## 6. Hiểu lầm phổ biến
1. *Hiểu lầm:* `dbt_cpc/profiles.yml` có thể dùng thẳng biến `S3_ENDPOINT=http://minio:9000` giống Spark.  
   *Sự thật (Bẫy K1):* DuckDB S3 Secret **cấm tiền tố `http://` hoặc `https://`** ở trường `endpoint` (chỉ nhận `host:port` như `localhost:9000`). Trong dự án, `profiles.yml` dùng bộ lọc Jinja `| replace('http://', '') | replace('https://', '')` và đặt `use_ssl: false`.
2. *Hiểu lầm:* Dùng `SELECT *` ở model staging cho gọn khi Silver đã sạch.  
   *Sự thật:* `SELECT *` làm rò rỉ thay đổi schema ngoài ý muốn xuống Gold. Luôn liệt kê và `cast(...)` tường minh từng cột ở tầng staging.
3. *Hiểu lầm:* `dbt build` sao chép toàn bộ dữ liệu Silver vào file cục bộ khi chạy `stg_cpc_events`.  
   *Sự thật:* `stg_cpc_events` có `materialized='view'` trên DuckDB `:memory:`, không sao chép dữ liệu; mọi truy vấn đều đọc trực tiếp từ MinIO qua `delta_scan()`.

## 7. Trong repo này

| File:dòng | Symbol | Đoạn code | Giải thích dòng nào quan trọng |
| :--- | :--- | :--- | :--- |
| `dbt_cpc/profiles.yml:14-22` | `dbt_cpc.outputs.dev.secrets` | Cấu hình S3 Secret an toàn | Đọc `S3_ACCESS_KEY`/`S3_SECRET_KEY` qua `env_var` (R6), cắt `http://` ở `endpoint` (K1), bật `url_style: "path"`, `use_ssl: false`. |
| `dbt_cpc/models/staging/sources.yml:6-10` | `lakehouse_silver.cpc_events` | Khai báo `external_location` | Dùng `delta_scan('s3://.../silver/cpc_events')` trực tiếp qua `config.external_location` trên `dbt-duckdb 1.11.1`. |
| `dbt_cpc/models/staging/stg_cpc_events.sql:21-36` | `stg_cpc_events` | Chiếu cột tường minh + `event_hour` | Không dùng `SELECT *`, chuẩn hóa kiểu dữ liệu và thêm `date_trunc('hour', event_time) as event_hour` theo UTC. |
| `dbt_cpc/models/staging/schema.yml:7-30` | `stg_cpc_events` `data_tests` | Generic tests chuẩn `dbt-core 1.11` | Dùng `data_tests:` và `arguments:` lồng trong `accepted_values` để tránh deprecation warning. |
| `dbt_cpc/models/gold/fct_campaign_hourly_pacing.sql:1-7` | `external` Parquet sink | `materialized='external'`, `format='parquet'` | Xuất thẳng Gold mart ra tệp Parquet trên MinIO (`s3://.../gold/*.parquet`) mà không cần lưu bảng nội bộ DuckDB. |

```sql
-- Trích nguyên văn từ dbt_cpc/models/staging/stg_cpc_events.sql:21-36
select
    event_id,
    event_time,
    date_trunc('hour', event_time) as event_hour,
    produced_at,
    processed_at,
    event_date,
    event_type,
    job_id,
    campaign_id,
    group_id,
    publisher_id,
    bid,
    spend,
    source_row_id
from source_events
```

## 8. Tự kiểm tra
1. *Câu hỏi:* Vì sao `dbt_cpc/profiles.yml` bắt buộc đặt `TimeZone: "UTC"` trong `settings` dù `event_time` ở Silver đã là UTC?  
   *Đáp án:* Máy chủ Windows host chạy múi giờ `+07:00`; nếu không ép `TimeZone='UTC'` ở DuckDB, các phép `date_trunc('hour', event_time)` hay `cast(event_time as date)` trên kiểu `TIMESTAMPTZ` có thể bị lệch 7 giờ so với chuẩn Rule R7.
2. *Câu hỏi:* Vì sao `dbt_cpc/models/staging/schema.yml` đặt `values` bên trong `arguments:` thay vì đặt ngang hàng dưới `accepted_values:`?  
   *Đáp án:* Từ `dbt-core 1.10.8+` và `1.11.x`, đặt tham số test trực tiếp ở cấp trên sẽ kích hoạt `MissingArgumentsPropertyInGenericTestDeprecation`; cú pháp chuẩn là tách `arguments:` (input macro) và `config:` (Rule R10).
3. *Câu hỏi:* Khi `dbt build -s stg_cpc_events` chạy với `path: ':memory:'`, dữ liệu có bị ghi ra đĩa ở `dbt_cpc/` không?  
   *Đáp án:* Không. View chỉ tồn tại trong phiên `:memory:` để chạy các `data_tests` ngay trong lần build đó; chỉ `dbt_cpc/target/` (đã nằm trong `.gitignore`) lưu SQL biên dịch và metadata.

## 9. Đọc thêm
- dbt Core Documentation (v1.11 — `data_tests` & `arguments`): https://docs.getdbt.com/reference/resource-properties/data_tests
- `dbt-duckdb` Adapter Documentation (Secrets & `external_location`): https://github.com/duckdb/dbt-duckdb
- DuckDB Delta Extension (`delta_scan`): https://duckdb.org/docs/extensions/delta
