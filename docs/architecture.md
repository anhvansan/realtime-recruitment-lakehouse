# Kiến trúc Hệ thống Near Real-Time CPC AdTech Lakehouse

## 1. Tổng quan Luồng Dữ liệu (End-to-End Architecture)

Hệ thống được thiết kế theo kiến trúc **Zero-Copy Lakehouse** trên máy cá nhân (16GB RAM, Windows/WSL2) với 2 đường phục vụ dữ liệu riêng biệt:

```
 tracking.csv (+ các file nguồn đã chọn ở Task 2.1)
      ──► [1] Replay Producer (Python, confluent-kafka)
               • Ánh xạ thời gian mô phỏng, speed_factor
               • Bơm 2% trùng / 2% trễ / 1% JSON hỏng + ghi manifest sự thật
                               │  key = campaign_id
                               ▼
           [2] Redpanda (Kafka API)  ──── topic cpc_events_raw (3 partitions)
                       │                    topic cpc_events_dlq (1 partition)
                       ▼
         ┌─────────────── [3] Spark Structured Streaming (1 JVM, local mode) ───────────────┐
         │  Q1 ingest : Kafka ➔ parse(from_json PERMISSIVE) ➔ validate                      │
         │       foreachBatch ├─ hợp lệ  ➔ Bronze (Delta, append, idempotent txnVersion)    │
         │                    └─ lỗi     ➔ DLQ Delta + topic cpc_events_dlq                 │
         │  Q2 refine : Bronze (readStream Delta) ➔ chuẩn hoá ➔ watermark 10m ➔ dedup       │
         │       foreachBatch ➔ MERGE (insert-if-not-matched on event_id) ➔ Silver          │
         │  Maintenance: chỉ OPTIMIZE các partition ngày cũ đã đóng · Listener ➔ ops        │
         └──────────────────────────────────────────────────────────────────────────────────┘
                               │ Delta files + _delta_log
                               ▼
           [4] MinIO (S3-compatible object store, bucket `lakehouse`, dữ liệu ở ổ D)
                ▲                                  ▲
 đọc trực tiếp (delta_scan)                  đọc/ghi qua dbt-duckdb
                │                                  │
    [5a] Streamlit + DuckDB :memory:          [5b] dbt-duckdb: gold marts + data tests
         (đường nhanh, làm tươi ~10s)               (đường sâu, chạy theo lịch/on-demand)
                │
    [6] Alert Worker (process riêng) ──► Telegram / Discord (có cooldown, trạng thái, resolve)
```

## 2. Hai đường dữ liệu phục vụ Dashboard (Fast Path & Deep Path)

1. **Đường nhanh (Fast Path):**
   - Streamlit sử dụng DuckDB (`:memory:`) gọi `delta_scan()` trực tiếp trên tầng Silver của MinIO.
   - Chu kỳ làm tươi: ~10 giây.
   - Phục vụ: Giám sát pacing thời gian thực (P1), phát hiện click fraud tức thì (P2), kiểm tra sức khỏe pipeline (P4).

2. **Đường sâu (Deep Path):**
   - dbt-duckdb chạy định kỳ / theo lịch biến đổi dữ liệu Silver thành các Gold Data Marts dạng Parquet.
   - Phục vụ: Phân tích hiệu quả tuyển dụng chuyên sâu (P3), đối soát tài chính định kỳ, chạy bộ kiểm định chất lượng (dbt test).

## 3. Quản trị Tài nguyên & Bố cục Lưu trữ (S3 MinIO)

Toàn bộ dữ liệu Delta và Parquet được tổ chức trong bucket `lakehouse`:
- `s3://lakehouse/bronze/cpc_events/`: Append-only, phân vùng theo `event_date`.
- `s3://lakehouse/silver/cpc_events/`: Deduplicated và validated theo `event_id`, phân vùng theo `event_date`.
- `s3://lakehouse/dlq/cpc_events_dlq/`: Lưu vết toàn bộ bản ghi lỗi để truy vấn và đối soát.
- `s3://lakehouse/ops/stream_metrics/`: Ghi nhận tiến trình streaming của Spark.
- `s3://lakehouse/gold/<model>/*.parquet`: Bảng marts do dbt tạo.
- `s3://lakehouse/checkpoints/`: Checkpoint cho Spark Streaming queries.
