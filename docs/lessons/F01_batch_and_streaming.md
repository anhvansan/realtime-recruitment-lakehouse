# F01 — Batch và Streaming: Bản chất của dữ liệu vô hạn

## 1. Câu hỏi gốc
Khi luồng click quảng cáo tuyển dụng đến liên tục và không có điểm kết thúc, khái niệm "xử lý xong" khác gì so với chạy báo cáo trên một file CSV tĩnh?

## 2. Trực giác
- **Batch (Dữ liệu hữu hạn — Bounded):** Giống việc chụp ảnh cả mặt hồ vào cuối ngày rồi đếm số cá — biết rõ điểm bắt đầu và điểm kết thúc.
- **Streaming (Dữ liệu vô hạn — Unbounded):** Giống đứng ở cửa xả nước đếm từng con cá bơi qua ngay khi nó xuất hiện; hệ thống chạy liên tục 24/7 và xuất kết quả từng phần theo thời gian.

## 3. Cơ chế & Kiến trúc Micro-batch

```
[Bounded CSV / Parquet] ──► (Chunk Streaming Loader) ──► [Redpanda Unbounded Log]
                                                                │
        ┌─── Micro-batch Trigger (mỗi 10 giây) ─────────────────▼
        │    1. Ghi offset dự kiến ➔ checkpoint/offsets
        │    2. Thực thi Batch nhỏ trên dữ liệu mới nhất
        │    3. Ghi kết quả idempotent xuống Delta Lake (Bronze / Silver)
        │    4. Ghi nhận hoàn tất ➔ checkpoint/commits
```

1. **Micro-batch (Spark Structured Streaming):** Chia dòng chảy vô tận thành các lô nhỏ chạy theo chu kỳ `trigger` (10s), tận dụng được cùng một engine SQL/DataFrame với batch.
2. **Định nghĩa Real-time qua SLO:** Trong dự án này, SLO độ trễ đầu–cuối `p95 <= 45 giây` khớp hoàn toàn với trigger 10 giây + thời gian xử lý ~2 giây.
3. **Đọc luồng theo Chunk ở phía Producer:** Ngay cả khi phát lại file lịch sử `replay_sorted.parquet`, Replay Producer vẫn đọc theo batch nhỏ (`chunk_size=256` qua `ParquetFile.iter_batches`) thay vì nạp hết bảng vào RAM.

## 4. Ví dụ số
Với trigger 10 giây ở tầng Ingest:
- Event đến ở giây `0.1` phải chờ tới giây `10.0` mới vào lô ➔ chờ trung bình **~5s** + xử lý **~2s** = độ trễ ~7s (tối đa ~12s).
- Replay Producer nén thời gian với `speed_factor=60`: **1 phút ngoài đời thật = 1 giờ dữ liệu sự kiện** (tức 1 giây thật = 1 phút mô phỏng).

## 5. Điểm mạnh / Điểm yếu / Khi nào không dùng
- **Điểm mạnh (Micro-batch):** Thông lượng cao, chịu lỗi dễ, tái sử dụng API batch.
- **Điểm yếu:** Có độ trễ tối thiểu bằng chu kỳ trigger; tốn RAM giữ JVM chạy liên tục.
- **Khi nào không dùng:** Cần phản ứng mức mili-giây cho từng event đơn lẻ (chống gian lận thanh toán thẻ) ➔ dùng event-at-a-time (Apache Flink).

## 6. Hiểu lầm phổ biến
1. *Hiểu lầm:* Hạ `trigger` từ 10s xuống 1s giúp hệ thống nhanh hơn miễn phí.  
   *Sự thật:* Mỗi micro-batch tạo một commit `_delta_log` và các file Parquet nhỏ mới trên MinIO ➔ hạ trigger xuống 1s tạo file nhỏ gấp 10 lần và làm chậm cả Spark lẫn DuckDB!

## 7. Trong repo này

| File:dòng | Symbol | Giải thích |
| :--- | :--- | :--- |
| `src/producer/replay_producer.py:302-327` | `iter_parquet_chunks` | Đọc Parquet theo chunk `iter_batches(batch_size=256)`, không nạp cả file vào RAM. |
| `src/producer/time_mapper.py:30-58` | `TimeMapper` | Nén đồng hồ dữ liệu lịch sử theo `speed_factor` thành luồng sự kiện liên tục. |
| `config/pipeline.yaml` | `ingest_trigger`, `refine_trigger` | Cấu hình chu kỳ micro-batch `10 seconds` cho cụm Spark. |

## 8. Tự kiểm tra
1. *Câu hỏi:* Trong hệ thống của ta, nguồn nào là bounded và luồng nào là unbounded?  
   *Đáp án:* 18 file CSV và `replay_sorted.parquet` là bounded; topic `cpc_events_raw` trên Redpanda khi Producer phát liên tục là luồng unbounded.
2. *Câu hỏi:* Vì sao `iter_parquet_chunks` dùng `ParquetFile.iter_batches` thay vì `pandas.read_parquet(...).to_dict('records')`?  
   *Đáp án:* Để giữ RAM của Producer gần như không đổi bất kể file nguồn lớn hàng triệu dòng (Task 6.4 benchmark).
