# F05 — Trạng thái (State) trong Streaming & Bẫy Tràn Bộ Nhớ

## 1. Câu hỏi gốc
Để biết một `event_id` đã từng xuất hiện hay chưa giữa các micro-batch cách nhau vài giây hoặc vài phút, Spark phải lưu "trí nhớ" đó ở đâu, làm sao để trí nhớ đó không phình vô hạn làm sập container 2.3GB RAM, và làm sao khôi phục lại nếu Spark bị tắt đột ngột?

## 2. Trực giác
Hãy coi **State Store** giống như cuốn sổ tay ghi nhanh đặt trên bàn làm việc của nhân viên kiểm vé (RAM Heap của Spark Driver), còn **Checkpoint State** giống như bản sao lưu cuốn sổ tay đó cất vào két sắt (MinIO `s3a://lakehouse/checkpoints/refine_silver/state/`).
- Nếu nhân viên cứ ghi thêm tên khách mới mà **không bao giờ gạch bỏ tên cũ** (quên đặt watermark), cuốn sổ sẽ dày lên vô tận cho đến khi sập bàn làm việc (OOM Killer `exit 137`).
- Khi đặt watermark `10 minutes`, nhân viên định kỳ xóa các vé cũ hơn 10 phút khỏi sổ tay ➔ kích thước state giữ ổn định quanh một ngưỡng cố định.

## 3. Cơ chế Quản lý State trong Q2 Refine

```text
[Bronze Micro-batch] ──► dropDuplicatesWithinWatermark(["event_id"])
                                │
             ┌──────────────────┴──────────────────┐
             ▼                                     ▼
   [State Store (RAM Heap)]              [MinIO Checkpoint /state]
   • Lưu event_id còn trong 10m          • Sao lưu delta state mỗi batch
   • Evict key < (watermark - 10m)       • Khôi phục state khi restart
   • (Tùy chọn bật RocksDB nếu lớn)
```

1. **HDFSBackedStateStoreProvider (Mặc định):** Lưu toàn bộ map state trong JVM Heap (`driver_memory: 1536m`) và sao lưu các file delta snapshot lên `checkpoints/refine_silver/state/`. Phù hợp khi số key hoạt động trong cửa sổ nhỏ/vừa.
2. **RocksDBStateStoreProvider (Dự phòng):** Đưa dữ liệu state ra bộ nhớ native off-heap/đĩa cục bộ container để giảm áp lực Garbage Collection trên JVM Heap khi state chứa hàng triệu key (dự án chọn chiến lược **"đo trước, bật sau"** qua `spark.enable_rocksdb` trong `config/pipeline.yaml`).
3. **Phối hợp 2 lớp State + Global MERGE:** State chỉ cần nhớ `event_id` trong cửa sổ 10 phút gần nhất để chặn nhanh phần lớn trùng lặp trong bộ nhớ; mọi bản trùng đến muộn hơn (sau khi state đã evict) được Delta `MERGE` ở lớp sink chặn lại.

## 4. Ví dụ số
Với luồng chạy `ac_3_3`:
- Tổng `1,986` bản ghi Bronze trong cùng một cửa sổ sự kiện.
- Toàn bộ state `event_id` giữ trong bộ nhớ chưa tới vài trăm KB — nằm gọn rất sâu dưới trần `1536m` của Spark Driver heap.
- Nếu không có `withWatermark` khi chạy liên tục hàng triệu sự kiện qua nhiều tuần, state sẽ giữ mãi toàn bộ `event_id` lịch sử ➔ vượt ngưỡng `mem_limit: 2304m` và bị Linux OOM Killer chấm dứt container.

## 5. Điểm mạnh / Điểm yếu / Khi nào không dùng
- **Điểm mạnh:** Khử trùng và tổng hợp liên tục xuyên suốt nhiều micro-batch với tốc độ bộ nhớ, có khả năng khôi phục trạng thái sau crash nhờ checkpoint.
- **Điểm yếu:** Tốn RAM/GC; thay đổi logic stateful sau khi đã có checkpoint có thể gây không tương thích schema state (cần xóa checkpoint khi nâng cấp logic stateful lớn).
- **Khi nào không dùng:** Các bước lọc/biến đổi từng dòng độc lập (stateless) như Q1 Ingest (`validate_df`) — không cần state store và không cần watermark.

## 6. Hiểu lầm phổ biến
1. *Hiểu lầm:* Bật RocksDB lúc nào cũng nhanh và tiết kiệm RAM hơn Heap mặc định.  
   *Sự thật:* Với tập dữ liệu nhỏ/vừa (< vài trăm nghìn key), Heap mặc định ít overhead tuần tự hóa (serialization) hơn; RocksDB chỉ phát huy tác dụng khi state lên tới hàng triệu key gây áp lực GC nặng, và vẫn tốn bộ nhớ off-heap cần tính vào `mem_limit` của Docker.

## 7. Trong repo này

| File:dòng | Symbol | Giải thích |
| :--- | :--- | :--- |
| `src/streaming/refine.py:260-286` | `start_refine_query` | Kết hợp toán tử có trạng thái `dropDuplicatesWithinWatermark` với checkpoint `refine_silver`. |
| `src/streaming/spark_session.py:85-86` | `build_spark_conf` | Cho phép chuyển đổi sang `RocksDBStateStoreProvider` qua cờ `enable_rocksdb`. |
| `config/pipeline.yaml:25-26` | `spark.enable_rocksdb` | Chốt mặc định `false` ("đo trước, bật sau" — Task 3.2 & 3.7). |

## 8. Tự kiểm tra
1. *Câu hỏi:* Trong kiến trúc 2 query Q1 và Q2, query nào là stateless và query nào là stateful?  
   *Đáp án:* Q1 Ingest (`ingest.py`) là **stateless** (chỉ đọc Kafka, validate từng dòng, append Bronze/DLQ); Q2 Refine (`refine.py`) là **stateful** (dùng watermark + `dropDuplicatesWithinWatermark` nhớ `event_id` giữa các batch).
2. *Câu hỏi:* Nếu xóa thư mục checkpoint của Q2 (`checkpoints/refine_silver`), điều gì xảy ra khi khởi động lại Q2?  
   *Đáp án:* Q2 mất state trong bộ nhớ và đọc lại toàn bộ Bronze từ đầu, nhưng nhờ tầng sink dùng **Delta `MERGE` insert-only theo `event_id`** nên bảng Silver vẫn không bị trùng dữ liệu (kịch bản S3 ở Task 3.6).
