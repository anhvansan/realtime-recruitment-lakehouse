# F16 — Mô hình Lỗi và Hồi phục (Failure Models & Recovery)

## 1. Câu hỏi gốc
Khi một thành phần trong pipeline (Spark, Redpanda, MinIO, hay ổ đĩa) bị chết đột ngột giữa chừng, chuyện gì xảy ra với dữ liệu đang xử lý, hệ thống có mất hoặc nhân đôi tiền không, và ta chứng minh khả năng "tự lành" bằng cách nào?

## 2. Trực giác
Với bất kỳ thành phần nào, kỹ sư dữ liệu luôn đặt **4 câu hỏi**:
1. *Nó chết thì ảnh hưởng gì ngay lúc đó?*
2. *Dữ liệu nào có nguy cơ bị mất hoặc bị ghi đúp?*
3. *Hệ thống tự hồi phục bằng cơ chế nào?*
4. *Kiểm chứng hồi phục bằng thí nghiệm nào?*

Các khái niệm trụ cột: **Checkpoint** (lưu offset đã đọc + state + commit log trên MinIO), **Backpressure** (`maxOffsetsPerTrigger` chặn tràn RAM khi đọc bù lag), **Retry + Backoff**, và **Idempotent Sink** (`txnVersion` ở Bronze/DLQ, `MERGE` ở Silver).

## 3. Bảng Mô hình Lỗi của Dự án

| Sự cố | Hệ quả tức thời | Cơ chế tự hồi phục | Kiểm chứng |
| :--- | :--- | :--- | :--- |
| **Spark bị `docker kill` giữa batch** | Micro-batch đang chạy bị đứt ngang | Restart Spark: đọc lại batch đã ghi `offsets` nhưng chưa có `commits` từ checkpoint; Bronze dùng `txnVersion`, Silver dùng `MERGE` chống trùng | Task 3.6 **S1**, **Lab L1** |
| **Spark dừng 1–5 phút khi Producer chạy** | Offset chưa đọc dồn lại trên Redpanda (lag tăng) | Khi bật lại Spark, `maxOffsetsPerTrigger=50000` điều tiết tốc độ catch-up an toàn trong ngân sách RAM | Task 3.6 **S2** |
| **Xoá checkpoint Q2 (`refine_silver`)** | Q2 mất trí nhớ vị trí đã đọc trong Bronze | Q2 đọc lại toàn bộ Bronze từ commit 0; Delta `MERGE` (`t.event_id = s.event_id`) chặn toàn bộ bản trùng | Task 3.6 **S3**, **Lab L8** |
| **Restart `redpanda` khi Producer đang gửi** | Client Producer tạm báo lỗi kết nối broker | `confluent-kafka` tự retry nội bộ (`enable.idempotence=true`, `acks=all`) và flush đầy đủ | Task 3.6 **S4** |
| **MinIO mất kết nối tạm thời** | Batch hiện tại lỗi I/O | Spark retry task/batch; Dashboard hiển thị cảnh báo mất kết nối đọc | Thử tắt `minio` |
| **Message JSON hỏng (Poison Pill)** | Không parse hoặc sai hợp đồng `cpc_event.v1` | `validate_df` tách vào DLQ (`dlq/cpc_events_dlq` + `cpc_events_dlq`), luồng chính không crash | Task 3.3 |
| **Ổ D (`DATA_ROOT`) đầy** | MinIO/Redpanda báo lỗi ghi đĩa (`ENOSPC`) | `make disk-report`, retention Redpanda 1h (`log_retention_ms`), compaction/vacuum an toàn (Task 3.8) | Task 1.5 |

## 4. Ví dụ số & Vì sao S3 (xoá checkpoint Q2) an toàn còn xoá checkpoint Q1 thì KHÔNG?
- **Checkpoint của Q1 (`checkpoints/ingest_bronze/`):** Lưu danh sách Kafka `(topic, partition, offset)` đã được lên lịch (`offsets/`) và đã hoàn tất (`commits/`).
- **Vì sao xoá checkpoint Q2 an toàn (S3 / Lab L8)?** Vì sink của Q2 là **Silver Delta** dùng `MERGE WHEN NOT MATCHED ON event_id` — đọc lại 1.986 dòng Bronze thì `MERGE` vẫn lọc ra đúng 1.958 `event_id` duy nhất (`SUM(spend) = 1589.5000`).
- **Vì sao KHÔNG được xoá checkpoint Q1 (khi chưa dọn Bronze)?** Vì Bronze ghi **append-only** kèm `txnAppId="ingest_bronze"` và `txnVersion=batch_id`. Nếu xoá `checkpoints/ingest_bronze/`, Spark mất lịch sử offset và khởi động lại từ `batch_id = 0` với dữ liệu Kafka cũ ➔ có nguy cơ nối thêm bản ghi vào Bronze hoặc làm sai lệch `batch_id` (làm gãy đối soát `(b)` ở `make reconcile`).

## 5. Điểm mạnh / Điểm yếu / Khi nào không dùng
- **Điểm mạnh:** Tách biệt hoàn toàn tính đúng đắn dữ liệu khỏi uptime của tiến trình; kill cứng giữa chừng vẫn bảo toàn `SUM(spend)` 100%.
- **Điểm yếu:** Thời gian hồi phục phụ thuộc thời gian khởi động JVM + đọc checkpoint trên S3 (~15–45s ở local); nếu Redpanda giữ retention quá ngắn (< thời gian Spark chết), offset hết hạn sẽ rơi vào chính sách `failOnDataLoss=false` (Task 1.4).
- **Khi nào không dùng:** Hệ thống yêu cầu failover dưới 1 giây (cần hot standby multi-node thay vì single-JVM local restart).

## 6. Hiểu lầm phổ biến
1. *Hiểu lầm:* Có checkpoint nghĩa là tự động không bao giờ ghi trùng dữ liệu.  
   *Sự thật:* Checkpoint Spark chỉ bảo đảm giao thức **At-least-once** (không mất batch chưa commit). Để đạt **Exactly-once** không trùng tiền, sink bắt buộc phải idempotent (`txnVersion` ở Q1, `MERGE` ở Q2).

## 7. Trong repo này

| File:dòng | Symbol | Giải thích |
| :--- | :--- | :--- |
| `src/streaming/run_all.py:78-134` | `run_streaming_pipeline` | Chạy song song Q1 + Q2 trong 1 JVM duy nhất, xử lý `SIGINT`/`SIGTERM` để dừng sạch. |
| `src/streaming/ingest.py:31-57` | `read_kafka_raw_stream` | Áp dụng `maxOffsetsPerTrigger` (backpressure) và `failOnDataLoss` theo chính sách Task 1.4. |
| `src/streaming/refine.py:192-257` | `make_upsert_silver` | Delta `MERGE` insert-only giúp kịch bản xoá checkpoint Q2 (S3 / Lab L8) hồi phục không trùng. |
| `tests/integration/reconcile.py:130-221` | `evaluate_reconciliation` | Cổng nghiệm thu `PASS`/`FAIL` sau mỗi kịch bản kill/restart S1–S4. |
| `docs/lessons/labs.md:32-59` | `Lab L1`, `Lab L8` | Quy trình phá hệ thống có dự đoán trước khi chạy cho S1 và S3. |

## 8. Tự kiểm tra
1. *Câu hỏi:* Trong thư mục `checkpoints/ingest_bronze/`, cặp thư mục `offsets/` và `commits/` giúp Spark phát hiện batch dang dở khi crash như thế nào?  
   *Đáp án:* Spark ghi file dự kiến offset vào `offsets/<batch_id>` **trước** khi xử lý và chỉ ghi `commits/<batch_id>` **sau** khi sink ghi xong; khi khởi động lại, batch nào có mặt trong `offsets/` mà chưa có trong `commits/` sẽ được phát lại.
2. *Câu hỏi:* Vì sao kịch bản S3 (xoá checkpoint Q2) an toàn nhưng xoá checkpoint Q1 lại nguy hiểm?  
   *Đáp án:* Q2 ghi xuống Silver bằng `MERGE` theo `event_id` nên phát lại từ đầu vẫn ra kết quả như cũ; còn Q1 ghi append-only xuống Bronze nên đọc lại từ đầu Kafka sẽ cộng thêm dòng vào Bronze (và reset `batch_id` về 0).
3. *Câu hỏi:* Nếu ổ D (`DATA_ROOT`) đầy trong lúc hệ thống đang chạy, cần kiểm tra bằng công cụ nào trong repo?  
   *Đáp án:* Chạy `make disk-report` để kiểm tra dung lượng trống ổ C/D, kích thước `DATA_ROOT` và số file Delta.
