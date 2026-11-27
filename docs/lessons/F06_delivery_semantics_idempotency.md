# F06 — Bảo đảm Giao nhận và Tính Idempotency (Chống trùng & chống mất)

## 1. Câu hỏi gốc
Khi Replay Producer, Redpanda hoặc Spark Streaming bị tắt đột ngột giữa chừng hoặc mạng chập chờn gây gửi lại message, làm sao hệ thống đảm bảo mỗi sự kiện `click` hợp lệ không bị mất (thiếu tiền) và không bị tính hai lần (nhân đôi tiền)?

## 2. Trực giác
Hãy tưởng tượng việc chuyển khoản ngân hàng:
- **At-most-once:** Gửi lệnh một lần rồi quên, rớt mạng thì mất tiền ➔ không chấp nhận được cho dữ liệu tài chính.
- **At-least-once:** Rớt mạng thì gửi lại đến khi nhận được xác nhận (`acks=all`). An toàn không mất, nhưng có nguy cơ cùng một giao dịch đến nơi hai lần.
- **Exactly-once đầu–cuối = At-least-once + Sink Idempotent:** Gửi kèm **mã giao dịch duy nhất (`event_id`)**. Nếu ngân hàng thấy `event_id` đã ghi sổ rồi thì bỏ qua một cách an toàn — thực hiện 1 lần hay 10 lần đều cho kết quả y hệt nhau.

## 3. Cơ chế trong Pipeline CPC Lakehouse

| Chặng xử lý | Cơ chế kỹ thuật | Mức bảo đảm |
| :--- | :--- | :--- |
| Data Contract (`validate_event`) | Sinh `event_id` tất định (SHA-1) và gắn `source_row_id` | Tiền đề để mọi chặng sau khử trùng |
| Producer ➔ Redpanda | `enable.idempotence=true`, `acks=all` | Chống trùng do retry nội bộ trong 1 phiên producer |
| Redpanda ➔ Spark (Q1) | Lưu Kafka offset vào `checkpoint/` trên MinIO | Không mất message (trong hạn retention 1 giờ) |
| Spark ➔ Bronze / DLQ Delta | `txnAppId` + `txnVersion=batch_id` | Idempotent theo micro-batch |
| Spark ➔ Silver Delta (Q2) | `dropDuplicatesWithinWatermark` + `MERGE` insert-only theo `event_id` | Idempotent toàn cục theo khoá `event_id` |
| Spark ➔ Topic `cpc_events_dlq` | Ghi Kafka | At-least-once (chấp nhận được với bản ghi lỗi) |

## 4. Ví dụ số
Giả sử một `click` trị giá `bid = "2.5000"` được gửi 2 lần với cùng `event_id = "a94a8fe5..."` (do Producer chạy lại hoặc Spark chạy lại batch chưa kịp commit checkpoint):
- **Tầng Bronze:** Lưu vết cả 2 lần nhận (append-only, trừ khi trùng đúng `batch_id` thì `txnVersion` chặn lại).
- **Tầng Silver (`MERGE WHEN NOT MATCHED`):** Chỉ chèn dòng đầu tiên; dòng thứ 2 trùng `event_id` bị bỏ qua ➔ Tổng `spend` luôn là **`2.5000`**, không bao giờ thành `5.0000`.

## 5. Điểm mạnh / Điểm yếu / Khi nào không dùng
- **Điểm mạnh:** Chịu lỗi crash và restart an toàn mà không cần cơ chế two-phase commit (2PC) phân tán đắt đỏ giữa các hệ thống khác nhau.
- **Điểm yếu:** Đòi hỏi mọi event phải mang định danh duy nhất `event_id` ngay từ Data Contract (Task 2.2) và tốn chi phí tra cứu chỉ mục khi `MERGE` ở Silver.
- **Khi nào không dùng:** Dữ liệu log thô chấp nhận trùng nhẹ hoặc đếm xấp xỉ không liên quan tiền bạc.

## 6. Hiểu lầm phổ biến
1. *Hiểu lầm:* Bật `enable.idempotence=True` trên Kafka Producer là hệ thống tự động chống trùng toàn bộ khi chạy lại script Python.  
   *Sự thật:* Cờ này chỉ chống trùng do gói tin bị retry trong **cùng một phiên producer**; nếu bạn chạy lại script gửi cùng dòng CSV với `event_id` cũ, producer vẫn phát một bản sao mới lên Kafka ➔ tầng Silver phải tự khử trùng bằng `event_id`.

## 7. Trong repo này

| File:dòng | Symbol | Giải thích |
| :--- | :--- | :--- |
| `src/common/schemas/cpc_event.py:77-118` | `CpcEventV1` | Quy định trường `event_id` (SHA-1) và `source_row_id` bắt buộc cho khử trùng & truy vết. |
| `src/common/schemas/cpc_event.v1.json:25-29` | `properties.event_id` | Khoá hợp đồng định danh duy nhất cho phép mọi chặng phía sau thực thi idempotency. |
| `src/producer/fault_injector.py:229-250` | `FaultInjector.process_event` | Bơm 2% message trùng (`event_id` giữ nguyên) sau 0–60s mô phỏng để kiểm chứng dedup downstream. |
| `src/streaming/ingest.py:86-139` | `make_process_batch` | Ghi Bronze & DLQ Delta idempotent với `txnAppId` + `txnVersion=batch_id`; ghi topic `cpc_events_dlq` at-least-once. |
| `src/streaming/refine.py:192-257` | `make_upsert_silver` | Khử trùng 2 lớp: trong batch (`dropDuplicates(["event_id"])`) + toàn cục qua Delta `MERGE` insert-only (`t.event_id = s.event_id AND t.event_date = s.event_date`). |
| `tests/unit/test_refine.py:187-220` | `_run_upsert_silver_idempotency_check` | Gọi lặp lại `upsert_silver` trên cùng micro-batch (kể cả replay `batch_id=0` và `batch_id=1`) chứng minh Silver & DLQ không nhân đôi dòng. |
| `docs/lessons/labs.md:9-23` | `Lab L2` | Thí nghiệm dừng producer và chạy lệch `--start-row` để chứng minh producer idempotency không thay thế Silver dedup. |

## 8. Tự kiểm tra
1. *Câu hỏi:* Vì sao Data Contract `cpc_event.v1` bắt buộc phải có `event_id` do Producer tính từ `(source_file, row_number)` thay vì để Spark sinh `uuid4()` ngẫu nhiên?  
   *Đáp án:* Nếu Spark sinh `uuid4()` ngẫu nhiên, cùng một dòng CSV bị gửi lại hai lần sẽ nhận hai `event_id` khác nhau ➔ `MERGE` ở Silver không thể nhận ra bản trùng.
2. *Câu hỏi:* Công thức của Exactly-once đầu–cuối là gì?  
   *Đáp án:* `At-least-once delivery` (không mất) + `Idempotent sink / operation` (ghi nhiều lần cùng kết quả như một lần).
