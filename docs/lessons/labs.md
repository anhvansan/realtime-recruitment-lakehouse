# Labs: Học bằng cách Phá Hệ Thống

Quy trình chuẩn cho mọi thí nghiệm: **Dự đoán trước khi chạy** ➔ **Thực thi phá hệ thống** ➔ **Ghi nhận quan sát thực tế** ➔ **Giải thích bằng nguyên lý nền tảng**.

---

## Lab L2 — Dừng Producer giữa chừng & Chạy lại lệch `--start-row` (Task 2.7 / F06)

- **Nguyên lý liên quan:** **F06** (At-least-once vs Idempotency, vì sao `enable.idempotence` của Kafka Producer không chống được việc chạy lại script), **F12** (Bronze vs Silver dedup).

### 1. Dự đoán trước khi chạy
1. Khi chạy Replay Producer phát một phần dữ liệu từ `--start-row 79` rồi bấm `Ctrl+C` (ví dụ dừng sau khi đã gửi ~50–100 dòng), các message đã ack đều nằm bền vững trên partition của Redpanda.
2. Khi khởi động lại Producer với `--start-row` lùi lại (ví dụ `--start-row 75 --limit 40`), những dòng giao nhau (từ row 79 trở đi) sẽ được cấp **cùng `event_id` SHA-1 tất định** (`sha1(f"{source_file}:{source_row_id}")`), nhưng bị Redpanda nhận **làm một message mới ở offset cao hơn**!
3. Vì sao cờ `enable.idempotence=True` của `confluent-kafka` **không** chặn được bản trùng này? Bởi vì Producer ID (PID) và sequence number trong giao thức Kafka chỉ tồn tại trong bộ nhớ của **một phiên producer** để chống trùng khi rớt gói tin mạng retry; khi chạy một tiến trình mới (`--start-row` lệch), Redpanda cấp PID mới nên chấp nhận ghi thêm bản sao vào topic `cpc_events_raw`.
4. **Hệ quả kiến trúc:** Lớp Bronze (append-only) sẽ ghi nhận cả 2 bản sao có cùng `event_id` nhưng khác `kafka_offset`; do đó tầng Silver (Task 3.4) **bắt buộc** phải có `dropDuplicatesWithinWatermark` + `MERGE WHEN NOT MATCHED ON event_id` để khử trùng toàn cục và bảo toàn `SUM(spend)` không bị nhân đôi.

### 2. Quan sát thực tế
- **Lệnh đã chạy:**
  1. `python src/producer/replay_producer.py --no-faults --speed-factor 600 --start-row 79 --run-id lab_l2_part1` (dừng bằng `Ctrl+C` sau 20.77s)
     - Kết quả: `STOPPED BY SIGINT` | `Rows read: 13` | `Produced / Acked: 12 / 12` | `Expected spend: 5.0000` (tương ứng rows 79..90).
  2. `python src/producer/replay_producer.py --no-faults --speed-factor 600 --max-sleep-sec 0.01 --start-row 75 --limit 25 --run-id lab_l2_part2`
     - Kết quả: `COMPLETED` | `Rows read: 25` | `Produced / Acked: 25 / 25` | `Expected spend: 11.0000` (tương ứng rows 75..99).
- **Số message tăng thêm trong topic `cpc_events_raw`:** Tổng cộng `12 + 25 = 37` message được ghi mới, trong đó toàn bộ **12 message ở đoạn giao nhau (rows 79..90)** bị ghi lặp lại ở các offset cao hơn với cùng `event_id` SHA-1 tất định (cộng dồn thành `3,994 + 37 = 4,031` message trên topic).

### 3. Giải thích & Kiểm chứng dự đoán
- **Tất cả 4 dự đoán ở mục 1 đều khớp 100%:** dù `enable.idempotence=True` được bật trong `build_producer()`, cờ này chỉ bảo vệ chống gửi trùng trong phạm vi **một phiên PID duy nhất** khi mạng rớt và client tự retry.
- Khi tiến trình `lab_l2_part1` bị ngắt bởi `SIGINT` và một tiến trình mới (`lab_l2_part2`) được khởi chạy lại với `--start-row 75`, broker Redpanda cấp một Producer ID mới tinh nên chấp nhận ghi toàn bộ 12 bản ghi giao nhau như các bản ghi hợp lệ mới ở offset tiếp theo.
- **Kết luận cho Epic 3:** Tầng Bronze (append-only) sẽ lưu cả hai bản sao; tầng Silver (Task 3.4) **bắt buộc** phải dùng `dropDuplicatesWithinWatermark` kết hợp `Delta MERGE WHEN NOT MATCHED ON event_id` để `SUM(spend)` không bị cộng đúp.

---

## Lab L1 — `docker kill` Spark giữa lúc đang chạy (Task 3.6 S1 / F06, F16)

- **Nguyên lý liên quan:** **F06** (At-least-once checkpoint + Idempotent Sink), **F16** (Crash recovery SLA `<= 60s`).

### 1. Dự đoán trước khi chạy
1. Khi chạy `make stream` (`run_all.py`) trong lúc Replay Producer đang đẩy dữ liệu vào `cpc_events_raw`, lệnh `docker kill cpc-spark` gửi tín hiệu `SIGKILL` cắt ngang tiến trình JVM ngay lập tức (không kịp chạy `finally` hay `query.stop()`).
2. Khi khởi động lại bằng `make spark-up && make stream`, Spark đọc lại `checkpoints/ingest_bronze/` và `checkpoints/refine_silver/` trên MinIO: mọi micro-batch đã ghi `offsets/<id>` nhưng chưa kịp ghi `commits/<id>` sẽ được **chạy lại tự động**.
3. Nhờ `txnAppId` + `txnVersion` ở Q1 và Delta `MERGE` ở Q2, dữ liệu không bị mất và không bị nhân đôi; tổng thời gian từ lúc bật lại `make spark-up` đến khi Q1/Q2 chạy bù xong kỳ vọng **`<= 60s`**, và `make reconcile` trả **`PASS`**.

### 2. Quan sát thực tế
- Chạy `make stream` (Q1 + Q2 trong 1 JVM) song song với Producer `replay_s1_kill` (`speed_factor=300x`, chạy trong `33.73s`).
- Giữa lúc Q1 đang xử lý, chạy `docker kill cpc-spark`: tiến trình Spark bị cắt ngang ngay khi Q1 mới xử lý xong phần đầu (`batch_id=0` ➔ khi bật lại Q1 nối tiếp từ `batch_id=1`).
- Chạy lại `make spark-up` (`0.5s`) + `run_all.py --available-now`: SparkSession + Q1 hoạt động lại sau `~17s`, chạy bù `batch_id=1` (`valid=214, invalid=2`) và `batch_id=2` (`valid=1772, invalid=17`), rồi Q2 khôi phục state v1 từ MinIO và xử lý `batch_id=1` (`valid_deduped=1958`), tổng thời gian drain toàn bộ là `195s`.
- `make reconcile MANIFEST=/app/data/manifests/replay_s1_kill.json`: **`PASS`** (exit `0`), `Bronze=1986`, `Silver=1958`, `DLQ=19`, `SUM(spend)=1589.5000`.

### 3. Giải thích & Kiểm chứng dự đoán
- Khớp 100% dự đoán: dù `docker kill` cắt ngang không kịp chạy `finally`, checkpoint trên MinIO vẫn bảo toàn chính xác ranh giới `offsets/` và `commits/` giúp Q1 tiếp tục đúng từ `batch_id=1` và Q2 khôi phục state `HDFSBackedStateStoreProvider` (`version 1`) từ S3.
- Nhờ `txnAppId`/`txnVersion` ở Bronze/DLQ và `MERGE` ở Silver, không có bản ghi nào bị mất hoặc bị tính trùng tiền.

---

## Lab L8 — Xoá checkpoint Q2 và dựng lại Silver từ Bronze (Task 3.6 S3 / F05, F06, F16)

- **Nguyên lý liên quan:** **F05** (State checkpoint), **F06** (Global `MERGE` idempotency), **F16** (Failure recovery).

### 1. Dự đoán trước khi chạy
1. Khi xoá toàn bộ thư mục `checkpoints/refine_silver/` trên MinIO (giữ nguyên bảng Bronze và bảng Silver hiện có) rồi chạy lại Q2 (`python3 -m src.streaming.refine --available-now`), Q2 mất trí nhớ vị trí commit đã đọc trong Bronze nên sẽ **quét lại toàn bộ Bronze từ commit 0**.
2. Tuy nhiên, nhờ `upsert_silver()` thực hiện Delta `MERGE` insert-only với khóa `t.event_id = s.event_id AND t.event_date = s.event_date`, toàn bộ các bản ghi đã có trong Silver đều khớp nhánh `MATCHED` và **bị bỏ qua (0 dòng mới được chèn)**.
3. Kết quả đối soát `make reconcile` sau khi chạy lại phải vẫn là **`PASS`** với `silver_row_count` và `SUM(spend)` giữ nguyên tuyệt đối.

### 2. Quan sát thực tế
- Xoá `14` object trong `checkpoints/refine_silver/` trên MinIO (giữ nguyên Bronze `1.986` dòng và Silver `1.958` dòng của đợt `s2_catchup`).
- Chạy lại `python3 -m src.streaming.refine --available-now`: log ghi nhận `Q2 refine batch_id=0 valid_deduped=1958 invalid=0` (Q2 đã đọc lại toàn bộ Bronze từ đầu).
- Chạy `make reconcile MANIFEST=/app/data/manifests/replay_s2_catchup.json`: **`PASS`** (exit `0`), `silver_row_count=1958 == silver_distinct_event_ids=1958`, `actual_silver_spend="1589.5000"`.

### 3. Giải thích & Kiểm chứng dự đoán
- Khớp 100% dự đoán: khi mất checkpoint Q2, Spark mất state trong bộ nhớ nên đọc lại toàn bộ Bronze, nhưng vì Silver sử dụng Delta `MERGE` insert-only theo `(event_id, event_date)` nên mọi bản ghi đã tồn tại đều bị bỏ qua an toàn — chứng minh lý do S3 an toàn tuyệt đối (khác với việc xoá checkpoint Q1 trên sink append-only).
