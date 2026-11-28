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

---

## Lab L3 — Thực nghiệm Watermark (5m / 10m / 20m) vs P3 MERGE-only (Task 3.7 / F04, F05)

- **Nguyên lý liên quan:** **F04** (Watermark & Late Data), **F05** (State store sizing vs eviction), **ADR-006**.

### 1. Dự đoán trước khi chạy
1. Khi chia Bronze (`ac_3_9`, gồm 35 event trễ 5–15 phút mô phỏng trong `late_ids.jsonl`) thành 40 micro-batch tuần tự theo thứ tự đến:
   - **P1 (`watermark = 5 minutes`):** Watermark dâng sát giờ hiện tại nên sẽ loại nhiều event trễ (kể cả phần lớn nhóm 5–15 phút), đổi lại số dòng và dung lượng **state nhỏ nhất**.
   - **P1 (`watermark = 10 minutes`):** Giữ lại nhóm trễ 5–10 phút, chủ yếu loại một phần nhóm trễ `> 10 phút`; state lớn hơn mức 5 phút nhưng vẫn bị giới hạn.
   - **P1 (`watermark = 20 minutes`):** Vì độ trễ tối đa do `FaultInjector` bơm là `15 phút < 20 phút`, kỳ vọng giữ lại gần như toàn bộ hoặc 100% event trễ (0 hoặc rất ít mất mát), đổi lại **state lớn nhất** trong 3 mức P1.
   - **P3 (Bỏ dedup stateful, chỉ dùng Delta `MERGE`):** Không loại bất kỳ event trễ nào (**0 mất dòng, `spend_loss = 0.0000`**) và **`max_state_rows = 0`**, đổi lại mọi batch đều phải chạy Delta `MERGE` đầy đủ xuống đĩa/S3.

### 2. Quan sát thực tế
- Chạy `scripts/bench/exp_watermark.py --manifest /app/data/manifests/replay_ac_3_9.json --num-batches 40` trên `1,986` bản ghi Bronze (`1,958` unique, `35` event trễ `5–15 phút`), đo bằng `MetricsListener`:
  - **`P1_wm_5m` (`5 minutes`):** Silver còn **`1,951` dòng**, loại **`7` event** (7/7 đều thuộc `late_ids.jsonl`, 0 mất bất ngờ), `spend_loss = 12.4000`, `max_state_rows = 242` (`78,568 bytes`), `avg_batch_ms = 5,026.3`, tổng thời gian `201.99s`.
  - **`P1_wm_10m` (`10 minutes`):** Silver đủ **`1,958` dòng**, loại **`0` event**, `spend_loss = 0.0000`, `max_state_rows = 339` (`97,144 bytes`), `avg_batch_ms = 4,788.2`, tổng thời gian `192.83s`.
  - **`P1_wm_20m` (`20 minutes`):** Silver đủ **`1,958` dòng**, loại **`0` event**, `spend_loss = 0.0000`, `max_state_rows = 421` (`116,120 bytes`), `avg_batch_ms = 4,928.5`, tổng thời gian `197.83s`.
  - **`P3_merge_only` (không watermark state):** Silver đủ **`1,958` dòng**, loại **`0` event**, `spend_loss = 0.0000`, **`max_state_rows = 0` (`0 bytes`)**, `avg_batch_ms = 4,119.8`, tổng thời gian **`165.17s`**.

### 3. Giải thích & Kiểm chứng dự đoán
- **Đúng như dự đoán về xu hướng state & mất dữ liệu:** Khi tăng watermark `5m ➔ 10m ➔ 20m`, `max_state_rows` tăng đều `242 ➔ 339 ➔ 421` (`78 KB ➔ 97 KB ➔ 116 KB`). Tại `5m`, watermark dâng quá sát làm rơi 7 bản ghi trễ (`spend_loss = 12.4000`), còn từ `10m` trở lên giữ đủ `1,958` dòng.
- **Điểm bất ngờ có giá trị từ số đo (R14):** Ở tập ~2k dòng, **`P3_merge_only` không chỉ tốn 0 byte state mà còn nhanh hơn P1 khoảng 14%** (`165.17s` vs `192.83s`) vì bỏ được chi phí shuffle stateful và ghi state snapshot mỗi micro-batch. Kết quả này là cơ sở để giữ P1 `10 minutes` làm mặc định và ghi rõ điều kiện chuyển sang P3/P2 trong **ADR-006** (`docs/adr/006-late-data.md`).

---

## Lab L4 — Small File Problem & Compaction an toàn (Task 3.8 / F10, T03)

- **Nguyên lý liên quan:** **F10** (`_delta_log`, `dataChange=false` trong `OPTIMIZE`), **T03** (`OPTIMIZE`, `VACUUM` an toàn & tránh xung đột `ConcurrentModification`).

### 1. Dự đoán trước khi chạy
1. Khi tạo bảng Delta phân mảnh thành ~240 file Parquet nhỏ trên MinIO (`_bench_small_files`), DuckDB `delta_scan()` phải gửi hàng trăm HTTP GET request tới MinIO ➔ độ trễ truy vấn tăng vọt dù tổng dữ liệu chỉ vài chục KB.
2. Sau khi chạy `compact_table()` (`OPTIMIZE`), số file giảm mạnh xuống còn vài file (hoặc 1 file), kích thước trung bình mỗi file tăng lên tương ứng, và thời gian truy vấn DuckDB giảm rõ rệt.
3. Khi chạy `make compact` trên `bronze/cpc_events` (khiến `_delta_log` ghi commit mới chứa các file đã nén với `dataChange=false`), chạy lại `refine.py --available-now` sau đó sẽ **không đọc lại các file đã nén** và không tạo bản ghi mới ở Silver (`make reconcile` vẫn **PASS**).

### 2. Quan sát thực tế
- Chạy `scripts/bench/bench_small_files.py --target-files 240 --queries 5` (tạo `960` dòng trải trên `156` file Parquet nhỏ ở MinIO):
  - **Trước `OPTIMIZE`:** `156` file (`1,092 bytes/file`, tổng `170,318 bytes`), DuckDB `delta_scan()` mất **`572.41 ms`** (median) và **`645.97 ms`** (p95).
  - **Sau `OPTIMIZE` (`10.02s`):** còn **`1` file** (`5,621 bytes`, giảm **`99.36%`** số file), DuckDB chỉ mất **`54.45 ms`** (median) và **`67.31 ms`** (p95) — nhanh hơn **`10.51x`**.
- Chạy `make compact` trên dữ liệu `ac_3_9`: Bronze giảm `39 ➔ 30` file (`7.04s`), Silver giảm `79 ➔ 30` file (`7.55s`); `make reconcile` vẫn **`PASS`** (`0`); chạy lại `refine.py --available-now` không phát sinh batch mới và giữ nguyên **`1,958`** dòng Silver.

### 3. Giải thích & Kiểm chứng dự đoán
- Khớp 100% dự đoán: khi bị phân mảnh thành `156` file nhỏ trên MinIO, phần lớn thời gian truy vấn của DuckDB bị đốt vào chi phí HTTP round-trip + đọc footer Parquet từng file; gộp về 1 file làm giảm độ trễ hơn **10 lần** và thu gọn dung lượng tới **~96.7%** (loại bỏ overhead header/footer lặp lại).
- Vì `OPTIMIZE` ghi vào `_delta_log` các file mới kèm cờ **`dataChange=false`**, luồng Structured Streaming Q2 (`readStream` từ Bronze) nhận biết đây chỉ là tái sắp xếp vật lý không đổi dữ liệu logic nên tự động bỏ qua, hoàn toàn không gây nhân đôi hay đọc lại Silver.
