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
