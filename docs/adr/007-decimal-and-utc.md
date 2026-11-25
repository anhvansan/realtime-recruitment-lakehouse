# ADR-007 — Biểu diễn Tiền tệ dạng DECIMAL (JSON String) và Thời gian UTC ISO-8601

- **Trạng thái:** Accepted
- **Ngày:** 2026-11-25
- **Tác giả:** Chủ dự án & AI Assistant
- **Liên quan:** Task 2.2, Task 3.3, Task 3.5 (Reconciliation), F13 (Data Contract)

---

## 1. Bối cảnh

Hệ thống CPC AdTech Lakehouse xử lý trực tiếp dữ liệu tài chính (`bid`, `spend`, `daily_budget`) và các mốc thời gian sự kiện (`event_time`, `produced_at`, `processed_at`) đi qua nhiều engine có kiến trúc khác nhau:
`Python Replay Producer` ➔ `JSON / Redpanda` ➔ `PySpark Structured Streaming` ➔ `Delta Lake` ➔ `DuckDB / dbt`.

Hai rủi ro kỹ thuật nghiêm trọng thường gặp khi truyền dữ liệu qua biên giới các ngôn ngữ:
1. **Sai số dấu phẩy động (IEEE 754 Floating-Point Drift):** Trong JSON, kiểu `number` thường được parser ánh xạ sang IEEE 754 `double`/`float64`. Các giá trị tiền tệ như `0.1 + 0.2 = 0.30000000000000004` làm sai lệch kết quả đối soát tài chính (`SUM(spend)` ở Task 3.5 giữa manifest Python `Decimal` và Silver Delta `DECIMAL(18,4)`).
2. **Múi giờ ngầm định (Implicit Timezone Ambiguity):** Dữ liệu CSV có thể không đính kèm offset, trong khi máy host Windows ở UTC+7 (`Asia/Ho_Chi_Minh`) còn container Linux Spark chạy mặc định ở UTC (hoặc ngược lại). Nếu truyền timestamp không kèm múi giờ tường minh, phép chia giỏ theo giờ (`date_trunc('hour', event_time)`) tính sai `cum_spend` và `pacing_ratio`.

---

## 2. Các phương án

### Về biểu diễn tiền tệ (`bid`, `spend`) trong JSON
- **Phương án A (Chọn): Lưu/tính bằng `DECIMAL(18,4)` ở tầng dữ liệu và serialize thành `string` có dấu thập phân trong JSON (ví dụ `"12.5000"`).**
  - *Ưu điểm:* Bảo toàn chính xác tuyệt đối từng chữ số thập phân từ `Decimal` (Python) ➔ JSON wire ➔ `DecimalType(18,4)` (Spark/Delta) ➔ `DECIMAL(18,4)` (DuckDB); JSON Schema và mắt người đọc dễ hiểu trực tiếp bằng USD.
  - *Nhược điểm:* Tốn thêm vài byte chuỗi trong JSON so với số nguyên (không đáng kể khi bật nén `lz4` ở Kafka).
- **Phương án B: Truyền số thực JSON `number` (`12.5`).**
  - *Ưu điểm:* Gọn hơn vài ký tự nháy kép.
  - *Nhược điểm:* Bị `json.loads` / `from_json` parse qua float64 nếu sơ suất; gây lệch làm tròn nhị phân khi tổng hợp hàng trăm nghìn sự kiện.
- **Phương án C: Quy đổi sang số nguyên nhỏ nhất (integer micros / sub-cents).**
  - *Ưu điểm:* Chính xác cho số nguyên.
  - *Nhược điểm:* Dễ gây nhầm lẫn đơn vị (`125000` vs `$12.50` hay `$1.25`) khi đọc log thô Bronze hoặc viết truy vấn ad-hoc trên DuckDB.

### Về chuẩn hoá thời gian
- **Phương án A (Chọn): Chuẩn hoá toàn bộ thời gian sự kiện và hệ thống sang UTC, truyền qua JSON bằng chuỗi ISO-8601 có hậu tố múi giờ tường minh (`Z` hoặc `+00:00`).**
  - *Ưu điểm:* Nhất quán giữa Python (`datetime.now(UTC)`), Spark (`spark.sql.session.timeZone=UTC`), Delta (`TIMESTAMP`), và DuckDB; miễn nhiễm hoàn toàn với chênh lệch múi giờ giữa Windows host (+07) và Docker container (UTC).
  - *Nhược điểm:* Lớp hiển thị UI cần chuyển múi giờ nếu người dùng cuối yêu cầu giờ địa phương (không cần trong phạm vi v2 vì mọi giờ dashboard đều quy chuẩn UTC theo `sim_now`).

---

## 3. Bằng chứng

- Hợp đồng JSON Schema [cpc_event.v1.json](file:///d:/realtime-recruitment-lakehouse/src/common/schemas/cpc_event.v1.json) đặt `"type": "string"` và `"pattern": "^\\d{1,14}(\\.\\d{1,4})?$"` cho `bid` và `spend`, cùng regex ISO-8601 UTC cho `event_time` và `produced_at`.
- Bộ kiểm chuẩn [tests/unit/test_contract.py](file:///d:/realtime-recruitment-lakehouse/tests/unit/test_contract.py) kiểm chứng việc từ chối `float`/`int` JSON (lỗi `malformed_decimal`) và từ chối timestamp thiếu `Z`/`+00:00` hoặc lệch múi giờ `+07:00` (lỗi `bad_timestamp`).
- Smoke test Task 3.1 ([docs/progress/3.1.md](file:///d:/realtime-recruitment-lakehouse/docs/progress/3.1.md)) xác nhận `SUM(spend)=23.0000` (`Decimal` chính xác tuyệt đối) khi Spark ghi Delta `DecimalType(18,4)` và DuckDB đọc qua `delta_scan()`.

---

## 4. Quyết định

1. Toàn bộ giá trị tiền tệ (`bid`, `spend`, `daily_budget`) sử dụng **`DECIMAL(18,4)`** trong Python (`decimal.Decimal`), PySpark/Delta (`DecimalType(18,4)`), và DuckDB (`DECIMAL(18,4)`).
2. Khi serialize qua JSON trên Kafka/Redpanda (`cpc_events_raw`), `bid` và `spend` **bắt buộc truyền dạng chuỗi (`string`)**; mọi message gửi kiểu số JSON (`float`/`int`) sẽ bị từ chối với mã lỗi `malformed_decimal` và đưa vào DLQ.
3. Toàn bộ trường thời gian (`event_time`, `produced_at`, `ingested_at`, `processed_at`) thống nhất múi giờ **UTC**, biểu diễn trên dây mạng bằng chuỗi ISO-8601 kết thúc bởi `Z` hoặc `+00:00`.

---

## 5. Hệ quả

- **Được:**
  - Sai số làm tròn tài chính = **0**; cho phép đối soát `SUM(spend)` giữa Manifest, Silver Delta và Gold dbt khớp tuyệt đối tới `0.0001`.
  - Loại bỏ hoàn toàn lỗi lệch ngày partition (`event_date`) và giờ pacing giữa máy dev (+07) và Docker container (UTC).
- **Đánh đổi:**
  - Mọi thành phần producer và test fixture phải chủ động format số tiền sang string (`f"{val:.4f}"`) và gắn `tzinfo=UTC` thay vì đẩy trực tiếp float hoặc naive datetime.

---

## 6. Điều kiện xem xét lại

- Nếu ngành hàng mở rộng sang micro-bidding cần độ chi tiết nhỏ hơn `0.0001` USD (ví dụ `1e-6`), nâng độ chính xác lên `DECIMAL(18,6)` và tăng phiên bản hợp đồng sang `v2`.
- Nếu sản phẩm yêu cầu hiển thị Dashboard theo nhiều múi giờ người dùng, bổ sung cột hiển thị `timezone` ở lớp Presentation UI trong khi vẫn giữ toàn bộ tầng lưu trữ Lakehouse là UTC.
