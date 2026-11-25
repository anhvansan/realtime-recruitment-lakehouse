# Validation Rules Specification — `cpc_event.v1`

> **Nguồn sự thật dùng chung (Parity Contract):** Tài liệu này quy định chuẩn xác mọi quy tắc kiểm tra dữ liệu giữa hàm Python `validate_event(raw: str)` tại [cpc_event.py](file:///d:/realtime-recruitment-lakehouse/src/common/schemas/cpc_event.py) (Task 2.2) và hàm PySpark `validate_df(df)` tại `src/streaming/validation.py` (Task 3.3). Mọi bản ghi vi phạm đều bị định tuyến vào DLQ với `error_type` tương ứng.

---

## 1. Hợp đồng trường dữ liệu (`cpc_event.v1.json`)

| Trường | Kiểu Wire (JSON) | Kiểu Silver (Delta) | Bắt buộc | Ràng buộc chi tiết |
| :--- | :--- | :--- | :---: | :--- |
| `schema_version` | `string` | `STRING` | ✔ | Giá trị duy nhất `"v1"`. |
| `event_id` | `string` | `STRING` | ✔ | Chuỗi không rỗng, độ dài 1..64 (`sha1(f"{source_file}:{row_number}")`). |
| `event_time` | `string` | `TIMESTAMP` (UTC) | ✔ | Chuỗi ISO-8601 UTC kết thúc bằng `Z` hoặc `+00:00`. |
| `produced_at` | `string` | `TIMESTAMP` (UTC) | ✔ | Chuỗi ISO-8601 UTC kết thúc bằng `Z` hoặc `+00:00`. |
| `event_type` | `string` | `STRING` | ✔ | Enum 4 giá trị: `click`, `apply`, `qualified`, `unqualified` (bỏ `impression` theo Task 2.1). |
| `job_id` | `string` | `STRING` | ✔ | Chuỗi khác rỗng. |
| `campaign_id` | `string` | `STRING` | ✔ | Chuỗi khác rỗng (Kafka partition key). |
| `group_id` | `string \| null` | `STRING` | Tuỳ chọn | Có thể `null` (nhiều dòng `tracking.csv` không có `group_id`). |
| `publisher_id` | `string` | `STRING` | ✔ | Chuỗi khác rỗng. |
| `bid` | `string \| null` | `DECIMAL(18,4)` | ✔ với `click` | Tiền biểu diễn dạng chuỗi không âm (`^\d{1,14}(\.\d{1,4})?$`). Bắt buộc khác rỗng khi `event_type == 'click'`. |
| `spend` | `string` | `DECIMAL(18,4)` | ✔ | Tiền biểu diễn dạng chuỗi không âm. Theo ADR-009: `= bid` với `click`, `"0.0000"` với các loại khác. |
| `source_row_id` | `integer` | `BIGINT` | ✔ | Số nguyên không âm (`>= 0`). |

---

## 2. Danh mục 7 mã lỗi (`error_type`) và Quy tắc ưu tiên

### 2.1 `corrupt_json`
- **Điều kiện kích hoạt:**
  - `raw` không phải chuỗi, chuỗi rỗng/khoảng trắng, chứa hằng số JSON phi chuẩn (`NaN`, `Infinity`), hoặc `json.loads(raw)` phát sinh `JSONDecodeError`.
  - Kết quả parse JSON ở cấp cao nhất không phải `dict`/JSON Object (ví dụ `[1, 2]`, `"hello"`, `123`).
- **Hành động:** Dừng kiểm tra tiếp và trả về ngay `corrupt_json`.
- **Ví dụ hợp lệ:** `{"schema_version": "v1", ...}`
- **Ví dụ vi phạm:** `"{broken json"`, `"[1, 2, 3]"`

### 2.2 `schema_version_unsupported`
- **Điều kiện kích hoạt:** Trường `schema_version` có mặt (không null/rỗng) nhưng khác `"v1"` (kể cả số `1` hay chuỗi `"v2"`).
- *(Lưu ý: nếu thiếu `schema_version` hoặc `null`/rỗng ➔ báo lỗi `missing_required`.)*
- **Ví dụ hợp lệ biên:** `"schema_version": "v1"`
- **Ví dụ vi phạm:** `"schema_version": "v2"`, `"schema_version": 1`

### 2.3 `missing_required`
- **Điều kiện kích hoạt:**
  - Thiếu, `null`, hoặc chuỗi toàn khoảng trắng đối với bất kỳ trường bắt buộc nào: `schema_version`, `event_id`, `event_time`, `produced_at`, `event_type`, `job_id`, `campaign_id`, `publisher_id`, `spend`, `source_row_id`.
  - `event_type == "click"` nhưng thiếu `bid`, `bid` là `null` hoặc chuỗi rỗng.
  - Kiểu dữ liệu cấu trúc sai (vd `source_row_id` không phải `integer`, `event_id` dài > 64 ký tự, hoặc xuất hiện trường lạ ngoài schema).
- **Ví dụ hợp lệ biên:** `group_id: null` và `bid: null` đối với `event_type: "apply"`.
- **Ví dụ vi phạm:** `event_type: "click"` kèm `"bid": null`; thiếu `campaign_id`; `"job_id": "   "`.

### 2.4 `bad_enum`
- **Điều kiện kích hoạt:** `event_type` không thuộc tập `{"click", "apply", "qualified", "unqualified"}`.
- **Ví dụ hợp lệ biên:** `"event_type": "unqualified"`, `"event_type": "qualified"`
- **Ví dụ vi phạm:** `"event_type": "impression"` (đã loại ở Task 2.1), `"event_type": "CLICK"` (phân biệt hoa thường), `"event_type": "bounce"`.

### 2.5 `bad_timestamp`
- **Điều kiện kích hoạt:** `event_time` hoặc `produced_at` không phải chuỗi ISO-8601 UTC hợp lệ (bắt buộc có hậu tố `Z` hoặc `+00:00` và ngày tháng hợp lệ theo lịch dương).
- **Ví dụ hợp lệ biên:** `"2026-01-01T00:00:00Z"`, `"2026-01-01T23:59:59.999999+00:00"`
- **Ví dụ vi phạm:** `"2026-01-01T07:00:00+07:00"` (sai múi giờ), `"2026-01-01T00:00:00"` (thiếu tzinfo), `"2026-02-30T12:00:00Z"` (ngày không tồn tại).

### 2.6 `malformed_decimal`
- **Điều kiện kích hoạt (trên `spend` hoặc `bid` khác `null`):**
  - Giá trị trong JSON **không phải kiểu `string`** (ví dụ truyền số thực `float` `12.5` hay số nguyên `int` `12` — vi phạm ADR-007).
  - Chuỗi không khớp định dạng số thập phân hợp lệ (`abc`, `"12.50$"`, `""`, `"NaN"`).
  - Vượt giới hạn `DECIMAL(18,4)`: có nhiều hơn **4 chữ số sau dấu phẩy** (vd `"1.12345"`) hoặc phần nguyên vượt quá 14 chữ số (`>= 10^14`).
- **Ví dụ hợp lệ biên:** `"0"`, `"0.0000"`, `"99999999999999.9999"`
- **Ví dụ vi phạm:** `12.5` (float JSON), `12` (int JSON), `"abc"`, `"1.23456"`.

### 2.7 `negative_amount`
- **Điều kiện kích hoạt:**
  - Giá trị `spend` hoặc `bid` là số thập phân âm (`< 0`, ví dụ `"-0.0001"`, `"-5.0000"`).
  - Giá trị `source_row_id < 0`.
- **Ví dụ hợp lệ biên:** `"spend": "0.0000"`, `"bid": "0.0000"`, `"source_row_id": 0`
- **Ví dụ vi phạm:** `"spend": "-0.0001"`, `"bid": "-1.5000"`, `"source_row_id": -1`.

---

## 3. Ghi chú đối chiếu PySpark (`validate_df` ở Task 3.3)

Khi cài đặt `src/streaming/validation.py` ở Task 3.3:
1. PySpark phải đọc `value` thành `STRING`, kiểm tra bằng biểu thức regex `r"^\d{1,14}(\.\d{1,4})?$"` trước khi ép kiểu `cast("decimal(18,4)")`, hoặc dùng `from_json` kết hợp kiểm tra chuỗi gốc để phân biệt chính xác `float` JSON (`12.5`) với chuỗi (`"12.5000"`).
2. Khi chạy bộ test chéo (parity test), cùng một tập JSON input phải cho kết luận `valid/invalid` và `error_type` đồng nhất từng dòng giữa `validate_event()` và `validate_df()`.
