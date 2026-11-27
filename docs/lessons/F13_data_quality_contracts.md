# F13 — Chất lượng Dữ liệu, Data Contract và Xử lý DLQ (Dead Letter Queue)

## 1. Câu hỏi gốc
Làm sao các nhà phân tích tài chính và quản lý chiến dịch có thể tin tưởng 100% vào các con số báo cáo trên Dashboard khi luồng dữ liệu realtime liên tục nhận hàng triệu message từ nhiều nguồn đối tác khác nhau mà không bị ngưng trệ toàn bộ pipeline khi gặp bản ghi rác?

## 2. Trực giác
Hãy tưởng tượng hệ thống kiểm soát dữ liệu giống như cửa soi an ninh tại sân bay:
- **Data Contract:** Là quy định hành lý được in rõ trên vé máy bay (trọng lượng, chất cấm, giấy tờ hợp lệ). Ai tuân thủ thì được đi tiếp.
- **Shift-left Validation:** Hành khách bị kiểm tra giấy tờ ngay từ cửa vào sân bay (tầng Ingest) chứ không đợi đến tận lúc lên máy bay (tầng Gold) mới kiểm tra.
- **Dead Letter Queue (DLQ):** Nếu hành lý có vật dụng nghi vấn, bảo vệ sẽ chuyển hành lý đó sang phòng kiểm tra riêng biệt (DLQ) để xử lý mà không bắt toàn bộ hàng dài người phía sau phải dừng chuyến bay.

## 3. Cơ chế
Quy trình bảo đảm chất lượng dữ liệu và cách ly lỗi trong kiến trúc streaming:

```
[Incoming Raw Message]
          │
          ▼
  [Data Contract Check] ──── (JSON Schema v1, Decimal Format, Enum event_type)
          │
          ├─► Hợp lệ ──────► [Bronze Delta Table] ──► [Silver Stream Dedup]
          │
          └─► Vi phạm ─────► [Dead Letter Queue (DLQ)]
                                ├── Topic: cpc_events_dlq
                                └── Delta Table: dlq/cpc_events_dlq
                                      ├── payload_raw
                                      ├── error_type (corrupt_json, bad_enum, negative_amount...)
                                      ├── error_detail
                                      └── failed_at
```

**Các chiều kích thước chất lượng dữ liệu (Data Quality Dimensions):**
1. **Tính đầy đủ (Completeness):** Không thiếu các khóa ngoại cốt lõi (`campaign_id`, `job_id`, `publisher_id`).
2. **Tính hợp lệ (Validity):** Tiền không âm (`spend >= 0`), ngày giờ đúng chuẩn ISO-8601 UTC.
3. **Tính duy nhất (Uniqueness):** Không có hai bản ghi trùng `event_id` trong tầng Silver.
4. **Tính đúng hạn (Freshness):** Dữ liệu đến trong phạm vi watermark cho phép.
5. **Tính nhất quán (Consistency):** Tổng spend ở Silver phải bằng tổng thầu của các click hợp lệ.

## 4. Ví dụ số
- Producer cố tình bơm một sự kiện lỗi: `{"event_id": "abc", "event_type": "click", "spend": "-5.0000"}`.
- Nếu không có Data Contract: Hệ thống nạp vào Silver, làm `cum_spend` bị giảm âm thầm 5 USD ➔ Tính sai pacing.
- Khi có Data Contract (Shift-Left):
  - Bộ kiểm chuẩn phát hiện `spend < 0` ➔ Đánh dấu lỗi `negative_amount`.
  - Định tuyến ngay lập tức sang bảng `dlq/cpc_events_dlq`.
  - Pipeline tiếp tục xử lý các event sạch tiếp theo mà không bị crash.

## 5. Điểm mạnh / Điểm yếu / Khi nào không dùng
- **Điểm mạnh:** Bảo vệ các bảng báo cáo tài chính khỏi dữ liệu bẩn; minh bạch hóa trách nhiệm giữa bên phát sinh sự kiện và bên tiêu thụ; cô lập lỗi tự động.
- **Điểm yếu:** Đòi hỏi duy trì và phiên bản hóa schema chặt chẽ; chi phí lưu trữ thêm cho bảng DLQ.
- **Khi nào không dùng:** Các hệ thống thu thập log phi cấu trúc chỉ phục vụ tìm kiếm từ khóa tự do (unstructured text search).

## 6. Hiểu lầm phổ biến
1. *Hiểu lầm:* Chỉ cần viết `dbt test` ở tầng Gold là đủ đảm bảo chất lượng dữ liệu.  
   *Sự thật:* Khi dữ liệu bẩn đã vào Bronze/Silver, việc test ở Gold là quá muộn (shift-right), lỗi đã làm sai lệch trạng thái streaming stateful.
2. *Hiểu lầm:* DLQ chỉ cần in log ra màn hình console.  
   *Sự thật:* Bản ghi DLQ phải được lưu trữ có cấu trúc để phục vụ đối soát, cảnh báo tỷ lệ lỗi (`dlq_rate`) và replay sửa lỗi sau này.

## 7. Trong repo này

| File:dòng | Symbol | Đoạn code | Giải thích |
| :--- | :--- | :--- | :--- |
| `docs/data_dictionary.md:100-108` | `Event Type Mapping` | Quy tắc chuẩn hoá phễu | Ánh xạ từ dữ liệu CSV gốc sang 4 loại sự kiện chuẩn (bỏ `impression`). |
| `src/common/schemas/cpc_event.v1.json:1-97` | JSON Schema `v1` | Draft 2020-12 Contract | Bắt buộc `bid`/`spend` dạng chuỗi thập phân và điều kiện `if/then` khi `event_type == 'click'`. |
| `src/common/schemas/cpc_event.py:231-443` | `validate_event` | Bộ validator thuần Python | Phân loại chính xác 7 `error_type` trước khi dựng model `CpcEventV1`. |
| `src/common/schemas/validation_rules.md:1-85` | Spec Parity | Tài liệu quy chuẩn 7 mã lỗi | Làm căn cứ viết `validate_df()` bằng PySpark và test chéo ở Task 3.3. |
| `src/producer/fault_injector.py:149-296` | `FaultInjector` | Bơm lỗi có seed & Manifest | Bơm 2% trùng, 2% trễ, 1% corrupt chia 6 loại lỗi và ghi manifest loại trừ corrupt khỏi `expected_spend_total`. |
| `src/streaming/validation.py:132-304` | `validate_df` | Bộ validator PySpark thuần SQL | Đối chiếu 7 `error_type` (kể cả kiểm tra chuỗi JSON gốc bắt float `12.5`), tách `valid_df` / `invalid_df` không cần Python UDF. |
| `tests/unit/test_validation_parity.py:212-217` | `test_python_and_spark_validation_parity` | Test chéo Python vs Spark | Chạy cùng tập fixture qua `validate_event()` và `validate_df()`, khẳng định `is_valid` và `error_type` khớp 100%. |

```markdown
| Nguồn phát sinh | Điều kiện logic trên dữ liệu gốc | `event_type` chuẩn | Giá trị `bid` | Giá trị `spend` |
| :--- | :--- | :--- | :--- | :--- |
| `tracking.csv` | `ev = 2` AND `custom_track = 'click'` | **`click`** | Giá trị `bid` gốc | `= bid` |
| `application.csv` | Bản ghi tạo mới (`status IN (0, 1)`) | **`apply`** | `"0.0000"` | `"0.0000"` |
| `application.csv` | Bản ghi có `status = 2` HOẶC `is_match = 1` | **`qualified`** | `"0.0000"` | `"0.0000"` |
| `application.csv` | Bản ghi có `is_rejected = 1` HOẶC `score = 0` | **`unqualified`** | `"0.0000"` | `"0.0000"` |
```

## 8. Tự kiểm tra
1. *Câu hỏi:* Nguyên tắc "Shift-Left" trong kiểm soát chất lượng dữ liệu nghĩa là gì?  
   *Đáp án:* Di chuyển các bước kiểm tra và phát hiện lỗi càng gần nguồn phát sinh dữ liệu càng tốt (ngay tại Ingestion/Producer) thay vì đợi đến tầng cuối cùng mới kiểm tra.
2. *Câu hỏi:* Tại sao trong JSON Data Contract, các trường tiền tệ (`bid`, `spend`) bắt buộc phải serialize dạng chuỗi (`string`)?  
   *Đáp án:* Để tránh sai số làm tròn số thực dấu phẩy động (float IEEE 754) khi truyền tải qua mạng trước khi được ép kiểu sang `DECIMAL(18,4)`.
3. *Câu hỏi:* Một bản ghi có `event_type = 'unknown_action'` sẽ được hệ thống xử lý như thế nào?  
   *Đáp án:* Bị từ chối bởi bộ validate với mã lỗi `bad_enum` và chuyển hướng lưu vào topic/bảng Delta `dlq`.

## 9. Đọc thêm
- What is a Data Contract: https://greatexpectations.io/blog/what-is-a-data-contract/
- Enterprise Integration Patterns - Dead Letter Channel: https://www.enterpriseintegrationpatterns.com/patterns/messaging/DeadLetterChannel.html
