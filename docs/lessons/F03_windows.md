# F03 — Cửa sổ Thời gian (Time Windows) & Chống Trùng Theo Cửa Sổ

## 1. Câu hỏi gốc
Trên một dòng sự kiện chảy vô tận, muốn gom nhóm theo từng khung giờ hoặc biết *"sự kiện này đã xuất hiện trong cửa sổ thời gian gần đây chưa"* mà không phải giữ dữ liệu mãi mãi trong RAM thì chia thời gian thế nào?

## 2. Trực giác
Hãy tưởng tượng bạn đếm xe qua trạm thu phí bằng các cuốn sổ chia theo từng khung giờ (`10:00–11:00`, `11:00–12:00`). Thay vì cố nhớ toàn bộ xe từ đầu năm đến giờ, bạn chỉ giữ cuốn sổ của khung giờ hiện tại và khung giờ ngay trước đó (cửa sổ trượt theo watermark). Với khử trùng luồng, Spark dùng một "cửa sổ nhớ động" (`dropDuplicatesWithinWatermark`) chỉ nhớ các `event_id` còn nằm trong ngưỡng watermark `10 minutes`.

## 3. Cơ chế & Phân loại
1. **Tumbling Window (Cửa sổ cố định không chồng lấn):** Chia trục `event_time` thành các ô đều nhau (ví dụ mỗi giờ `10:00-11:00`, `11:00-12:00`). Trong dự án này, tổng hợp theo giờ (`cum_spend` từng giờ) được tính ở tầng Gold bằng DuckDB/dbt, còn tầng streaming dùng cửa sổ cho khử trùng.
2. **Sliding Window (Cửa sổ trượt):** Có độ dài `W` và bước trượt `S` (`S < W`), hữu ích cho bài toán phát hiện gian lận click dồn dập trong 5 phút trượt mỗi 1 phút.
3. **Watermark-bounded Deduplication Window (Cửa sổ khử trùng ở Q2 Refine):**
   - `withWatermark("event_time", "10 minutes")` + `dropDuplicatesWithinWatermark(["event_id"])` trong Spark `3.5.3` thiết lập một cửa sổ thời gian động tính lùi `10 phút` từ mốc `max_event_time`.
   - Mọi bản ghi trùng `event_id` rơi trong cửa sổ 10 phút bị loại ngay tại tầng state; các bản ghi trùng đến trễ hơn (sau khi state đã dọn) được lớp `MERGE` toàn cục trên Delta Silver chặn lại.

## 4. Ví dụ số
- Giả sử `click` có `event_id = "evt-1"` phát lúc `10:01:00Z`, bản trùng gửi lại lúc `10:01:20Z` (trễ 20 giây < 10 phút):
  - Bản thứ 1 đi qua `dropDuplicatesWithinWatermark(["event_id"])` ➔ ghi vào Silver.
  - Bản thứ 2 rơi vào cùng cửa sổ watermark 10 phút ➔ bị loại ngay trong state store mà không cần quét bảng Delta Silver.
- Nếu Producer chạy lại vào ngày hôm sau với cùng `event_id = "evt-1"` (vượt quá cửa sổ 10 phút): state không còn nhớ `"evt-1"`, nhưng lệnh Delta `MERGE` (`t.event_id = s.event_id AND t.event_date = s.event_date`) sẽ phát hiện khóa đã tồn tại và bỏ qua.

## 5. Điểm mạnh / Điểm yếu / Khi nào không dùng
- **Điểm mạnh:** Giới hạn rõ ràng khoảng thời gian cần nhớ trong bộ nhớ, tránh OOM khi stream chạy dài ngày.
- **Điểm yếu:** Sự kiện đến trễ hơn độ rộng cửa sổ watermark sẽ bị loại ở bước stateful (xem ADR-006 ở Task 3.7).
- **Khi nào không dùng:** Khi không chấp nhận mất bất kỳ sự kiện trễ nào ở tầng Silver ➔ bỏ stateful dedup, chỉ dùng `MERGE` (phương án P3 trong ADR-006).

## 6. Hiểu lầm phổ biến
1. *Hiểu lầm:* `dropDuplicates(["event_id"])` không kèm watermark vẫn an toàn nếu dữ liệu ít.  
   *Sự thật:* Không có watermark, Spark phải giữ **toàn bộ `event_id` từ lúc khởi động mãi mãi** trong state store; chạy đủ lâu chắc chắn tràn RAM.

## 7. Trong repo này

| File:dòng | Symbol | Giải thích |
| :--- | :--- | :--- |
| `src/streaming/refine.py:260-286` | `start_refine_query` | Gắn `withWatermark("event_time", cfg.pipeline.streaming.watermark_delay)` (10 phút) và `dropDuplicatesWithinWatermark(["event_id"])`. |
| `config/pipeline.yaml:13` | `streaming.watermark_delay` | Khai báo tập trung độ trễ cửa sổ watermark `"10 minutes"`. |

## 8. Tự kiểm tra
1. *Câu hỏi:* Vì sao `src/streaming/refine.py` phải ép `event_time` từ chuỗi ISO-8601 sang `TIMESTAMP` ngay khi đọc Bronze stream?  
   *Đáp án:* Vì `withWatermark("event_time", "10 minutes")` trong Spark Structured Streaming bắt buộc cột thời gian sự kiện phải có kiểu `TimestampType` (không chấp nhận `StringType`).
2. *Câu hỏi:* Tại sao `dropDuplicatesWithinWatermark(["event_id"])` cần Spark `>= 3.5`?  
   *Đáp án:* Ở các bản Spark cũ hơn, muốn xóa state sau watermark phải đưa cả cột `event_time` vào khóa `dropDuplicates(["event_id", "event_time"])`, khiến hai bản trùng khác `event_time` nhẹ không khử được; từ Spark 3.5+, `dropDuplicatesWithinWatermark(["event_id"])` khử trùng thuần theo `event_id` mà vẫn tự dọn state theo watermark.
