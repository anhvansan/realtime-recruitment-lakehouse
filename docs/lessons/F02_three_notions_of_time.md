# F02 — Ba loại thời gian trong Hệ thống Streaming

## 1. Câu hỏi gốc
Khi một sự kiện vừa xảy ra trên thiết bị người dùng, được gửi qua mạng, rồi mới được Spark xử lý sau đó vài giây (hoặc vài phút nếu bị trễ mạng), "bây giờ là mấy giờ" của sự kiện đó được tính theo đồng hồ nào?

## 2. Trực giác
Hãy tưởng tượng tin nhắn gửi từ điện thoại khi đi qua hầm chui:
- Bạn bấm gửi lúc **10:00:00** (**Event Time** — thời điểm nghiệp vụ thực sự xảy ra).
- Điện thoại gửi được tới máy chủ lúc **10:07:00** (**Ingestion / Produced Time** — thời điểm vào đường ống).
- Hệ thống phân tích tính toán xong lúc **10:07:05** (**Processing Time** — đồng hồ của engine xử lý).

## 3. Cơ chế & Áp dụng trong Pipeline CPC

| Loại thời gian | Trường dữ liệu | Ai gán | Mục đích sử dụng |
| :--- | :--- | :--- | :--- |
| **Event Time** | `event_time` | Producer ánh xạ (`TimeMapper`) | Tính đúng nghiệp vụ: `cum_spend` theo giờ, cửa sổ trượt 60 phút, partition `event_date`. |
| **Ingestion / Emit Time** | `produced_at`, `kafka_timestamp` | Producer / Broker | Đo độ trễ hàng đợi, kiểm tra `behind_schedule_ms`. |
| **Processing Time** | `ingested_at`, `processed_at` | Spark Streaming | Đo freshness SLA của Dashboard và hiệu năng pipeline. |

**Công thức đồng hồ mô phỏng (`TimeMapper`):**
- $\text{sim\_time} = \text{SIM\_START} + (\text{orig\_time} - \text{orig\_t0})$
- $\text{target\_wall} = t_{\text{start}} + \dfrac{\text{orig\_time} - \text{orig\_t0}}{\text{speed\_factor}}$

## 4. Ví dụ số
Với dữ liệu gốc có `orig_t0 = 2022-07-07T03:00:00Z`, `SIM_START = 2026-01-01T00:00:00Z` và `speed_factor = 60`:
- Một sự kiện gốc xảy ra lúc `04:00:00Z` (sau 1 giờ):
  - `event_time` mô phỏng trong JSON là `2026-01-01T01:00:00Z`.
  - Deadline gửi ngoài đời thật là `t_start + 3600s / 60 = t_start + 60s` (**1 phút thật = 1 giờ dữ liệu**).
- Dùng **deadline tuyệt đối** (`target_wall_time`) thay cho `time.sleep(delta)` cố định giữa các gói giúp sai số không bị cộng dồn (clock drift) qua hàng nghìn message.

## 5. Điểm mạnh / Điểm yếu / Khi nào không dùng
- **Điểm mạnh (theo Event Time):** Kết quả kinh doanh (`spend` theo giờ, tỷ lệ click-to-apply) **tất định (deterministic)** và không bị thay đổi nếu hệ thống chậm hay replay lại.
- **Điểm yếu:** Đòi hỏi cơ chế Watermark và xử lý dữ liệu đến trễ (F04).
- **Khi nào không dùng:** Chỉ cần đếm log giám sát hạ tầng hệ thống theo thời gian thực của server ➔ dùng Processing Time là đủ.

## 6. Hiểu lầm phổ biến
1. *Hiểu lầm:* Có thể lấy giờ máy (`datetime.now()`) làm `event_time` để đỡ phải ánh xạ thời gian.  
   *Sự thật:* Khi tua nhanh 600× để mô phỏng cả ngày dữ liệu trong vài phút, dùng `datetime.now()` sẽ dồn cả 24 giờ kinh doanh vào chung **1 ô giờ duy nhất**, làm hỏng hoàn toàn biểu đồ Pacing (P1) và cửa sổ trượt 60 phút (P2)!

## 7. Trong repo này

| File:dòng | Symbol | Giải thích |
| :--- | :--- | :--- |
| `src/producer/time_mapper.py:30-58` | `TimeMapper` | Cài đặt công thức `map_sim_time`, `target_wall_time` và `behind_schedule_ms`. |
| `src/producer/replay_producer.py:335-366` | `build_event_payload` | Tách bạch rõ `event_time` (đồng hồ mô phỏng từ `TimeMapper`) và `produced_at` (wall clock UTC). |
| `tests/unit/test_time_mapper.py:13-33` | `test_time_mapper_60x_compression_numeric_example` | Kiểm chứng ví dụ số `speed_factor=60` ➔ 1 phút thật = 1 giờ dữ liệu. |

## 8. Tự kiểm tra
1. *Câu hỏi:* Vì sao Dashboard cần hiển thị `sim_now = MAX(event_time)` thay vì lấy giờ hệ thống?  
   *Đáp án:* Vì thời gian trong dữ liệu đang chạy ở đồng hồ mô phỏng (`2026-01-01...`) với tốc độ gấp nhiều lần thực tế; đối chiếu đường pacing lý tưởng phải so với `sim_now`.
2. *Câu hỏi:* Khi tính `behind_schedule_ms > 0`, đại lượng đó đang phản ánh điều gì?  
   *Đáp án:* Producer đang gửi chậm hơn deadline kỳ vọng (ví dụ do `speed_factor` quá lớn vượt ngưỡng CPU/mạng hoặc do giới hạn `--max-rate`).
