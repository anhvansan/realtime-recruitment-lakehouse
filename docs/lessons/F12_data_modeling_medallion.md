# F12 — Mô hình hoá Dữ liệu và Kiến trúc Medallion (Bronze - Silver - Gold)

## 1. Câu hỏi gốc
Làm thế nào để tổ chức dữ liệu trong một hồ dữ liệu (Lakehouse) sao cho số liệu tài chính luôn chính xác, dễ truy vấn theo thời gian thực nhưng khi có sự cố logic hoặc dữ liệu sai lệch vẫn có thể sửa chữa và tái lập mà không làm hỏng toàn bộ hệ thống?

## 2. Trực giác
Hãy tưởng tượng kiến trúc Medallion giống như quy trình lọc nước sinh hoạt:
- **Tầng Bronze (Nước thô):** Nước được bơm trực tiếp từ sông hồ vào bể chứa lớn. Có cả phù sa, lá cây và rác. Tuyệt đối không uống được, nhưng đó là nguồn nước gốc không bao giờ cạn để nếu bộ lọc bị tắc thì ta vẫn còn nước để lọc lại.
- **Tầng Silver (Nước lọc sạch):** Nước đã qua hệ thống lắng lọc và khử trùng: loại bỏ rác (dedup), loại bỏ cặn bẩn (validate). Nước này đủ chuẩn để tắm giặt và cấp cho các vòi nước sinh hoạt (Dashboard realtime).
- **Tầng Gold (Nước tinh khiết đóng chai):** Nước được bổ sung khoáng chất, đóng chai đạt chuẩn y tế phục vụ cho các bữa tiệc cao cấp (báo cáo tài chính cho ban giám đốc, phân tích hiệu quả chuyên sâu).

## 3. Cơ chế
Kiến trúc phân tầng Medallion trong Lakehouse tách biệt rõ ràng trách nhiệm của từng lớp:

```
[Redpanda Streaming / CSV Raw]
              │
              ▼
    ┌───────────────────┐
    │  BRONZE LAYER     │ ──► Append-only Delta Table (partition by event_date)
    │  (Raw Audit Log)  │     Giữ nguyên payload thô + kafka_metadata (offset, timestamp)
    └───────────────────┘
              │
              ▼ (Streaming Dedup on event_id + Schema Enforcement)
    ┌───────────────────┐
    │  SILVER LAYER     │ ──► Cleaned & Deduplicated Delta Table
    │  (Conformed Fact) │     Mỗi event_id là duy nhất, tiền DECIMAL(18,4), thời gian UTC
    └───────────────────┘
              │
              ├─► Fast Path: DuckDB delta_scan() ➔ Dashboard (~10s)
              ▼
    ┌───────────────────┐
    │   GOLD LAYER      │ ──► Business Data Marts (External Parquet via dbt-duckdb)
    │  (Aggregates)     │     fct_campaign_hourly_pacing, fct_recruitment_efficiency
    └───────────────────┘
```

**Khái niệm then chốt:**
1. **Grain (Độ mịn dữ liệu):** Phải xác định một dòng đại diện cho cái gì trước khi viết SQL.
   - Bronze / Silver Fact: Một dòng = Một sự kiện click hoặc apply duy nhất (`event_id`).
   - Gold Mart Pacing: Một dòng = Một chiến dịch tại một khung giờ (`campaign_id` × `hour_ts`).
2. **Immutability (Bất biến) ở Bronze:** Bronze chỉ ghi tiếp (`append-only`), không bao giờ sửa hay xóa để bảo toàn lịch sử phục vụ replay.

## 4. Ví dụ số
- Producer phát 1,875 click sạch + 38 click trùng (2%) + 19 JSON hỏng (1%).
- **Tầng Bronze:** Lưu đủ `1,875 + 38 = 1,913` dòng hợp lệ. 19 JSON hỏng bị cách ly sang `dlq_events`.
- **Tầng Silver:** Sau khi chạy streaming dedup theo `event_id`, chỉ còn đúng **1,875 dòng duy nhất**.
- **Tầng Gold (`fct_campaign_hourly_pacing`):** Gom 1,875 dòng thành các bản ghi theo `campaign_id × hour`. Ví dụ Campaign 222 ở Giờ 09 có 150 clicks, mỗi click 2.0 USD ➔ `spend_hour = 300.0000 USD`.

## 5. Điểm mạnh / Điểm yếu / Khi nào không dùng
- **Điểm mạnh:** Cho phép xây dựng lại toàn bộ Silver và Gold từ Bronze khi logic nghiệp vụ thay đổi; cô lập dữ liệu lỗi; tối ưu hóa hiệu năng truy vấn cho dashboard.
- **Điểm yếu:** Tốn thêm dung lượng lưu trữ (lưu trữ 3 tầng); độ trễ tăng nhẹ qua từng trigger.
- **Khi nào không dùng:** Các ứng dụng giao dịch OLTP yêu cầu ghi tức thì độ trễ micro-giây.

## 6. Hiểu lầm phổ biến
1. *Hiểu lầm:* Nên làm sạch dữ liệu ngay trên broker Kafka trước khi ghi vào hồ.  
   *Sự thật:* Kafka broker không có khả năng lưu trữ vô hạn; Bronze trong Object Storage là nguồn sự thật duy nhất có thể replay bất cứ lúc nào.
2. *Hiểu lầm:* Tầng Gold có thể ghi đè trực tiếp lên Silver để tiết kiệm dung lượng.  
   *Sự thật:* Phá vỡ nguyên tắc phân tầng sẽ làm mất khả năng đối soát chi tiết từng event đơn lẻ.

## 7. Trong repo này

| File:dòng | Symbol | Đoạn code | Giải thích |
| :--- | :--- | :--- | :--- |
| `docs/data_dictionary.md:12-21` | `Nhóm A & B` | Bảng phân loại Stream vs Dimension | Phân định ranh giới giữa bảng Event Stream (Bronze/Silver) và bảng Dimension (Gold). |
| `dbt_cpc/models/gold/fct_campaign_hourly_pacing.sql:86-90` | `elapsed_day_fraction` | Tính đường lý tưởng cho giờ đóng & giờ dở | Đảm bảo giờ hiện tại dùng tỷ lệ giây thực tế tại `sim_now` thay vì `(hour + 1)/24`. |
| `dbt_cpc/models/gold/fct_publisher_quality.sql:62-68` | `fraud_flag_r1`, `fraud_flag_r2` | Cờ heuristic P2 theo `publisher × hour` | Dùng `NULLIF(clicks, 0)` và cửa sổ `median(...) over (partition by hour_ts)`. |
| `dbt_cpc/models/gold/fct_recruitment_efficiency.sql:46-50` | `cpc`, `cpa`, `cpqa`, `qualified_rate` | Chỉ số P3 theo `job × campaign × date` | Mọi phép chia dùng `NULLIF(..., 0.0)` để trả `NULL` khi mẫu số bằng 0 (R9). |

```sql
-- Trích nguyên văn từ dbt_cpc/models/gold/fct_campaign_hourly_pacing.sql:86-90
        case
            when hs.hour_ts = date_trunc('hour', hs.sim_now)
                then extract(epoch from (hs.sim_now - date_trunc('day', hs.sim_now))) / 86400.0
            else (extract(hour from hs.hour_ts) + 1) / 24.0
        end as elapsed_day_fraction,
```

## 8. Tự kiểm tra
1. *Câu hỏi:* Tại sao bảng Bronze cần lưu cả `kafka_offset` và `payload_raw`?  
   *Đáp án:* Để có thể truy vết chính xác nguồn gốc từng message từ broker và phân tích lại các trường dữ liệu mới nếu schema thay đổi.
2. *Câu hỏi:* Grain của bảng `fct_campaign_hourly_pacing` ở tầng Gold là gì?  
   *Đáp án:* `campaign_id` × `hour` (chiến dịch theo từng giờ).
3. *Câu hỏi:* Khi phát hiện một lỗi tính tiền trong logic Silver, ta xử lý thế nào?  
   *Đáp án:* Sửa code biến đổi, xóa dữ liệu Silver bị sai, và chạy lại streaming pipeline đọc từ mốc bắt đầu của Bronze mà không cần xin lại dữ liệu từ client.

## 9. Đọc thêm
- Databricks Medallion Architecture Guide: https://www.databricks.com/glossary/medallion-architecture
- Kimball Dimensional Modeling Techniques: https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/
