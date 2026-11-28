# ADR-010 — Nguồn Ngân sách Campaign (Dữ liệu thật `campaign.csv`) và Đường chi tiêu lý tưởng tuyến tính

- **Trạng thái:** Accepted
- **Ngày:** 2026-11-28
- **Tác giả:** Chủ dự án & AI Agent

## 1. Bối cảnh
Bài toán **P1 (Budget Pacing)** cần giá trị ngân sách ngày (`daily_budget`) cho từng chiến dịch và đường chuẩn kỳ vọng theo thời gian (`ideal_cum_spend`) để tính `pacing_ratio`, `budget_utilization` và `projected_eod_spend`. Theo quy tắc **R1 (Không bịa dữ liệu)** và **R13 (Tách mô phỏng khỏi thật)**, dự án phải ưu tiên sử dụng ngân sách thật nếu bộ dữ liệu gốc có sẵn, đồng thời đảm bảo công thức đường lý tưởng nhất quán tuyệt đối giữa Đường nhanh (DuckDB SQL trên Dashboard) và Đường sâu (dbt Gold marts).

## 2. Các phương án
- **Phương án A (Khuyến nghị): Dùng ngân sách thật từ `data/raw/campaign.csv` + đường lý tưởng tuyến tính theo giây ở giờ hiện tại**  
  Chuẩn hoá `campaign.csv` qua `scripts/prepare_dbt_seeds.py` thành `seed_campaign_budget.csv` / `dim_campaign_budget` (`DECIMAL(18,4)`). Với giờ đã đóng (`h < hour(sim_now)`), dùng `daily_budget * (h + 1) / 24.0`; với giờ đang chạy dở tại `sim_now`, dùng `daily_budget * (seconds_since_midnight(sim_now) / 86400.0)`.
  - *Ưu điểm:* 100% bám dữ liệu thật (`campaign.csv`), hoàn toàn tất định (deterministic), không cần sinh giả; tránh được lỗi phóng đại `ideal_cum_spend` ở đầu mỗi giờ khi giờ hiện tại chưa trôi hết 60 phút.
  - *Nhược điểm:* Đường tuyến tính chia đều 24 giờ chưa phản ánh phân phối lưu lượng ngày/đêm tự nhiên; dữ liệu thật chứa một vài campaign test có ngân sách ngoại lai (như `campaign_id = 1`).
- **Phương án B: Sinh ngân sách tổng hợp ngẫu nhiên (`generate_budget_seed.py`)**  
  Gán `daily_budget` giả lập dựa trên bội số của tổng `spend` thực tế.
  - *Ưu điểm:* Dễ ép mọi campaign có `pacing_ratio` nằm quanh vùng 0.5–1.5 để trình diễn cảnh báo.
  - *Nhược điểm:* Vi phạm **R1** khi `campaign.csv` đã có sẵn ngân sách hợp lệ; đánh mất tính chân thực kinh doanh của bộ dữ liệu gốc.
- **Phương án C: Đường cong kỳ vọng phi tuyến theo lịch sử từng giờ**  
  Ước lượng trọng số chi tiêu theo giờ từ dữ liệu lịch sử nhiều tuần.
  - *Ưu điểm:* Sát thực tế hành vi người tìm việc ban ngày/ban đêm hơn.
  - *Nhược điểm:* Phức tạp hoá công thức Gold và Fast Path; bộ dữ liệu sạch hiện có quy mô hẹp theo thời gian nên trọng số lịch sử dễ bị overfitting.

## 3. Bằng chứng
Kết quả khảo sát trực tiếp bằng DuckDB trên `data/raw/campaign.csv` và `data/tmp/replay_sorted.parquet` tại Task 2.1 (U9) & Task 4.2:
1. Tổng số bản ghi trong `campaign.csv`: **`193` campaign**, `193` ID duy nhất, **`0` giá trị `budget_total` null hoặc `<= 0`**.
2. Phân bố `pacing_unit`: **`179`** dòng `'Daily'` (`1` – `4,536,271,809`), **`13`** dòng `'daily'` (`220` – `1,932,570`), **`1`** dòng `'Weekly'` (`id=110`, `budget_total = 122,222,222`, chuẩn hoá ngày = `17,460,317.4286`).
3. Độ phủ khoá ngoại trên luồng sự kiện replay (`replay_sorted.parquet`): **11/11** `campaign_id` xuất hiện trong sự kiện đều tồn tại trong `campaign.csv` (`0` orphan).
4. **Ngoại lai cần lưu ý khi kiểm thử:**
   - `campaign_id = 1` (`Campaign 121`) có `budget_total = 1,002,930,129` USD (~1 tỷ, dữ liệu test gốc).
   - `campaign_id = 192` (`New Campaignfde`) có `budget_total = 121,212,123` USD.
   - Các chiến dịch phổ thông trong replay như `campaign_id = 15` (`100`), `222` (`500`), `93` (`1,000`), `122` (`1,000`), `193` (`5,555`), `151` (`10,000`) có dải ngân sách thực tế phù hợp để kiểm chứng P1 và ngưỡng cảnh báo.

## 4. Quyết định
**Chọn Phương án A:**
1. Sử dụng trực tiếp `budget_total` từ `data/raw/campaign.csv` (không dùng script sinh ngân sách ngẫu nhiên). Chuẩn hoá `pacing_unit` không phân biệt hoa thường (`Daily` / `Weekly`); trường hợp `Weekly` quy đổi tuyến tính `daily_budget = budget_total / 7` làm tròn `DECIMAL(18,4)`.
2. Gắn cờ `is_outlier_budget = true` cho các campaign có `daily_budget >= 100,000,000.0000` (bao gồm `campaign_id = 1`) và gắn nhãn `budget_source = 'campaign.csv'` để hiển thị minh bạch trên Dashboard theo **R13**.
3. Đường lý tưởng tuyến tính `ideal_cum_spend` tuân thủ tuyệt đối Mục 2.2 & 2.3 của Metric Catalog:
   - Giờ đã đóng (`h < hour(sim_now)`): `ideal_cum_spend = daily_budget * (h + 1) / 24.0`
   - Giờ đang chạy dở (`h = hour(sim_now)`): `ideal_cum_spend = daily_budget * (seconds_since_midnight(sim_now) / 86400.0)`

## 5. Hệ quả
- *Được:*
  - Tuân thủ tuyệt đối **R1**, **R7** (`DECIMAL(18,4)`), **R9**, **R13**; tái lập 100% khi chạy lại `scripts/prepare_dbt_seeds.py`.
  - Mọi `campaign_id`, `job_id`, `publisher_id` trong `stg_cpc_events` đều vượt qua kiểm định `relationships` với `dim_campaign_budget`, `dim_job`, `dim_publisher` (khi bổ sung dòng fallback `'unknown'` cho `dim_publisher`).
- *Mất / Đánh đổi:*
  - `campaign_id = 1` (ngân sách ~1 tỷ) sẽ luôn có `budget_utilization` xấp xỉ `0%` và `pacing_ratio << 0.6`; khi viết test hoặc đánh giá cảnh báo P1 (ví dụ `"pacing_ratio < 0.6"` hoặc ngưỡng > 90%), cần nhận biết rõ ngoại lai này thay vì coi là lỗi pipeline.

## 6. Điều kiện xem xét lại
Quyết định này được xem xét lại khi:
1. Có thêm dữ liệu lưu lượng nhiều tuần cho phép xây dựng đường cong pacing phi tuyến theo giờ mà vẫn giữ được parity giữa DuckDB và dbt; hoặc
2. Chủ dự án quyết định tách riêng môi trường demo giả lập ngân sách nhỏ hơn cho các campaign test ngoại lai (`campaign_id = 1`).
