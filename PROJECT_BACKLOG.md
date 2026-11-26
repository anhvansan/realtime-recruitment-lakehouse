# PROJECT BACKLOG — Near Real-Time CPC AdTech Lakehouse

> **Mục tiêu:** Xây dựng hệ thống giám sát hiệu quả và chi tiêu quảng cáo tuyển dụng (CPC) gần thời gian thực, chạy trên máy cá nhân (16GB RAM, ổ C hạn chế), kiến trúc **Zero-Copy Lakehouse**.
> **Stack:** Python Replay Producer ➔ Redpanda (Kafka API) ➔ PySpark Structured Streaming ➔ Delta Lake trên **MinIO (S3-compatible object store)** ➔ DuckDB + dbt ➔ Streamlit + Alert Worker.
> **Ngôn ngữ tài liệu:** tiếng Việt, giữ nguyên thuật ngữ kỹ thuật tiếng Anh.
> **Người học:** lần đầu làm project streaming, nên tài liệu này vừa là backlog vừa là giáo trình (xem Phần 7 và Phần 8).

> 📌 **Quyết định lưu trữ (ADR-001):** Dự án chốt sử dụng **MinIO** (bản tag ổn định, pin version cụ thể) làm S3-compatible object store cho môi trường local vì tính ổn định thực tế, nhẹ và có sẵn MinIO Console dễ quan sát. Tuy nhiên, toàn bộ code (Spark, DuckDB, dbt, boto3) bắt buộc đọc endpoint/credential từ biến môi trường (`.env`) chuẩn S3 để đảm bảo nguyên lý decoupling — có thể chuyển sang AWS S3, SeaweedFS hoặc Garage bất cứ lúc nào mà không phải sửa code.

## MỤC LỤC

```

```

# 0. CÁCH DÙNG TÀI LIỆU NÀY

## 0.1 Hai đối tượng đọc

| Đối tượng                 | Đọc gì trước                                          | Mục đích                                                                                      |
| ------------------------- | ----------------------------------------------------- | --------------------------------------------------------------------------------------------- |
| **AI Agent triển khai**   | Phần 0 → 1 → 2 → 3 → 4 → 5 (theo thứ tự task)         | Hiểu _vì sao_ làm, làm _đúng công thức_, kiểm chứng bằng lệnh                                 |
| **Chủ dự án (người học)** | Phần 1 → 7 → 8, rồi quay lại Phần 5 khi task đang làm | Hiểu nguyên lý trước khi xem code; mỗi task có mã `Learn:` trỏ tới module kiến thức tương ứng |

## 0.2 Quy tắc làm việc của Agent (BẮT BUỘC)

1.  **R1 — Không bịa dữ liệu.** Mọi giả định về schema, mối quan hệ và ý nghĩa cột của 17 file CSV trong `data/raw/` (`tracking.csv`, `search.csv`, `campaign.csv`, `application.csv`, `job.csv`, ...) phải đi qua Task 2.1 và được ghi trong `docs/data_dictionary.md`. Nếu mơ hồ, dừng lại, nêu 2–3 phương án kèm khuyến nghị và hỏi chủ dự án.

2.  **R2 — Dạy trước khi làm.** Trước mỗi task, viết 3–5 dòng "Sắp làm gì và vì sao" bằng tiếng Việt cho chủ dự án. Sau task, cập nhật lesson tương ứng trong `docs/lessons/` và `docs/code_map.md` (xem Phụ lục A).

3.  **R3 — Chỉ tick `[x]` khi đã chạy Acceptance Criteria (AC).** Dán output rút gọn vào `docs/progress/<task-id>.md`.

4.  **R4 — Pin phiên bản.** Không dùng tag `latest`. Ghi mọi phiên bản trong `docs/versions.md` (kèm ma trận tương thích Spark ↔ Delta ↔ hadoop-aws ↔ Scala ↔ DuckDB ↔ MinIO). Không dùng image `bitnami/*`. Ưu tiên image chính thức (`apache/spark`, `redpandadata/*`, `minio/minio`, `minio/mc`).

5.  **R5 — Kỷ luật tài nguyên.** Mỗi service có `mem_limit`, log rotation, dữ liệu bind mount sang ổ D. Chạy `make disk-report` trước/sau mỗi epic. Nếu RAM vượt ngân sách (mục 4.4) quá 20% hoặc ổ C tăng bất thường (> 1GB), dừng và báo cáo.

6.  **R6 — Không secrets trong git.** Credentials chỉ ở `.env` (đã `.gitignore`); `.env.example` liệt kê đủ biến. Log không được in credential.

7.  **R7 — Kiểu dữ liệu.** Tiền = `DECIMAL(18,4)` (không dùng float; khi truyền qua JSON bắt buộc serialize dạng **string**, ví dụ `"12.5000"`). Thời gian = UTC, ISO-8601. Ngày (`event_date`) suy ra từ `event_time` theo UTC.

8.  **R8 — Idempotency mặc định.** Mọi sink phải chạy lại an toàn: chạy lại 1 batch, restart job, hoặc replay không được làm sai số liệu tiền.

9.  **R9 — Metric Catalog là luật.** Công thức chỉ được định nghĩa ở Phần 2. Muốn đổi: sửa Phần 2 trước, ghi ADR, rồi mới sửa code.

10. **R10 — Tài liệu đúng phiên bản thắng backlog.** Nếu docs của phiên bản đã pin khác backlog, làm theo docs và ghi vào `docs/versions.md` mục _Deviations_. Mọi snippet trong tài liệu này là **minh hoạ pattern**, không phải code copy-paste.

11. **R11 — Code dễ test.** Logic nghiệp vụ viết thành hàm thuần (`DataFrame → DataFrame` hoặc `dict → dict`) để test bằng `pytest` mà không cần chạy cả hệ thống.

12. **R12 — Commit nhỏ.** Conventional Commits (`feat:`, `fix:`, `docs:`, `test:`, `chore:`). Mỗi task ≥ 1 commit; mô tả commit/PR dán output của AC.

13. **R13 — Tách "mô phỏng" khỏi "thật".** Thời gian mô phỏng (và ngân sách campaign nếu phải sinh tổng hợp) phải ghi rõ trong README và dashboard (nhãn "simulated"). Nếu lấy được ngân sách thật từ `campaign.csv`, ghi rõ nguồn thật.

14. **R14 — Báo cáo lệch.** Nếu kết quả đo khác kỳ vọng của backlog (latency, RAM, throughput), ghi số thật và cập nhật SLO với lý do, không giấu.

## 0.3 Mẫu một Task

```
### Task X.Y — Tên task            [Size: S|M|L]   [Depends: ...]
**Goal:**        vì sao task này tồn tại, giải quyết rủi ro/nhu cầu gì
**Steps:**       các bước cụ thể, theo thứ tự
**Deliverables:** file/thư mục sẽ có trong repo
**AC (Acceptance Criteria):** lệnh chạy được + kết quả kỳ vọng, kiểm chứng được
**Pitfalls:**    bẫy thường gặp
**Learn:**       mã module kiến thức (F = nền tảng, T = công cụ) cần đọc/viết lesson

```

Size: **S** ≤ 1 giờ · **M** vài giờ · **L** ≥ 1 ngày.
Trạng thái: `[ ]` chưa làm · `[~]` đang làm · `[x]` xong (đã chạy AC).

# 1. BUSINESS CONTEXT

## 1.1 Công ty giả định và mô hình kinh doanh

Công ty giả định **"HireBoost"**: nền tảng quảng cáo tuyển dụng **trả phí theo click (CPC, Cost Per Click)**.

```
Nhà tuyển dụng (Advertiser, group_id / company_id)
   └─ tạo Campaign (campaign_id): có ngân sách ngày (daily_budget) và giá thầu (bid) cho mỗi click
        └─ gắn Job (job_id): tin tuyển dụng cần quảng bá
Publisher (publisher_id): website/ứng dụng đối tác hiển thị tin tuyển dụng

Hành trình của một ứng viên (funnel):
  [Impression] ➔ Click ➔ Apply (nộp hồ sơ) ➔ Nhà tuyển dụng duyệt ➔ Qualified | Unqualified

```

**Dòng tiền:** mỗi click hợp lệ làm Advertiser bị trừ tiền = `spend` (giả thuyết H1: `spend` = `bid` của campaign tại thời điểm click; xem 3.2). Hết ngân sách ngày thì campaign ngừng hiển thị. Advertiser trả tiền cho _click_, nhưng giá trị họ muốn là _hồ sơ đạt (qualified)_. Khoảng cách giữa hai thứ này chính là nguồn gốc các bài toán dưới đây.

## 1.2 Bốn bài toán doanh nghiệp (Business Problems)

| ID     | Bài toán                                    | Kịch bản thực tế                                                                                                                                         | Quyết định mà dữ liệu hỗ trợ                | Độ trễ chấp nhận được         |
| ------ | ------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------- | ----------------------------- |
| **P1** | **Budget Pacing** (nhịp tiêu ngân sách)     | Campaign có 1.000/ngày. 10:00 sáng đã tiêu 700 → hết tiền trước trưa, bỏ lỡ cao điểm ứng tuyển buổi chiều. Ngược lại, tiêu quá chậm thì lãng phí cơ hội. | Giảm/tăng bid, tạm dừng, nạp thêm ngân sách | **Vài phút**                  |
| **P2** | **Publisher Quality / Click Fraud**         | Một publisher sinh 500 click trong 30 phút nhưng 0 hồ sơ nộp → tiền bị đốt vào click ảo hoặc traffic rác.                                                | Chặn/hạ ưu tiên publisher, đòi hoàn tiền    | **15–60 phút** (cửa sổ trượt) |
| **P3** | **Hiring Efficiency** (hiệu quả tuyển dụng) | Job A tốn 2 triệu cho mỗi hồ sơ đạt, Job B chỉ 400 nghìn. Ngân sách đang chia đều.                                                                       | Chuyển ngân sách sang job/campaign hiệu quả | **Giờ/ngày**                  |
| **P4** | **Data Trust & Pipeline Health**            | Event trùng làm `spend` nhân đôi → Finance xuất hóa đơn sai. Pipeline chết im lặng nên dashboard đứng yên mà không ai biết.                              | Tin hay không tin con số; sửa nguồn dữ liệu | **Giây – phút**               |

> **Nguyên tắc thiết kế rút ra:** P1 và P4 quyết định yêu cầu "near real-time" (phút). P2 cần cửa sổ trượt. P3 chấp nhận gold theo giờ. Vì vậy hệ thống chia 2 đường: **đường nhanh** (DuckDB đọc trực tiếp Silver, làm tươi mỗi \~10s) cho P1/P2/P4 và **đường sâu** (dbt gold marts) cho P3 và kiểm định chất lượng.

## 1.3 Người dùng và User Stories

| Persona                                  | Vai trò                        | User story                                                                                                            | Dashboard/Alert đáp ứng                           |
| ---------------------------------------- | ------------------------------ | --------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------- |
| **Linh** — Performance Marketing Manager | Quản lý hiệu quả campaign      | _Là Linh, tôi muốn thấy campaign nào đang tiêu nhanh hơn kế hoạch, để kịp điều chỉnh bid trước khi hết ngân sách._    | Panel 1 + Alert "campaign dùng > 90% ngân sách"   |
| **Minh** — Finance Analyst               | Đối soát chi tiêu              | _Là Minh, tôi muốn chắc chắn tổng `spend` không bị nhân đôi do event trùng, để hóa đơn đúng._                         | Panel 4 (dedup rate, reconciliation) + `dbt test` |
| **Hùng** — Publisher Operations          | Quản lý đối tác                | _Là Hùng, tôi muốn danh sách publisher nghi ngờ click ảo, để liên hệ hoặc chặn kịp thời._                             | Panel 2 + Alert "click cao, apply = 0"            |
| **Hạnh** — Hiring Success Manager        | Tư vấn hiệu quả cho Advertiser | _Là Hạnh, tôi muốn CPA và Cost per Qualified Application theo job, để khuyên Advertiser đổi chiến lược._              | Panel 3 + `fct_recruitment_efficiency`            |
| **Bạn** — Data/Platform Engineer         | Vận hành pipeline              | _Tôi muốn biết dữ liệu mới nhất trễ bao lâu và có bao nhiêu bản ghi bị loại vào DLQ, để biết khi nào phải can thiệp._ | Panel 4 + alert "data stale"                      |

## 1.4 Service Level Objectives (SLO) — mục tiêu cần đo, không phải lời hứa

> Các con số là **mục tiêu ban đầu**. Task 6.4 đo thực tế, và theo R14 phải cập nhật bảng này bằng số thật.

| Nhóm        | Chỉ tiêu                                                                 | Mục tiêu ban đầu                   | Cách đo                                                       |
| ----------- | ------------------------------------------------------------------------ | ---------------------------------- | ------------------------------------------------------------- |
| Latency     | End-to-end (producer gửi → hiển thị trên dashboard), p95                 | ≤ 45 giây (stretch: 30 giây)       | `produced_at` (producer) so với thời điểm truy vấn thấy event |
| Freshness   | Tuổi của event mới nhất trong Silver                                     | ≤ 60 giây; cảnh báo nếu > 120 giây | `now() - max(processed_at)`                                   |
| Correctness | Chênh lệch `spend` giữa Silver và "sự thật" từ manifest sau kill/restart | **= 0** (chính xác tuyệt đối)      | Test đối soát (Task 3.5 và 3.6)                               |
| Correctness | Số bản ghi lỗi được bắt vào DLQ                                          | = số lỗi đã bơm (theo manifest)    | Task 2.6, 3.3                                                 |
| Resource    | RAM tổng ở trạng thái ổn định                                            | ≤ 4.5GB (mục tiêu 4GB)             | `docker stats` (Task 1.6)                                     |
| Resource    | Ổ C tăng sau 24 giờ chạy                                                 | < 500MB                            | `make disk-report`                                            |
| Recovery    | Thời gian hồi phục sau `kill -9` job Spark                               | ≤ 60 giây, không trùng, không mất  | Lab L1                                                        |
| Query       | Độ trễ truy vấn mỗi panel, p95, với dữ liệu 1 ngày mô phỏng              | ≤ 1.5 giây                         | Log thời gian truy vấn trong app                              |

# 2. METRIC CATALOG (nguồn sự thật duy nhất)

> Mọi công thức ở đây là luật (R9). Dashboard, dbt, alert worker và test đều dùng đúng các định nghĩa này.
> Quy ước: `NULLIF(mẫu số, 0)` cho mọi phép chia; kết quả `NULL` được hiển thị là "n/a", không phải 0.

## 2.1 Ba loại thời gian (đọc trước khi đọc metric)

| Tên cột        | Nghĩa                                                                         | Ai gán   | Dùng để                                         |
| -------------- | ----------------------------------------------------------------------------- | -------- | ----------------------------------------------- |
| `event_time`   | Thời điểm sự kiện xảy ra **theo đồng hồ mô phỏng** (đã ánh xạ từ dữ liệu gốc) | Producer | Cửa sổ thời gian, watermark, tính theo giờ/ngày |
| `produced_at`  | Đồng hồ **thật (wall clock)** lúc producer gửi                                | Producer | Đo latency end-to-end                           |
| `processed_at` | Đồng hồ thật lúc Spark ghi vào Silver                                         | Spark    | Đo freshness, latency                           |

Kafka còn có `timestamp` riêng (thời điểm broker nhận). Bronze lưu cả `kafka_timestamp`, `partition`, `offset` để truy vết và replay.

## 2.2 Danh mục metric

| Metric                       | Ý nghĩa kinh doanh                                         | Công thức                                                                                                                                                                                      | Grain                                | Lưu ý/edge case                                                                            |
| ---------------------------- | ---------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------ | ------------------------------------------------------------------------------------------ |
| `impressions` _(Đã loại bỏ)_ | Số lần tin được hiển thị                                   | ĐÃ LOẠI BỎ (Kết quả Task 2.1 H4: không có log impression, events.csv impressions là NULL 100%)                                                                                                 | campaign × hour                      | Bỏ CTR và tầng Impression khỏi phễu theo R1 & R9                                           |
| `clicks`                     | Số click hợp lệ                                            | `COUNT(*) WHERE event_type='click'`                                                                                                                                                            | mọi grain                            | Đã dedup theo `event_id`                                                                   |
| `spend`                      | Tiền đã tiêu                                               | `SUM(spend)` trên event `click`                                                                                                                                                                | mọi grain                            | Chỉ click mới tạo spend (H1, ADR-009). `DECIMAL(18,4)`                                     |
| `applies`                    | Số hồ sơ nộp                                               | `COUNT(*) WHERE event_type='apply'`                                                                                                                                                            | mọi grain                            | Nguồn từ `application.csv`                                                                 |
| `qualified`                  | Hồ sơ đạt                                                  | `COUNT(*) WHERE event_type='qualified'`                                                                                                                                                        | mọi grain                            | Nguồn từ `application.csv`                                                                 |
| `unqualified`                | Hồ sơ không đạt                                            | `COUNT(*) WHERE event_type='unqualified'`                                                                                                                                                      | mọi grain                            | Nguồn từ `application.csv`                                                                 |
| `CPC`                        | Giá trung bình mỗi click                                   | `spend / NULLIF(clicks,0)`                                                                                                                                                                     | mọi grain                            | Nên xấp xỉ `bid`                                                                           |
| `CTR` _(Đã loại bỏ)_         | Tỷ lệ click trên hiển thị                                  | ĐÃ LOẠI BỎ (không có metric impressions)                                                                                                                                                       | publisher, campaign                  | Bỏ theo kết quả Task 2.1                                                                   |
| `click_to_apply_rate`        | Chất lượng traffic                                         | `applies / NULLIF(clicks,0)`                                                                                                                                                                   | publisher, campaign, job             | Chỉ số chính của P2                                                                        |
| `qualified_rate`             | Chất lượng ứng viên                                        | `qualified / NULLIF(applies,0)`                                                                                                                                                                | job, campaign                        |                                                                                            |
| `CPA`                        | Chi phí mỗi hồ sơ nộp                                      | `spend / NULLIF(applies,0)`                                                                                                                                                                    | job, campaign                        |                                                                                            |
| `CPQA`                       | Chi phí mỗi hồ sơ **đạt** (Cost per Qualified Application) | `spend / NULLIF(qualified,0)`                                                                                                                                                                  | job, campaign                        | Chỉ số chính của P3                                                                        |
| `daily_budget`               | Ngân sách ngày                                             | Lấy trực tiếp từ `campaign.csv` (cột `budget_total` với `pacing_unit = 'Daily'`)                                                                                                               | campaign                             | Đã xác nhận ở Task 2.1 (U9); dùng dữ liệu thật, không sinh ngẫu nhiên                      |
| `budget_utilization`         | % ngân sách đã dùng                                        | `cum_spend_today / daily_budget`                                                                                                                                                               | campaign (hiện tại)                  | Alert nếu `> 0.90`                                                                         |
| `ideal_cum_spend`            | Đường chi tiêu lý tưởng (tuyến tính)                       | \- **Giờ đã kết thúc (`h < hour(sim_now)`):** `daily_budget * (h + 1) / 24.0` \- **Thời điểm hiện tại (`sim_now` đang chạy dở):** `daily_budget * (seconds_since_midnight(sim_now) / 86400.0)` | campaign × hour / thời điểm hiện tại | Tránh lỗi phóng đại `ideal_cum_spend` ở đầu mỗi giờ khi giờ hiện tại chưa chạy hết 60 phút |
| `pacing_ratio`               | Đang tiêu nhanh/chậm so với kế hoạch                       | `cum_spend / NULLIF(ideal_cum_spend,0)`                                                                                                                                                        | campaign × hour / hiện tại           | `> 1.2` = tiêu quá nhanh; `< 0.6` = tiêu quá chậm (ngưỡng trong `config/alerts.yaml`)      |
| `burn_rate`                  | Tốc độ đốt tiền/giờ                                        | `spend` trong 60 phút gần nhất (đồng hồ mô phỏng)                                                                                                                                              | campaign                             |                                                                                            |
| `projected_eod_spend`        | Dự báo cuối ngày                                           | `cum_spend + burn_rate * giờ_còn_lại_tính_từ_sim_now`                                                                                                                                          | campaign                             | Dự báo đơn giản, không phải ML                                                             |
| `fraud_flag_R1`              | Click nhiều, không có hồ sơ                                | `clicks_60m >= 100 AND applies_60m = 0`                                                                                                                                                        | publisher                            | Ngưỡng cấu hình được                                                                       |
| `fraud_flag_R2`              | Tỷ lệ chuyển đổi thấp bất thường                           | `click_to_apply_rate_60m < 0.2 * median(click_to_apply_rate của mọi publisher cùng cửa sổ)` AND `clicks_60m >= 50`                                                                             | publisher                            | Heuristic, **không phải ML**; ghi rõ trong README                                          |
| `data_freshness_s`           | Dữ liệu cũ bao lâu                                         | `now() - MAX(processed_at)` (giây)                                                                                                                                                             | toàn hệ thống                        | Alert nếu > 120                                                                            |
| `dlq_rate`                   | Tỷ lệ bản ghi lỗi                                          | `dlq_rows / (dlq_rows + bronze_rows)`                                                                                                                                                          | khoảng 15 phút                       |                                                                                            |
| `dedup_rate`                 | Tỷ lệ bản ghi trùng bị loại                                | `1 - silver_rows / bronze_rows`                                                                                                                                                                | khoảng 15 phút                       | Với 2% trùng được bơm, kỳ vọng ≈ 2%                                                        |
| `late_rate`                  | Tỷ lệ event đến trễ                                        | `COUNT(event_time < watermark_tại_lúc_nhận) / total`                                                                                                                                           | khoảng 15 phút                       | Lấy từ metric của Spark/manifest                                                           |
| `e2e_latency_s`              | Độ trễ end-to-end                                          | `processed_at - produced_at` (p50, p95)                                                                                                                                                        | toàn hệ thống                        | Dùng cho SLO                                                                               |

## 2.3 Ví dụ số để kiểm tra hiểu công thức (dùng làm unit test)

Campaign `C1`, `daily_budget = 1000`.

- **Trường hợp 1 — Giờ đã kết thúc:** Lúc cuối giờ 05 (tức `05:59:59` UTC, đã qua 6 tiếng), tích luỹ đã tiêu **400**.
  - `ideal_cum_spend(5) = 1000 * (5 + 1) / 24 = 250` ➔ `pacing_ratio = 400 / 250 = 1.6` ➔ **tiêu quá nhanh** (vượt ngưỡng 1.2).

  - `budget_utilization = 400 / 1000 = 0.40` (chưa tới 0.90, nên _chỉ_ cảnh báo pacing, chưa cảnh báo hết ngân sách).

  - Nếu `burn_rate = 120/giờ` thì `projected_eod_spend = 400 + 120 * 18 = 2560` ➔ gấp 2.56 lần ngân sách ➔ ngân sách sẽ cạn khoảng giờ 10 (`400 + 120*t = 1000` ➔ `t = 5` giờ nữa).

- **Trường hợp 2 — Giờ đang chạy dở (`sim_now = 10:15:00` UTC):** Đã trôi qua `10.25` giờ (`36.900` giây / `86.400` giây).
  - `ideal_cum_spend(sim_now) = 1000 * (36900 / 86400) = 427.0833`. (Nếu tính sai theo công thức `(10 + 1)/24` sẽ ra `458.3333`, làm sai lệch tỷ lệ pacing ở đầu giờ).

Publisher `P9` trong 60 phút: `clicks = 130`, `applies = 0` ➔ `fraud_flag_R1 = TRUE`. Nếu `clicks = 99`, `applies = 0` ➔ `FALSE` (kiểm tra biên).

## 2.4 Quy tắc "now" trong môi trường mô phỏng

Dữ liệu được tua nhanh nên "hôm nay" theo đồng hồ thật không có ý nghĩa. Định nghĩa:

- `sim_now = MAX(event_time)` trong Silver (đồng hồ mô phỏng). Mọi panel "hôm nay", "giờ hiện tại", "60 phút gần nhất" dùng `sim_now`.

- Chỉ `data_freshness_s` và `e2e_latency_s` dùng đồng hồ thật (`processed_at`, `produced_at`).

- Alert dùng `sim_now` để tính cửa sổ, nhưng cooldown chống spam dùng đồng hồ thật.

# 3. DỮ LIỆU NGUỒN VÀ DATA CONTRACT

## 3.1 Tập 17 file CSV nguồn trong `data/raw/`

Khác với giả định ban đầu chỉ có 2 file (`tracking.csv` và `search.csv`), bộ dữ liệu gốc trong `data/raw/` gồm **17 file CSV** xuất từ hệ thống OLTP tuyển dụng/quảng cáo:

| Nhóm dự kiến (cần xác minh ở Task 2.1)                                                       | Tên file (kích thước tham khảo)                                                                                                                                                                        | Vai trò dự kiến trong kiến trúc                                                                                                                                                                                                                |
| -------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **1. Nhóm Event / Streaming 후보**                                                           | `tracking.csv` (\~2.58 MB) `application.csv` (\~20 KB) `events.csv` (\~2 KB) `search.csv` (\~439 KB)                                                                                                   | Luồng sự kiện quảng cáo & ứng tuyển (`click`, `apply`, `qualified`, ...) hoặc từ điển mã sự kiện. Cần xác định xem `tracking.csv` đã chứa đủ phễu hay phải kết hợp với `application.csv`/`events.csv`, và vai trò thật của `search.csv`.       |
| **2. Nhóm Dimension / Master Data 후보**                                                     | `campaign.csv` (\~34 KB) `job.csv` (\~459 KB) `master_publisher.csv` (\~8 KB) `company.csv` (\~14 KB) `group.csv` (\~5 KB) `master_major_category.csv` (\~2 KB) `master_minor_category.csv` (\~29 KB)  | Bảng danh mục phục vụ tầng Silver/Gold (dbt seeds hoặc bảng dimension): thông tin chiến dịch (có thể chứa `daily_budget`, `bid`), thông tin tin tuyển dụng (`job`), tên đối tác (`publisher`), nhà tuyển dụng (`company`/`group`), ngành nghề. |
| **3. Nhóm Quản trị nội bộ / Địa lý chi tiết (Dự kiến loại bỏ để tránh phình phạm vi - K14)** | `master_city.csv` (\~11 MB) `job_location.csv` (\~630 KB) `dev_user.csv` (\~74 KB) `conversation.csv` (\~8 KB) `user_role.csv` (\~8 KB) `master_role.csv` (\~3 KB) `user_client_assignee.csv` (\~2 KB) | Dữ liệu user nội bộ, phân quyền, chat, danh mục thành phố chi tiết. Khảo sát nhanh schema ở Task 2.1; nếu không phục vụ trực tiếp 4 bài toán P1–P4 thì đưa vào danh sách **Ignore** kèm lý do.                                                 |

Agent **chưa được phép khẳng định** cấu trúc cột trước khi chạy script khảo sát. Task 2.1 là cổng bắt buộc để quét toàn bộ 17 file này, phân loại chính thức và trình chủ dự án duyệt trước khi thiết kế pipeline.

## 3.2 Giả thuyết (Hypotheses) và câu hỏi chưa biết (Unknowns) cần xác minh ở Task 2.1

| ID        | Giả thuyết / câu hỏi                                                                                                                          | Cách xác minh (DuckDB trên 17 file CSV)                                                          | Nếu sai / Kết quả dẫn tới                                                                                                                            |
| --------- | --------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------- |
| **U0**    | Trong 17 file CSV, những file nào thực sự cần dùng cho 4 bài toán P1–P4?                                                                      | Quét `DESCRIBE`, `COUNT(*)`, mẫu 3 dòng và khoá ngoại của cả 17 file                             | Lập bảng phân loại: **Stream Source**, **Dimension/Seed**, **Ignore** và hỏi ý kiến chủ dự án                                                        |
| **U1**    | Cột nào là thời điểm sự kiện, múi giờ gì, định dạng gì?                                                                                       | `DESCRIBE` + `SELECT min/max` + kiểm tra định dạng trên `tracking.csv` (và `application.csv`)    | Chuẩn hoá về UTC, ghi vào data dictionary                                                                                                            |
| **U2**    | Phễu sự kiện (`click`, `apply`, `qualified`...) nằm toàn bộ trong `tracking.csv` hay chia sang `application.csv` và tra cứu qua `events.csv`? | `SELECT col, COUNT(*) GROUP BY 1` trên `tracking.csv`, `application.csv`, đối chiếu `events.csv` | Nếu tách rời: thống nhất cách gộp luồng sự kiện ở Producer hoặc ánh xạ bảng `events.csv` ➔ `event_type` chuẩn                                        |
| **H1**    | `spend` của một click = `bid` của click đó (trong `tracking.csv` hoặc lấy từ `campaign.csv`)                                                  | So sánh cột giá/bid trong `tracking.csv` và `campaign.csv`; kiểm tra null/0                      | Nếu `bid` là giá tối đa chứ không phải giá thực trả, ghi ADR-009 và chọn quy tắc tính `spend`                                                        |
| **U4/H4** | Dữ liệu có sự kiện impression không?                                                                                                          | Đếm theo U2                                                                                      | Bỏ `impressions`, `CTR` và tầng Impression ở funnel; sửa Phần 2                                                                                      |
| **U5**    | Có khoá tự nhiên duy nhất cho mỗi event không?                                                                                                | `COUNT(*) vs COUNT(DISTINCT col)`                                                                | Dùng `event_id = sha1(f"{source_file}:{row_number}")` (3.3)                                                                                          |
| **U6**    | Mẫu null/thiếu giá trị, giá trị âm, ngoại lai                                                                                                 | Profile từng cột các file được chọn                                                              | Quy tắc validation ở Task 3.3                                                                                                                        |
| **U7**    | Khoảng thời gian, số event/ngày, phân phối theo giờ                                                                                           | `GROUP BY date, hour`                                                                            | Chọn `speed_factor` mặc định, kích thước benchmark                                                                                                   |
| **U8**    | Vai trò khác nhau giữa `search.csv` và `job.csv` là gì? Khoá join với `tracking.csv`?                                                         | So sánh schema `search.csv` vs `job.csv`, thử join với `tracking.csv`                            | Chọn bảng chuẩn cho `dim_job` (kết hợp `company.csv`, `group.csv`, `master_publisher.csv`), bỏ bảng thừa                                             |
| **U9**    | File `campaign.csv` có sẵn cột ngân sách (`daily_budget` / `total_budget`) không?                                                             | `DESCRIBE campaign.csv` + kiểm tra phân phối giá trị                                             | Nếu **có sẵn và hợp lệ**: dùng trực tiếp dữ liệu thật từ `campaign.csv`! Nếu thiếu hoặc rỗng: bổ sung bằng script sinh ngân sách mô phỏng ở Task 4.2 |

**Kết quả bắt buộc của Task 2.1:** `docs/data_dictionary.md` gồm: bảng phân loại toàn bộ 17 file CSV (Dùng làm Stream / Dùng làm Dimension / Loại bỏ kèm lý do), chi tiết cột của các file được chọn, bảng ánh xạ `event_type`, kết luận cho từng U0–U9/H1/H4, và danh sách câu hỏi trình chủ dự án chốt.

## 3.3 Canonical Event Contract — `cpc_event.v1`

Đây là schema của message trong topic `cpc_events_raw` (JSON). Tên cột là _đề xuất_; sau Task 2.1 có thể điều chỉnh, nhưng phải ghi vào `src/common/schemas/cpc_event.v1.json`.

| Trường           | Kiểu trong JSON                                     | Kiểu trong Silver | Bắt buộc      | Mô tả / quy tắc                                                                                           |
| ---------------- | --------------------------------------------------- | ----------------- | ------------- | --------------------------------------------------------------------------------------------------------- | ------- | ------- | ----------- | ------------- |
| `schema_version` | string                                              | STRING            | ✔             | `"v1"`                                                                                                    |
| `event_id`       | string(40)                                          | STRING            | ✔             | `sha1(f"{source_file}:{row_number}")` hex. **Producer** tính. Event bơm trùng gửi lại **cùng** `event_id` |
| `event_time`     | string ISO-8601 UTC                                 | TIMESTAMP         | ✔             | Thời gian mô phỏng (xem 2.4)                                                                              |
| `produced_at`    | string ISO-8601 UTC                                 | TIMESTAMP         | ✔             | Wall clock lúc gửi                                                                                        |
| `event_type`     | string (enum)                                       | STRING            | ✔             | `impression`                                                                                              | `click` | `apply` | `qualified` | `unqualified` |
| `job_id`         | string                                              | STRING            | ✔             |                                                                                                           |
| `campaign_id`    | string                                              | STRING            | ✔             | Cũng là Kafka key (ADR-005, xem Task 2.3)                                                                 |
| `group_id`       | string                                              | STRING            | tuỳ           | Advertiser                                                                                                |
| `publisher_id`   | string                                              | STRING            | ✔             |                                                                                                           |
| `bid`            | **string** (biểu diễn số thập phân, vd `"12.5000"`) | `DECIMAL(18,4)`   | ✔ với `click` | **Bắt buộc truyền dạng string trong JSON** để tránh sai số float. Không âm                                |
| `spend`          | **string** (biểu diễn số thập phân, vd `"12.5000"`) | `DECIMAL(18,4)`   | ✔             | `= bid` cho `click` (nếu H1 đúng), `"0.0000"` cho loại khác. Truyền dạng string, không âm                 |
| `source_row_id`  | integer                                             | BIGINT            | ✔             | Số dòng trong CSV, phục vụ truy vết                                                                       |

**Quy tắc validation (Task 3.3)** — vi phạm bất kỳ quy tắc nào ➔ DLQ với `error_type` tương ứng:
`corrupt_json` (không parse được) · `missing_required` · `bad_enum` (event_type lạ) · `malformed_decimal` (`bid`/`spend` không parse được sang `DECIMAL(18,4)`) · `negative_amount` · `bad_timestamp` · `schema_version_unsupported`.

**Message DLQ (`cpc_events_dlq` và bảng Delta `dlq/cpc_events_dlq`):**
`payload_raw`, `error_type`, `error_detail`, `source_topic`, `source_partition`, `source_offset`, `failed_at`, `stage` (`ingest`|`silver`).

## 3.4 Bố cục lưu trữ (bucket `lakehouse` trên MinIO)

```
s3://lakehouse/
├── bronze/cpc_events/           # Delta, append-only, partition by event_date
├── silver/cpc_events/           # Delta, đã làm sạch + dedup, partition by event_date
├── dlq/cpc_events_dlq/          # Delta, bản ghi lỗi (bản sao của topic DLQ để truy vấn)
├── ops/stream_metrics/          # Delta, metric tiến trình của Spark (Task 3.9)
├── gold/<model>/*.parquet       # External Parquet do dbt ghi
└── checkpoints/{ingest_bronze,refine_silver,dlq}/

```

**Bronze** giữ: các trường hợp lệ ở trên + `payload_raw`, `kafka_topic`, `kafka_partition`, `kafka_offset`, `kafka_timestamp`, `ingested_at`, `event_date`.
**Silver** giữ: các trường chuẩn hoá (đúng kiểu, `DECIMAL(18,4)`, `TIMESTAMP`), `event_date`, `processed_at`; **một dòng duy nhất cho mỗi `event_id`**.

## 3.5 Bảng Gold (dbt)

| Model                                                                                           | Grain                      | Cột chính                                                                                                | Phục vụ    |
| ----------------------------------------------------------------------------------------------- | -------------------------- | -------------------------------------------------------------------------------------------------------- | ---------- |
| `fct_campaign_hourly_pacing`                                                                    | campaign × giờ             | `cum_spend`, `ideal_cum_spend`, `pacing_ratio`, `budget_utilization`, `burn_rate`, `projected_eod_spend` | P1         |
| `fct_publisher_quality`                                                                         | publisher × giờ            | `clicks`, `applies`, `click_to_apply_rate`, `ctr`\*, cờ `fraud_flag_R1`, `fraud_flag_R2`                 | P2         |
| `fct_recruitment_efficiency`                                                                    | job × campaign × ngày      | `spend`, `clicks`, `applies`, `qualified`, `cpc`, `cpa`, `cpqa`, `qualified_rate`                        | P3         |
| `dim_campaign_budget` (và các bảng dim từ `campaign.csv`, `job.csv`, `master_publisher.csv`...) | campaign / job / publisher | `daily_budget`, tên campaign, tên job, tên publisher, công ty (xác định từ Task 2.1 & 4.2)               | P1, P2, P3 |

## 3.6 Quy ước chung

Tên bảng/cột `snake_case`; thời gian UTC; tiền `DECIMAL(18,4)`; không dùng `SELECT *` trong model gold; mọi biến cấu hình đọc từ `.env`/`config/*.yaml`, không hard-code endpoint, credential, ngưỡng.

# 4. KIẾN TRÚC, CẤU TRÚC REPO, NGÂN SÁCH TÀI NGUYÊN

## 4.1 Sơ đồ kiến trúc

```
 tracking.csv (+ các file nguồn đã chọn ở Task 2.1)
      ──► [1] Replay Producer (Python, confluent-kafka)
               • ánh xạ thời gian mô phỏng, speed_factor
               • bơm 2% trùng / 2% trễ / 1% JSON hỏng + ghi manifest sự thật
                              │  key = campaign_id
                              ▼
          [2] Redpanda (Kafka API)  ──── topic cpc_events_raw (3 partitions)
                      │                    topic cpc_events_dlq (1 partition)
                      ▼
        ┌─────────────── [3] Spark Structured Streaming (1 JVM, local mode) ───────────────┐
        │  Q1 ingest : Kafka ➔ parse(from_json PERMISSIVE) ➔ validate                      │
        │       foreachBatch ├─ hợp lệ  ➔ Bronze (Delta, append, idempotent txnVersion)    │
        │                    └─ lỗi     ➔ DLQ Delta + topic cpc_events_dlq                 │
        │  Q2 refine : Bronze (readStream Delta) ➔ chuẩn hoá ➔ watermark 10m ➔ dedup       │
        │       foreachBatch ➔ MERGE (insert-if-not-matched on event_id) ➔ Silver          │
        │  Maintenance: chỉ OPTIMIZE các partition ngày cũ đã đóng · Listener ➔ ops        │
        └──────────────────────────────────────────────────────────────────────────────────┘
                              │ Delta files + _delta_log
                              ▼
          [4] MinIO (S3-compatible object store, bucket `lakehouse`, dữ liệu ở ổ D)
               ▲                                  ▲
đọc trực tiếp (delta_scan)                  đọc/ghi qua dbt-duckdb
               │                                  │
   [5a] Streamlit + DuckDB :memory:          [5b] dbt-duckdb: gold marts + data tests
        (đường nhanh, làm tươi ~10s)               (đường sâu, chạy theo lịch/on-demand)
               │
   [6] Alert Worker (process riêng) ──► Telegram / Discord (có cooldown, trạng thái, resolve)

```

**Vì sao Spark chỉ chạy 1 JVM với nhiều query?** Mỗi JVM Spark tốn 1.5–2GB RAM; hai ứng dụng riêng sẽ vượt ngân sách. Một `SparkSession` chạy nhiều streaming query dùng chung bộ nhớ và dễ quan sát hơn (Task 3.1).

**Vì sao Q2 đọc từ Bronze thay vì đọc Kafka lần hai?** (a) Bronze là nguồn sự thật có thể replay: sửa logic Silver rồi dựng lại từ Bronze mà không phụ thuộc retention 1 giờ của Redpanda. (b) Tránh đọc Kafka hai lần. Cái giá: hai trigger nối tiếp, cộng thêm vài giây latency (4.3).

## 4.2 Hai đường dữ liệu phục vụ dashboard

| Đường     | Dùng cho                | Cách tính                                                             | Độ trễ               | Rủi ro                    | Biện pháp                          |
| --------- | ----------------------- | --------------------------------------------------------------------- | -------------------- | ------------------------- | ---------------------------------- |
| **Nhanh** | Panel P1, P2, P4        | DuckDB `delta_scan(silver)` + SQL trong `src/dashboard/queries/*.sql` | \~10–45s             | Công thức lệch so với dbt | Test "metric parity" (Task 4.7)    |
| **Sâu**   | P3, đối soát, kiểm định | dbt model ➔ Parquet gold                                              | 1–5 phút (theo lịch) | Trễ                       | Chấp nhận, hiển thị "cập nhật lúc" |

## 4.3 Ngân sách độ trễ (Latency budget, ước lượng cần đo)

| Chặng                                | Ước lượng                     | Ghi chú                                                         |
| ------------------------------------ | ----------------------------- | --------------------------------------------------------------- |
| Producer ➔ Redpanda                  | ≤ 1s                          | `linger.ms` nhỏ                                                 |
| Q1: chờ trigger + xử lý + ghi Bronze | ≤ 12s                         | trigger 10s + ghi \~2s                                          |
| Q2: chờ trigger + merge Silver       | ≤ 13s                         | trigger 10s + merge \~3s                                        |
| Streamlit làm tươi + truy vấn        | ≤ 11s                         | `run_every=10s` + truy vấn ≤ 1s                                 |
| **Tổng worst-case**                  | **\~37s**; p95 mục tiêu ≤ 45s | Giảm trigger Q2 xuống 5s nếu cần (đánh đổi: nhiều file nhỏ hơn) |

## 4.4 Ngân sách tài nguyên (RAM giới hạn bằng `mem_limit` trong compose)

| Service                                     | Profile | `mem_limit`             | Ghi chú                                 |
| ------------------------------------------- | ------- | ----------------------- | --------------------------------------- |
| `redpanda`                                  | core    | 704M                    | cờ `--memory 512M --smp 1`              |
| `redpanda-console`                          | core    | 192M                    |                                         |
| `minio`                                     | core    | 448M                    | S3 API cổng `9000`, Console cổng `9001` |
| `minio-init`                                | core    | \~32M (chạy xong thoát) | dùng `minio/mc` tạo bucket `lakehouse`  |
| `spark` (local\[2\])                        | apps    | 2304M                   | driver heap \~1.5G + overhead           |
| `dashboard` (Streamlit + DuckDB)            | apps    | 384M                    | DuckDB `memory_limit` ≈ 256MB           |
| `alert-worker`                              | apps    | 96M                     |                                         |
| `producer`                                  | tools   | 128M                    | chạy theo lệnh                          |
| `dbt`                                       | tools   | 384M                    | chạy theo lệnh                          |
| **Tổng ở trạng thái ổn định (core + apps)** |         | **≈ 4.1GB**             | chưa tính VM nền của Docker Desktop     |

Khuyến nghị host (Windows): giới hạn WSL2 trong `%UserProfile%\.wslconfig` (ví dụ `memory=8GB`, `processors=4`) để máy không bị nghẹt; cân nhắc `sparseVhd=true` trong mục `[experimental]` để file `ext4.vhdx` tự co lại (kiểm tra phiên bản WSL hiện hành).

## 4.5 Cổng và biến môi trường chính (`.env.example` phải liệt kê đủ)

| Biến                                                             | Ví dụ                                                                       | Dùng ở                                                                                         |
| ---------------------------------------------------------------- | --------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------- |
| `KAFKA_BOOTSTRAP_INTERNAL`                                       | `redpanda:9092`                                                             | Spark, alert-worker (trong mạng docker)                                                        |
| `KAFKA_BOOTSTRAP_EXTERNAL`                                       | `localhost:19092`                                                           | producer chạy trên host                                                                        |
| `S3_ENDPOINT`                                                    | `http://minio:9000` (trong docker) hoặc `http://localhost:9000` (trên host) | Spark, boto3. Lưu ý: `config.py` tự tách phần `host:port` không có `http://` cho DuckDB Secret |
| `S3_ACCESS_KEY` / `S3_SECRET_KEY`                                | (không commit)                                                              | MinIO, Spark, DuckDB, dbt                                                                      |
| `S3_BUCKET`                                                      | `lakehouse`                                                                 | tất cả                                                                                         |
| `S3_REGION`                                                      | `us-east-1`                                                                 | Spark/DuckDB                                                                                   |
| `DATA_ROOT`                                                      | `D:/docker_data/cpc_lakehouse`                                              | compose bind mount                                                                             |
| `SPEED_FACTOR`                                                   | `60`                                                                        | producer                                                                                       |
| `SIM_START_DATE`                                                 | `2026-01-01`                                                                | producer (ánh xạ thời gian)                                                                    |
| `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID` / `DISCORD_WEBHOOK_URL` | (không commit)                                                              | alert-worker                                                                                   |
| `ALERT_DRY_RUN`                                                  | `true`                                                                      | alert-worker (mặc định chỉ in ra console)                                                      |

Cổng tham khảo: Redpanda Kafka ngoài `19092`, Admin API `9644`, Redpanda Console `8080`, MinIO S3 API `9000`, MinIO Console `9001`, Streamlit `8501`.

## 4.6 Cấu trúc thư mục repo

```
cpc-adtech-lakehouse/
├── .env.example
├── .gitignore
├── .pre-commit-config.yaml
├── .github/workflows/ci.yml
├── Makefile
├── docker-compose.yml
├── README.md
├── config/
│   ├── alerts.yaml                # ngưỡng, cooldown, kênh gửi
│   └── pipeline.yaml              # trigger interval, watermark, topic, đường dẫn bảng
├── infra/
│   ├── spark/Dockerfile           # base image + jars pinned + ivy cache
│   └── redpanda/                  # script tạo topic (nếu cần)
├── scripts/
│   ├── profile_tracking.py        # Task 2.1 (DuckDB quét & phân loại toàn bộ 17 file CSV)
│   ├── s3_conformance.py          # Task 1.2
│   ├── generate_budget_seed.py    # Task 4.2
│   ├── disk_report.sh             # Task 1.5
│   └── bench/                     # Task 6.4
├── src/
│   ├── common/                    # config loader, schemas/, time_utils, logging
│   ├── producer/                  # replay_producer.py, fault_injector.py, time_mapper.py
│   ├── streaming/                 # run_all.py, spark_session.py, ingest.py, refine.py,
│   │                              # validation.py, maintenance.py, listener.py
│   ├── dashboard/                 # app.py, db.py, queries/*.sql, panels/*.py, alerting_view.py
│   └── alerting/                  # worker.py, rules.py, state.py, sinks.py
├── dbt_cpc/                       # models/, seeds/, tests/, macros/, profiles.yml
├── tests/                         # unit/, integration/, fixtures/
└── docs/
    ├── architecture.md
    ├── data_dictionary.md         # Task 2.1
    ├── versions.md                # phiên bản + ma trận tương thích + Deviations
    ├── code_map.md                # khái niệm ➔ file:dòng (Phụ lục A)
    ├── adr/                       # ADR-001 ... (mẫu ở Phụ lục A)
    ├── benchmarks/
    ├── progress/                  # output AC của từng task
    └── lessons/                   # F01..F16, T01..T09, epic labs

```

## 4.7 Danh mục ADR (Architecture Decision Records) cần viết

| ADR | Quyết định                                                                                        | Tạo ở task |
| --- | ------------------------------------------------------------------------------------------------- | ---------- |
| 001 | Dùng MinIO (pin version) làm S3-compatible object store với cấu hình tách rời qua biến môi trường | 1.2        |
| 002 | Redpanda thay Kafka+Zookeeper cho môi trường local                                                | 1.3        |
| 003 | Delta Lake làm table format                                                                       | 3.1        |
| 004 | Serving trực tiếp DuckDB thay vì OLAP server                                                      | 5.1        |
| 005 | Partition key của `cpc_events_raw`                                                                | 2.3        |
| 006 | Chính sách watermark, dedup và dữ liệu đến trễ                                                    | 3.7        |
| 007 | Tiền dùng DECIMAL (serialize dạng string trong JSON), thời gian UTC                               | 2.2        |
| 008 | Bind mount ổ D vs di chuyển Docker disk image                                                     | 1.5        |
| 009 | Quy tắc tính `spend` (kết quả của H1)                                                             | 2.1        |
| 010 | Nguồn ngân sách campaign (từ `campaign.csv` hoặc mô phỏng) và đường pacing lý tưởng               | 4.2        |

# 5. EPICS VÀ TASKS

> Mã `Learn:` trỏ tới module ở Phần 7 (F01–F16, nền tảng) và Phần 8 (T01–T09, công cụ).
> Thứ tự khuyến nghị: Epic 1 → 2 → 3 → 4 → 5 → 6 (Epic 7 là tuỳ chọn). Task 3.1 (kiểm tra tương thích Spark ↔ Delta ↔ DuckDB ↔ MinIO) là rủi ro lớn nhất, nên làm sớm ngay sau Epic 1.

## 🚀 EPIC 1 — Hạ tầng và Quản trị tài nguyên

**Mục tiêu:** dựng môi trường Docker gọn (< 4.5GB RAM), dữ liệu và log không làm phình ổ C, mọi thứ khởi động bằng 1 lệnh.
**Rủi ro chính:** cú pháp endpoint S3 khác nhau giữa Spark và DuckDB; I/O bind mount trên NTFS chậm; retention của Redpanda không kích hoạt nếu segment chưa đóng.

### Task 1.1 — Khởi tạo repo và khung dự án \[S\] \[Depends: —\] \[x\]

**Goal:** có xương sống chuẩn để mọi task sau chỉ việc thêm code, có lint/test từ đầu.
**Steps:**

1. Tạo cây thư mục theo mục 4.6 (thư mục rỗng dùng `.gitkeep`).

2. `.gitignore` phải chặn: `.env`, `data/`, `.venv/`, `__pycache__/`, `dbt_cpc/target/`, `dbt_cpc/logs/`, `*.duckdb`, thư mục `DATA_ROOT`.

3. `.env.example` liệt kê đủ biến ở mục 4.5 (giá trị mẫu, không có secret thật).

4. `pyproject.toml`: cấu hình `ruff` (line-length 100) và `pytest`; `.pre-commit-config.yaml` chạy ruff.

5. `Makefile` với target `help` (liệt kê các target), các target khác để trống có chú thích (điền dần).

6. `src/common/config.py`: nạp `.env` + `config/pipeline.yaml`, trả về đối tượng cấu hình có type; **không** hard-code endpoint/credential. **Bắt buộc có property `s3_endpoint_host_port`** (tự động cắt bỏ `http://` hoặc `https://` khỏi `S3_ENDPOINT`, ví dụ `http://minio:9000` ➔ `minio:9000`) và `s3_use_ssl: bool` để dùng riêng cho câu lệnh `CREATE SECRET` của DuckDB và `dbt-duckdb`.

7. `docs/` khung rỗng + `docs/adr/_TEMPLATE.md` + `docs/versions.md` (bảng trống: thành phần, phiên bản, ngày, ghi chú) + `docs/code_map.md` (tiêu đề bảng).
   **Deliverables:** các file trên, 1 test mẫu `tests/unit/test_config.py` (kiểm tra cả `s3_endpoint` và `s3_endpoint_host_port`).
   **AC:** `make help` hiển thị danh sách target · `ruff check .` và `pytest -q` đều pass · `git ls-files | grep -E '\.env$'` không ra kết quả · `python -c "from src.common.config import load; print(load().s3_endpoint_host_port)"` in ra `minio:9000` (không có `http://`).
   **Pitfalls:** commit nhầm `.env`; để Windows line endings làm hỏng shell script (đặt `.gitattributes` với `*.sh text eol=lf`); truyền nguyên `http://minio:9000` vào DuckDB `CREATE SECRET` gây lỗi kết nối.
   **Learn:** T08, T09.

### Task 1.2 — Xác nhận MinIO Conformance và viết ADR-001 \[S\] \[Depends: 1.1\] \[x\]

**Goal:** ghi lại quyết định dùng MinIO (ADR-001) và có bộ test kiểm chứng S3 API + đo hiệu năng I/O cơ bản trên máy cá nhân.
**Steps:**

1. Chọn một bản tag `minio/minio` và `minio/mc` ổn định, ghi rõ vào `docs/versions.md`.

2. Viết `scripts/s3_conformance.py` (dùng `boto3` đọc cấu hình từ `.env`) kiểm tra: path-style addressing; PUT/GET/HEAD/DELETE; ListObjectsV2 có phân trang; multipart upload (≥ 32MB); ghi đè object; ghi đồng thời nhiều object. In bảng PASS/FAIL và thời gian thực thi.

3. Đo: RAM idle của container `minio` (`docker stats`), thời gian ghi nhiều file nhỏ 1MB và vài file lớn, thời gian list 1.000 key.

4. Viết `docs/adr/001-object-store.md`: ghi rõ lý do chọn MinIO cho môi trường local (ổn định, quen thuộc, có sẵn Web Console cổng 9001, RAM nhẹ) và nguyên tắc **decoupling qua biến môi trường S3** để có thể thay bằng SeaweedFS/Garage/AWS S3 khi cần mà không sửa code.
   **Deliverables:** `scripts/s3_conformance.py`, `docs/adr/001-object-store.md`, `docs/progress/1.2.md`.
   **AC:** `make s3-check` (chạy sau khi dựng compose ở 1.3) in toàn bộ PASS · RAM idle của MinIO ≤ 448MB · ADR-001 hoàn chỉnh.
   **Pitfalls:** quên `path-style`; sai `region` làm SDK ký request lỗi.
   **Learn:** F08, T04.

### Task 1.3 — Docker Compose cho nhóm `core` \[M\] \[Depends: 1.1, 1.2\] \[x\]

**Goal:** `make up` dựng Redpanda, Redpanda Console, MinIO, khởi tạo bucket `lakehouse`; có healthcheck và giới hạn tài nguyên.
**Steps:**

1. Services: `redpanda`, `redpanda-console`, `minio`, `minio-init` (dùng `minio/mc` tạo bucket `lakehouse` rồi thoát).

2. Mỗi service: `mem_limit` theo mục 4.4; `logging: {driver: json-file, options: {max-size: "10m", max-file: "3"}}`; `restart: unless-stopped` (trừ `minio-init`); cổng chỉ bind `127.0.0.1`.

3. Healthcheck + `depends_on: condition: service_healthy` để `minio-init` chỉ chạy khi `minio` sẵn sàng.

4. Dùng **profiles**: `core` (luôn bật), `apps` (spark, dashboard, alert-worker), `tools` (producer, dbt).

5. Image pin phiên bản cụ thể và ghi vào `docs/versions.md`.

6. Viết ADR-002 (Redpanda).
   **Snippet minh hoạ (Redpanda & MinIO dev, đối chiếu docs của phiên bản đã pin):**

```
redpanda:
  image: redpandadata/redpanda:<PIN_VERSION>
  mem_limit: 704m
  command:
    - redpanda start
    - --mode dev-container
    - --smp 1
    - --memory 512M
    - --reserve-memory 0M
    - --overprovisioned
    - --kafka-addr internal://0.0.0.0:9092,external://0.0.0.0:19092
    - --advertise-kafka-addr internal://redpanda:9092,external://localhost:19092
  ports: ["127.0.0.1:19092:19092", "127.0.0.1:9644:9644"]
  volumes: ["${DATA_ROOT}/redpanda:/var/lib/redpanda/data"]
  logging: { driver: json-file, options: { max-size: "10m", max-file: "3" } }

minio:
  image: minio/minio:<PIN_VERSION>
  mem_limit: 448m
  command: server /data --console-address ":9001"
  environment:
    MINIO_ROOT_USER: ${S3_ACCESS_KEY}
    MINIO_ROOT_PASSWORD: ${S3_SECRET_KEY}
  ports: ["127.0.0.1:9000:9000", "127.0.0.1:9001:9001"]
  volumes: ["${DATA_ROOT}/minio:/data"]
  healthcheck:
    test: ["CMD", "mc", "ready", "local"]
    interval: 5s
    timeout: 5s
    retries: 5
  logging: { driver: json-file, options: { max-size: "10m", max-file: "3" } }

```

**AC:** `make up` ➔ mọi service `core` ở trạng thái `healthy` trong ≤ 90s · Redpanda Console mở được ở `http://localhost:8080`, MinIO Console mở được ở `http://localhost:9001` · `python scripts/s3_conformance.py` pass và thấy bucket `lakehouse` · tổng RAM nhóm core (`docker stats --no-stream`) < 1.6GB.
**Pitfalls:** `advertise-kafka-addr` sai khiến producer ngoài host kết nối được nhưng không gửi được; chạy hai bản `up` song song làm tranh cổng.
**Learn:** T01, T04, T08, F07.

### Task 1.4 — Cấu hình retention và dọn dữ liệu cũ của Redpanda \[S\] \[Depends: 1.3\] \[x\]

**Goal:** broker không ăn dần ổ đĩa, nhưng không làm mất dữ liệu trước khi Spark kịp đọc.
**Steps:**

1. Đặt mặc định cluster `log_retention_ms=3600000` (1 giờ) bằng `rpk cluster config set ...`; chi tiết topic đặt ở Task 2.4.

2. **Hiểu và xử lý bẫy segment:** retention chỉ xoá các segment _đã đóng_. Với lượng dữ liệu nhỏ, segment đang ghi có thể không bao giờ đóng ➔ dữ liệu không bị xoá. Đặt `segment.ms` ngắn (ví dụ 10 phút) cho topic.

3. Viết vào `docs/progress/1.4.md`: nếu Spark dừng > 1 giờ thì offset hết hạn; ghi rõ chính sách (dev: `failOnDataLoss=false` + log cảnh báo; Bronze là nguồn sự thật nên chấp nhận).
   **AC:** `rpk cluster config get log_retention_ms` = 3600000 · Lab L0 (xem Phần 6) chứng minh dữ liệu cũ thực sự bị xoá sau khi segment được đóng.
   **Pitfalls:** nhầm retention với compaction; retention quá ngắn khi debug.
   **Learn:** T01, F07, F16.

### Task 1.5 — Kỷ luật ổ đĩa trên Windows/WSL2 \[M\] \[Depends: 1.3\] \[x\]

**Goal:** đo, chọn và chứng minh chiến lược không làm đầy ổ C.
**Steps:**

1. Ghi baseline: dung lượng trống ổ C và D, kích thước file đĩa ảo của Docker/WSL2 (`ext4.vhdx`/`docker_data.vhdx`), `docker system df`.

2. **Chiến lược A — Bind mount** toàn bộ dữ liệu có trạng thái (Redpanda, MinIO, ivy cache của Spark, extension cache của DuckDB) sang `${DATA_ROOT}` trên ổ D.

3. **Chiến lược B — Đổi vị trí Docker disk image** sang ổ D trong cài đặt Docker Desktop (Resources ➔ Advanced). Khi dùng B có thể giữ named volume, nhanh hơn bind mount NTFS.

4. Đo I/O thực tế của A so với B (ghi nhiều file nhỏ qua `s3_conformance.py`), vì Delta tạo nhiều file nhỏ. Viết ADR-008 và chọn.

5. Viết `scripts/disk_report.sh` (+ `make disk-report`): dung lượng trống C/D, `docker system df`, kích thước `DATA_ROOT`, số file trong thư mục Delta.

6. Thêm `make clean` (dừng + dọn dangling an toàn) và `make nuke-data` (xoá dữ liệu, **bắt buộc xác nhận** gõ chữ).

7. Gợi ý `.wslconfig` (giới hạn RAM/CPU, `sparseVhd`) ghi vào `docs/progress/1.5.md`.
   **AC:** sau `make up` + chạy producer 10 phút, dung lượng trống ổ C giảm < 200MB · dữ liệu xuất hiện dưới `DATA_ROOT` ở ổ D · báo cáo `make disk-report` được lưu.
   **Pitfalls:** quyền sở hữu file trên NTFS; đường dẫn `D:/` và `/mnt/d` khác nhau tuỳ ngữ cảnh; file `ext4.vhdx` chỉ phình, không tự co nếu không bật `sparseVhd` hoặc compact thủ công.
   **Learn:** T08, F08.

### Task 1.6 — Đo baseline tài nguyên \[S\] \[Depends: 1.3\] \[x\]

**Goal:** có số thật để so với ngân sách 4.4.
**Steps:** viết `scripts/bench/resource_snapshot.sh` lấy mẫu `docker stats` mỗi 5s ra CSV; chạy 5 phút khi idle; so với bảng 4.4.
**AC:** `docs/benchmarks/baseline.md` có RAM từng service so với ngân sách + ghi chú lệch (R14).
**Learn:** F14.

**Lab L0 (thí nghiệm hạ tầng):** dừng và khởi động lại container, chứng minh dữ liệu còn nguyên ở ổ D; tạo log lớn để thấy log rotation; chứng minh retention (xem Phần 6).

## 📨 EPIC 2 — Giả lập luồng dữ liệu và Ingestion

**Mục tiêu:** bộ phát dữ liệu có thể tua nhanh thời gian, cố tình bơm dữ liệu bẩn, và ghi lại **sự thật tuyệt đối** (manifest) để mọi bước sau có thể đối soát.
**Rủi ro chính:** bỏ sót bảng quan trọng hoặc ôm đồm quá nhiều bảng phụ trong 17 file CSV; đồng hồ mô phỏng sai làm hỏng cả watermark/pacing; thiết kế khoá partition gây lệch.

### Task 2.1 — Khảo sát 17 file CSV, phân loại và chốt Data Dictionary \[L\] \[Depends: 1.1\] \[x\]

**Goal:** biết bộ 17 file CSV trong `data/raw/` _thực sự_ chứa gì, file nào dùng cho streaming, file nào dùng làm bảng dimension/seed, và file nào loại bỏ để giữ gọn phạm vi dự án (R1, K14). Đây là cổng bắt buộc của Epic 2–4.
**Steps:**

1. Đặt toàn bộ 17 file CSV vào `data/raw/` (git-ignored). Hỏi chủ dự án trước khi commit bất kỳ dữ liệu nào.

2. Viết `scripts/profile_tracking.py` dùng **DuckDB đọc trực tiếp các file CSV** trong `data/raw/` theo 2 giai đoạn:
   - **Giai đoạn 1 — Inventory & Triage (Quét tổng thể 17 file):** Với mỗi file trong `data/raw/*.csv`, xuất ra: kích thước file, số dòng (`COUNT(*)`), danh sách cột & kiểu dữ liệu suy ra (`DESCRIBE`), tỷ lệ null của các cột khoá (`id`, `campaign_id`, `job_id`, `publisher_id`, `group_id`, `company_id`), và 3 dòng mẫu.

   - **Giai đoạn 2 — Deep Profile (Đào sâu các file trọng tâm):**
     - Phân tích luồng sự kiện trên `tracking.csv` (và đối chiếu với `application.csv`, `events.csv`, `search.csv`): khoảng thời gian dữ liệu, số event theo ngày/giờ, khoảng trống, thứ tự thời gian, phân phối các loại sự kiện (`impression`, `click`, `apply`, `qualified`, `unqualified`), số dòng trùng hoàn toàn.

     - Kiểm tra `campaign.csv`: có cột ngân sách (`daily_budget` / `budget`) và giá thầu (`bid`) hay không, độ phủ `campaign_id` so với `tracking.csv`.

     - Kiểm tra quan hệ khoá ngoại giữa `tracking.csv` với `campaign.csv`, `job.csv`, `search.csv`, `master_publisher.csv`, `company.csv`, `group.csv`.

3. Lập **Bảng phân loại 17 file CSV** thành 3 nhóm rõ ràng:
   - **Nhóm A — Event Stream Source** (đưa vào Replay Producer bắn qua Redpanda, ví dụ `tracking.csv` hoặc kết hợp `application.csv`).

   - **Nhóm B — Dimension / Seed Tables** (nạp vào dbt/Silver để làm giàu dữ liệu cho 3 bảng Gold và hiển thị tên thật trên Dashboard, ví dụ `campaign.csv`, `job.csv`, `master_publisher.csv`, `company.csv`, `group.csv`, `events.csv`).

   - **Nhóm C — Ignore / Out-of-Scope** (các file quản trị user, phân quyền, chat, danh mục thành phố chi tiết như `dev_user.csv`, `user_role.csv`, `master_role.csv`, `user_client_assignee.csv`, `conversation.csv`, `master_city.csv`... không cần cho bài toán P1–P4), ghi rõ lý do loại bỏ.

4. Trả lời từng mục **U0–U9, H1, H4** (mục 3.2): mỗi mục ghi **kết luận + câu SQL bằng chứng**.

5. Viết `docs/data_dictionary.md` và ADR-009 (quy tắc tính `spend`).

6. **Dừng lại báo cáo bảng phân loại 17 file và các câu hỏi còn mơ hồ để chủ dự án duyệt** trước khi viết code Task 2.2 và Producer.
   **Deliverables:** `scripts/profile_tracking.py`, `docs/data_dictionary.md` (có bảng phân loại 17 file), `docs/adr/009-spend-rule.md`.
   **AC:** `make profile` quét hết 17 file, chạy lại được và sinh cùng kết quả · bảng phân loại đủ 17 file kèm lý do Dùng/Bỏ · mọi U0–U9/H1/H4 có verdict kèm bằng chứng SQL · chủ dự án đã duyệt danh sách file sử dụng.
   **Pitfalls:** ôm hết cả 17 file vào pipeline làm phình dự án; bỏ sót `application.csv` hoặc `campaign.csv` khi trong đó chứa hồ sơ ứng tuyển hoặc ngân sách thật; múi giờ ngầm định; tiền biểu diễn bằng số thực.
   **Learn:** F12, F13, T05.

### Task 2.2 — Data Contract và hàm validate dùng chung \[M\] \[Depends: 2.1\] \[x\]

**Goal:** một định nghĩa duy nhất cho event hợp lệ, test được mà không cần Kafka/Spark.
**Steps:**

1. Chốt tên trường theo kết quả 2.1; viết `src/common/schemas/cpc_event.v1.json` (JSON Schema) và model Pydantic (hoặc dataclass) `cpc_event.py`. **Lưu ý bắt buộc:** trong JSON Schema, `bid` và `spend` phải có `type: "string"` với `pattern` số thập phân để không mất độ chính xác trước khi vào Spark.

2. Viết hàm thuần `validate_event(raw: str) -> ValidationResult` trả về danh sách lỗi theo bộ `error_type` ở mục 3.3 (bao gồm cả `malformed_decimal` và `negative_amount`).

3. Viết `src/common/schemas/validation_rules.md` mô tả từng quy tắc bằng lời + ví dụ đúng/sai (sẽ dùng để viết bản Spark ở Task 3.3 và test chéo).

4. ADR-007: tiền dùng `DECIMAL(18,4)`, thời gian UTC, bắt buộc serialize decimal trong JSON dưới dạng chuỗi (`string`).
   **AC:** `pytest tests/unit/test_contract.py` pass, mỗi `error_type` có ít nhất 2 fixture (1 hợp lệ biên, 1 vi phạm) · truyền float hoặc chuỗi không phải số vào `bid`/`spend` bị bắt đúng lỗi · schema JSON được validate bằng thư viện chuẩn.
   **Pitfalls:** dùng float cho tiền trong JSON; chấp nhận field thừa/thiếu không rõ ràng; không version hoá schema.
   **Learn:** F13, F06.

### Task 2.3 — Phân tích lệch khoá partition (ADR-005) \[S\] \[Depends: 2.1\] \[x\]

**Goal:** chọn `campaign_id` hay `job_id` làm Kafka key dựa trên số liệu.
**Steps:** dùng DuckDB tính tỷ lệ event theo từng ứng viên khoá (`campaign_id`, `job_id`, `publisher_id`); mô phỏng chia 3 partition bằng `hash(key) % 3` (gần đúng với partitioner thực); đo tỷ lệ lớn nhất của một partition và của top-1 khoá.
**AC:** `docs/adr/005-partition-key.md` có bảng số liệu (max partition share, top-1 key share) cho từng ứng viên và kết luận; nếu một khoá chiếm > 40% sự kiện thì giải thích hậu quả (hot partition).
**Learn:** F07.

### Task 2.4 — Tạo topic \[S\] \[Depends: 1.4, 2.3\] \[x\]

**Steps:** viết script idempotent (`make topics`) dùng `rpk` tạo `cpc_events_raw` (3 partition) và `cpc_events_dlq` (1 partition), replication 1, `retention.ms=3600000`, `segment.ms=600000`, giới hạn `retention.bytes` hợp lý.
**AC:** `rpk topic describe cpc_events_raw` khớp cấu hình · chạy `make topics` hai lần không lỗi.
**Learn:** T01, F07.

### Task 2.5 — Replay Producer v1 (chưa bơm lỗi) \[L\] \[Depends: 2.2, 2.4\] \[x\]

**Goal:** phát lại dữ liệu theo đúng nhịp thời gian, ổn định, có thể đo.
**Steps:**

1. `src/producer/time_mapper.py` (hàm thuần): `sim_time = SIM_START + (orig_time - orig_t0)`; thời điểm gửi thật `= t_start + (orig_time - orig_t0) / speed_factor`. Test bằng ví dụ số: `speed_factor=60` ➔ 1 phút thật = 1 giờ dữ liệu.

2. Sắp xếp dữ liệu theo thời gian gốc (nếu chưa sắp) bằng DuckDB ghi ra Parquet tạm; **đọc theo luồng/chunk**, không nạp cả file vào RAM.

3. CLI `replay_producer.py`: `--file`, `--speed-factor`, `--max-rate`, `--start-row`, `--seed`, `--dry-run`, `--no-faults`.

4. Cấu hình `confluent-kafka` an toàn: `enable.idempotence=true`, `acks=all`, nén `lz4`, `linger.ms` nhỏ.

5. Lên lịch gửi theo **deadline tuyệt đối** (không `sleep` cố định giữa message để tránh trôi); nếu tụt lịch, log `behind_schedule_ms`.

6. Gán `event_id`, `produced_at`, `source_row_id`, định dạng `bid` và `spend` thành chuỗi thập phân 4 chữ số sau dấu phẩy (mục 3.3); khoá message = `campaign_id` (hoặc kết luận của 2.3).

7. Delivery callback đếm ack/lỗi; `poll(0)` đều đặn; xử lý `BufferError` bằng `poll` rồi thử lại; `flush()` khi thoát; bắt SIGINT để thoát sạch và in tóm tắt.

8. Log tiến độ mỗi 5s: đã gửi, msg/s, độ trễ so với lịch.
   **Deliverables:** `src/producer/{time_mapper,replay_producer}.py`, tests.
   **AC:** chạy `--speed-factor 600` trong 2 phút: số message trong topic (tổng offset các partition) = số dòng đã gửi · `event_time` không giảm trong luồng · thoát bằng Ctrl+C có tóm tắt, không mất message đã ack.
   **Pitfalls:** `sleep` từng message làm lệch giờ; GIL làm throughput thấp (ghi nhận, không tối ưu sớm); múi giờ; quên `flush`.
   **Learn:** F01, F02, F06, F07, T01, T09.

### Task 2.6 — Fault Injection và Manifest sự thật \[M\] \[Depends: 2.5\] \[x\]

**Goal:** bơm dữ liệu bẩn **có kiểm soát** và biết chính xác đã bơm gì.
**Steps:**

1. `src/producer/fault_injector.py` (hàm thuần + RNG có seed):
   - **Duplicate 2%:** gửi lại **y nguyên** message (cùng `event_id`) sau độ trễ ngẫu nhiên 0–60 giây mô phỏng.

   - **Late 2%:** giữ `event_time` gốc nhưng trì hoãn việc gửi 5–15 phút mô phỏng (đồng hồ chạy tiếp rồi mới gửi).

   - **Corrupt 1%:** chia đều các loại: JSON cụt/hỏng ➔ `corrupt_json`; thiếu trường bắt buộc ➔ `missing_required`; `event_type` lạ ➔ `bad_enum`; `spend` không phải số ➔ `malformed_decimal`; `spend` âm ➔ `negative_amount`; `event_time` sai định dạng ➔ `bad_timestamp`.

2. Ghi **manifest** `data/manifests/replay_<run_id>.json`: `rows_read`, `sent_valid_unique`, `injected_duplicates`, `injected_late` (kèm số lượng trễ > 10 phút), `injected_corrupt` theo `error_type`, `expected_unique_valid_events`, `expected_spend_total` (decimal), `seed`, `speed_factor`.

3. Quy tắc: các message hỏng **không** được tính vào `expected_unique_valid_events`.
   **AC:** cùng `seed` chạy 2 lần ➔ manifest giống hệt · trên 100.000 dòng, tỷ lệ mỗi loại sai lệch ≤ 0.5 điểm phần trăm so với cấu hình · unit test cho từng hàm inject.
   **Pitfalls:** event hỏng nhưng vẫn bị đếm là hợp lệ; "late" tính theo đồng hồ thật thay vì đồng hồ mô phỏng.
   **Learn:** F04, F13, F06.

### Task 2.7 — Kiểm thử producer và quan sát trên Console \[S\] \[Depends: 2.6\] \[x\]

**Steps:** viết test `time_mapper`, tỷ lệ inject, xác định manifest; chạy thật rồi dùng Redpanda Console xem phân phối message theo partition và so với kết quả 2.3.
**AC:** `pytest tests/unit/producer` pass · ảnh chụp/ghi chú phân phối partition trong `docs/progress/2.7.md`.
**Lab L2:** dừng producer giữa chừng rồi chạy lại với `--start-row` lệch để thấy trùng message ➔ hiểu vì sao hệ thống phía sau phải idempotent (liên hệ F06).

## ⚡ EPIC 3 — Streaming và Kiến trúc Medallion (PySpark + Delta Lake)

**Mục tiêu:** trái tim kỹ thuật. Đọc luồng từ Redpanda, bắt dữ liệu bẩn vào DLQ, khử trùng, ghi Delta Lake trên MinIO **idempotent**, và có công cụ **chứng minh** số liệu đúng.
**Rủi ro chính:** (1) tương thích phiên bản Spark ↔ Delta ↔ hadoop-aws ↔ DuckDB; (2) hiểu sai watermark ➔ mất event im lặng; (3) hàng nghìn file nhỏ và xung đột giữa `OPTIMIZE` với luồng `MERGE`; (4) ghi không idempotent ➔ nhân đôi tiền.

### Task 3.1 — Spark runtime và kiểm tra tương thích (smoke test liên thông) \[L\] \[Depends: 1.3\] \[x\]

**Goal:** loại bỏ rủi ro lớn nhất _trước khi_ viết logic: Spark ghi Delta lên MinIO ➔ DuckDB đọc được, kể cả khi đang ghi.
**Steps:**

1. `infra/spark/Dockerfile` dựa trên image Spark chính thức (pin phiên bản). Cài `delta-spark` (pip) cùng phiên bản khớp Spark; **nướng sẵn JAR** (Kafka connector, Delta, hadoop-aws và bundle AWS SDK đúng với phiên bản Hadoop đi kèm Spark) vào image để chạy lặp lại được và không tải lúc chạy. Cache Ivy/Maven trỏ sang ổ D.

2. Lập **ma trận tương thích** trong `docs/versions.md`: Spark, Scala, Delta, hadoop-aws, AWS SDK, DuckDB + delta extension, MinIO. Nguồn: release notes của từng công cụ.

3. Viết `tests/integration/smoke_delta_interop.py`:
   - (a) Spark ghi bảng Delta nhỏ lên `s3a://lakehouse/_smoke/t1` qua 3 commit (append, append, merge).

   - (b) DuckDB `delta_scan('s3://lakehouse/_smoke/t1')` đọc ra đúng số dòng và giá trị.

   - (c) Spark chạy vòng lặp 20 commit trong khi DuckDB đọc liên tục: không lỗi, không đọc phải trạng thái nửa vời.

   - (d) Kiểm tra DuckDB đọc được với **table features** mà Spark bật (nếu bật deletion vectors hay tính năng mới, DuckDB phiên bản đã pin có thể không đọc được ➔ tắt tính năng đó ở bảng, hoặc nâng phiên bản).

4. ADR-003: vì sao Delta Lake (so sánh ngắn với Iceberg/Hudi, ghi rõ giới hạn của bản DuckDB đã pin).
   **Deliverables:** `infra/spark/Dockerfile`, `docs/versions.md` (ma trận), `tests/integration/smoke_delta_interop.py`, `docs/adr/003-delta.md`.
   **AC:** `make smoke-interop` pass cả 4 mục · `make spark-shell` mở được · container Spark idle < 1GB RAM · build image lặp lại hai lần cho cùng kết quả.
   **Pitfalls:** lệch phiên bản Scala (`_2.12` vs `_2.13`); thiếu SDK AWS khiến `s3a` báo lỗi lớp không tìm thấy; thiếu `path.style.access`; DuckDB đòi extension `httpfs` + secret S3 dùng `cfg.s3_endpoint_host_port` (không kèm `http://`).
   **Learn:** T02, T03, T05, F10.

### Task 3.2 — SparkSession factory và cấu hình \[M\] \[Depends: 3.1\]

**Goal:** một nơi duy nhất cấu hình Spark, phù hợp máy 16GB.
**Steps:** `src/streaming/spark_session.py` dựng session với:

- `master=local[2]`, bộ nhớ driver theo mục 4.4; **`spark.sql.shuffle.partitions=4`** (mặc định 200 là lãng phí ở local và tạo nhiều file nhỏ).

- Extension và catalog của Delta; `spark.sql.session.timeZone=UTC`.

- `s3a`: endpoint (`cfg.s3_endpoint`), `path.style.access=true`, credentials từ biến môi trường, tắt SSL cho MinIO local.

- Cân nhắc **RocksDB state store** để giảm áp lực heap khi state lớn (đo trước, bật sau; ghi ADR nhỏ trong `docs/progress/3.2.md`).

- Tham số từ `config/pipeline.yaml`: trigger, watermark, `maxOffsetsPerTrigger`, đường dẫn bảng/checkpoint.
  **AC:** test đơn vị dựng session với đường dẫn local (không S3) và ghi/đọc Delta thành công · `make spark-shell` liệt kê được object trong bucket `lakehouse` trên MinIO · không có credential trong log.
  **Learn:** T02, T08.

### Task 3.3 — Q1 Ingest: parse, validate, định tuyến Bronze/DLQ \[L\] \[Depends: 2.2, 2.4, 3.2\]

**Goal:** mọi message đi đúng một trong hai nơi: Bronze (hợp lệ) hoặc DLQ (lỗi), không mất và không trùng.
**Steps:**

1. Đọc Kafka: `startingOffsets=earliest` (lần đầu), `maxOffsetsPerTrigger` (giới hạn để bảo vệ RAM), `failOnDataLoss` theo chính sách ở Task 1.4.

2. Parse bằng `from_json` với schema **tường minh** (trong đó `bid` và `spend` nhận vào kiểu `StringType`), chế độ `PERMISSIVE` và `columnNameOfCorruptRecord`.

3. `src/streaming/validation.py`: `validate_df(df) -> (valid_df, invalid_df)` (hàm `DataFrame → DataFrame`) biểu diễn **đúng các quy tắc** của `validation_rules.md` (kiểm tra cả việc ép kiểu `bid`/`spend` từ `StringType` sang `DECIMAL(18,4)` có bị null hay âm không); gắn `error_type` và `error_detail` cho bản ghi lỗi.

4. **Test chéo (parity):** chạy cùng tập fixture qua validator Python (Task 2.2) và `validate_df`; kết luận phải giống nhau từng bản ghi.

5. `foreachBatch(process_batch)`: `persist()` DataFrame của batch (dùng 2 lần), ghi:
   - **Bronze** (`append`, partition `event_date`) kèm `payload_raw`, `kafka_*`, `ingested_at`.

   - **DLQ Delta** (`dlq/cpc_events_dlq`) và **topic `cpc_events_dlq`**.

   - Dùng ghi Delta **idempotent** bằng `txnAppId` + `txnVersion=batch_id`, nên retry cùng batch không nhân đôi. Ghi sang Kafka DLQ là at-least-once (chấp nhận và ghi rõ).

   - `unpersist()` ở cuối.

6. `trigger(processingTime="10 seconds")`, checkpoint `s3a://lakehouse/checkpoints/ingest`.
   **Snippet minh hoạ (pattern, không copy mù quáng):**

```
def process_batch(batch_df, batch_id: int):
    batch_df.persist()
    valid, invalid = validate_df(batch_df)
    (valid.write.format("delta").mode("append")
        .option("txnAppId", "ingest_bronze").option("txnVersion", batch_id)
        .partitionBy("event_date").save(BRONZE_PATH))
    (invalid.write.format("delta").mode("append")
        .option("txnAppId", "ingest_dlq").option("txnVersion", batch_id)
        .save(DLQ_PATH))
    publish_to_kafka_dlq(invalid)   # at-least-once
    batch_df.unpersist()

```

**AC:** replay 100.000 dòng có lỗi: **số dòng DLQ theo `error_type` = manifest** · Bronze = (hợp lệ duy nhất + trùng + trễ), tức mọi message hợp lệ đã gửi · không có `kafka_offset` nào xuất hiện hai lần trong Bronze · parity test pass.
**Pitfalls:** bản ghi JSON hợp lệ nhưng thiếu trường **không** làm `from_json` báo corrupt (phải bắt bằng quy tắc `missing_required`); `event_date` null làm hỏng partition; quên `persist` khiến batch bị tính hai lần.
**Learn:** F06, F13, T02, T03.

### Task 3.4 — Q2 Refine: Silver chuẩn hoá và khử trùng \[L\] \[Depends: 3.3\]

**Goal:** Silver có **đúng một dòng cho mỗi `event_id`**, kiểu dữ liệu chuẩn, ghi idempotent.
**Steps:**

1. `readStream` từ **bảng Delta Bronze** (`maxFilesPerTrigger` hoặc `maxBytesPerTrigger`).

2. Chuẩn hoá: `event_time` ➔ `TIMESTAMP` (UTC), `spend`/`bid` (từ string) ➔ `DECIMAL(18,4)`; thêm `processed_at = current_timestamp()`.

3. `withWatermark("event_time", "10 minutes")` + `dropDuplicatesWithinWatermark(["event_id"])` (yêu cầu Spark ≥ 3.5; nếu thấp hơn, dùng `dropDuplicates(["event_id", "event_time"])` kèm watermark và ghi vào _Deviations_).

4. `foreachBatch`: `dropDuplicates(["event_id"])` trong batch, rồi **MERGE chỉ chèn nếu chưa có**, có điều kiện partition để cắt tỉa file:

```
def upsert_silver(batch_df, batch_id: int):
    src = batch_df.dropDuplicates(["event_id"])
    (DeltaTable.forPath(spark, SILVER_PATH).alias("t")
        .merge(src.alias("s"), "t.event_id = s.event_id AND t.event_date = s.event_date")
        .whenNotMatchedInsertAll()
        .execute())

```

5. Bản ghi không chuẩn hoá được ➔ DLQ với `stage='silver'`.

6. Checkpoint `s3a://lakehouse/checkpoints/refine_silver`, trigger 10s.
   **Vì sao vừa dedup trong luồng vừa MERGE?** Dedup theo watermark chỉ nhớ `event_id` trong cửa sổ để **giới hạn state/RAM**. MERGE là lớp bảo đảm **toàn cục** (kể cả bản trùng đến sau cửa sổ, hoặc sau khi restart). Cái giá: event đến trễ hơn watermark có thể bị loại ở bước stateful (xem Task 3.7).
   **AC:** trong Silver, `COUNT(*) = COUNT(DISTINCT event_id)` · tổng `spend` khớp manifest ngoài phần event trễ vượt watermark (đối soát đầy đủ ở Task 3.5) · chạy lại cùng batch không tạo dòng mới.
   **Pitfalls:** quên điều kiện `event_date` trong MERGE ➔ quét cả bảng; MERGE có nhánh `whenMatched` vô tình làm ghi đè; chạy `OPTIMIZE` đồng thời trên cùng partition đang ghi gây xung đột (xem 3.8).
   **Learn:** F03, F04, F05, F06, T02, T03.

### Task 3.5 — Bộ đối soát đúng–sai (Reconciliation Harness) \[M\] \[Depends: 2.6, 3.4\]

**Goal:** biến câu "hệ thống chạy đúng" thành một lệnh có kết quả PASS/FAIL, dựa trên **sự thật từ manifest**.
**Steps:**

1. Bổ sung Task 2.6: producer ghi thêm `data/manifests/replay_<run>.late_ids.jsonl` (mỗi dòng: `event_id`, độ trễ phút) và `corrupt_ids` theo `error_type`.

2. `tests/integration/reconcile.py --manifest <file>` dùng DuckDB `delta_scan` đọc Bronze/Silver/DLQ và kiểm:
   - (a) DLQ theo `error_type` = manifest.

   - (b) Bronze = tổng message hợp lệ đã gửi.

   - (c) Silver không trùng `event_id`.

   - (d) Mọi `event_id` có trong Bronze nhưng **không** có trong Silver phải thuộc danh sách "trễ vượt watermark"; ngược lại ➔ FAIL.

   - (e) `SUM(spend)` Silver = `expected_spend_total` trừ spend của các event bị loại do trễ.

3. In báo cáo JSON + mã thoát khác 0 khi FAIL.
   **AC:** `make reconcile MANIFEST=...` ➔ PASS trên lần chạy sạch · cố tình xoá 1 dòng Silver thủ công ➔ FAIL ở đúng mục (d)/(e).
   **Learn:** F13, F14.

### Task 3.6 — Kiểm thử hồi phục và idempotency (kill/restart) \[M\] \[Depends: 3.5\]

**Goal:** chứng minh không mất, không trùng tiền khi hệ thống chết.
**Kịch bản (mỗi kịch bản kết thúc bằng `make reconcile` PASS):**

- **S1:** `docker kill` container Spark giữa lúc đang xử lý, khởi động lại.

- **S2:** dừng Spark 5 phút (producer vẫn chạy), bật lại: theo dõi catch-up và độ trễ.

- **S3:** (chỉ dev) xoá checkpoint của Q2 để Q2 đọc lại toàn bộ Bronze ➔ MERGE phải ngăn trùng.

- **S4:** restart Redpanda trong lúc producer chạy.
  **AC:** cả 4 kịch bản PASS · thời gian hồi phục S1 ≤ 60s · ghi số đo vào `docs/progress/3.6.md`.
  **Pitfalls:** S3 chỉ an toàn vì MERGE; hiểu vì sao Q1 _không_ được xoá checkpoint nếu không có cơ chế chống trùng khác.
  **Learn:** F06, F16.

### Task 3.7 — Chính sách dữ liệu đến trễ (ADR-006) bằng thực nghiệm \[M\] \[Depends: 3.5\]

**Goal:** quyết định dựa trên số đo: chấp nhận mất bao nhiêu event trễ ở Silver, đổi lại state nhỏ thế nào.
**Steps:** chạy cùng một dữ liệu với watermark 5 / 10 / 20 phút; ghi: số event bị loại do trễ, kích thước state (`numRowsTotal`, `memoryUsedBytes` từ listener ở 3.9), độ trễ. So sánh **P1** (dedup theo watermark + MERGE, loại event trễ ở Silver) với **P3** (bỏ dedup stateful, chỉ MERGE ➔ không mất event trễ, state ≈ 0, MERGE tốn hơn). **P2** (job `reconcile_silver` dựng lại từ Bronze) là hướng mở rộng (Task 7.2).
**AC:** `docs/adr/006-late-data.md` có bảng số đo và kết luận kèm _điều kiện xem xét lại_; mặc định chọn theo dữ liệu đo được (v2 đề xuất bắt đầu bằng P1 vì dạy được watermark, nhưng P3 hợp lệ nếu số đo ủng hộ).
**Learn:** F04, F05.

### Task 3.8 — Compaction và bảo trì Delta an toàn \[M\] \[Depends: 3.4\]

**Goal:** xử lý **small file problem** để truy vấn không chậm dần mà **không gây xung đột với luồng MERGE đang chạy** hoặc làm crash DuckDB reader.
**Steps:**

1. `src/streaming/maintenance.py`:
   - **Tránh xung đột ghi đồng thời (ConcurrentModification):** Когда Q2 đang chạy `MERGE` mỗi 10s trên partition của ngày hiện tại (`current_sim_date`), việc chạy `OPTIMIZE` trên cùng partition từ một thread độc lập sẽ liên tục gây lỗi `ConcurrentAppendException`/`ConcurrentDeleteReadException`. Vì vậy:
     - Với **chế độ tự động định kỳ**: chỉ chạy `OPTIMIZE` kèm điều kiện lọc cho các partition **đã đóng** (`where=f"event_date < '{current_sim_date}'"`), hoặc kích hoạt `OPTIMIZE` ngay bên trong callback sau mỗi N=30 micro-batch của chính Q2 (tuần tự, không đụng độ).

     - Cung cấp thêm lệnh CLI `make compact` để nén toàn bộ bảng khi cần.

   - Bắt các ngoại lệ xung đột Delta và retry có backoff nếu vẫn xảy ra.

2. **Quy tắc an toàn cho `VACUUM`:** Giữ nguyên retention mặc định (168 giờ). **Tuyệt đối không chạy `vacuum(0)` khi Dashboard (DuckDB) hoặc Q2 đang đọc**, vì xoá ngay các file Parquet vừa được gộp sẽ làm các truy vấn đang đọc snapshot cũ bị crash ngay lập tức (`FileNotFound`). Nếu muốn demo giảm dung lượng đĩa, tắt tạm reader hoặc giữ retention tối thiểu `>= 1 giờ`.

3. Đo trước/sau: số file (`DESCRIBE DETAIL`), kích thước trung bình file, và **thời gian truy vấn DuckDB** (ví dụ \~500, \~2.000, \~5.000 file).

4. Chạy `make reconcile` sau khi nén để chứng minh nén không làm mất/trùng dữ liệu; kiểm tra Q2 (đọc stream từ Bronze) không đọc lại dữ liệu sau OPTIMIZE.

```
dt = DeltaTable.forPath(spark, SILVER_PATH)
# Chỉ nén các ngày đã khép lại khi luồng đang chạy để tránh xung đột với MERGE
dt.optimize().where(f"event_date < '{current_sim_date}'").executeCompaction()
dt.vacuum()        # giữ retention mặc định (168h), không dùng vacuum(0) khi đang có reader

```

**AC:** số file giảm rõ rệt, truy vấn nhanh hơn (số đo trong `docs/benchmarks/small_files.md`) · reconcile PASS sau nén · không có ngoại lệ xung đột làm job streaming chết hay làm Dashboard bị lỗi đọc file.
**Learn:** F10, T03.

### Task 3.9 — Quan sát luồng: StreamingQueryListener \[M\] \[Depends: 3.3\]

**Goal:** pipeline tự báo cáo sức khoẻ để Panel 4 hiển thị.
**Steps:** `src/streaming/listener.py` cài `StreamingQueryListener` (PySpark ≥ 3.4). Mỗi progress ghi: tên query, `batchId`, `inputRowsPerSecond`, `processedRowsPerSecond`, thời gian batch, `eventTime.watermark`, số dòng/bộ nhớ state. Ghi hai nơi:

- `ops/latest.json` (ghi đè một object nhỏ, phục vụ panel "live").

- Bộ đệm lịch sử flush vào `ops/stream_metrics` (Delta) **mỗi \~60 giây** (không ghi mỗi batch, kẻo tạo thêm file nhỏ).
  **AC:** dashboard (Task 5.6) đọc được `latest.json` · bảng lịch sử có bản ghi theo từng query · log không làm đầy đĩa.
  **Learn:** F14, T02.

## 🦆 EPIC 4 — Tầng Gold và Kiểm định chất lượng (dbt + DuckDB)

**Mục tiêu:** tách logic kinh doanh khỏi luồng streaming; tính chỉ số bằng SQL có test; kiểm định dữ liệu tự động.
**Rủi ro chính:** công thức lệch giữa dbt và dashboard; test sai ngữ nghĩa (ví dụ so sánh click với apply ở grain không hợp lệ); xử lý nguồn ngân sách và các bảng dimension từ 17 file CSV.

### Task 4.1 — Khởi tạo dbt-duckdb và kết nối Delta trên MinIO \[M\] \[Depends: 3.1\]

**Steps:**

1. Tạo `dbt_cpc/` với `dbt-core` + `dbt-duckdb` (pin phiên bản **tương thích với DuckDB đã pin**, ghi vào `docs/versions.md`).

2. `profiles.yml`: `type: duckdb`, `path: ':memory:'`, nạp extension `httpfs`, `delta`, cấu hình secret S3 từ biến môi trường. **Lưu ý:** trường `endpoint` trong secret của DuckDB phải dùng dạng `host:port` không kèm `http://` (ví dụ `minio:9000` hoặc `localhost:9000`, `use_ssl: false`, `url_style: 'path'`).

3. Khai báo **source** trỏ tới Silver bằng `delta_scan('s3://lakehouse/silver/cpc_events')` (dùng cơ chế `external_location` của source nếu phiên bản hỗ trợ; nếu không, dùng hook `on-run-start` tạo view). Ghi cách đã chọn vào ADR nhỏ.
   **AC:** `dbt debug` ✔ · `dbt run -s stg_cpc_events` trả về dòng dữ liệu từ Silver thật trên MinIO.
   **Learn:** T05, T06.

### Task 4.2 — Tích hợp Dimension từ các file CSV đã chọn và Ngân sách Campaign (ADR-010) \[M\] \[Depends: 2.1\]

**Goal:** tận dụng các bảng danh mục có giá trị từ 17 file CSV (đã được chốt ở Task 2.1, ví dụ `campaign.csv`, `job.csv`, `master_publisher.csv`, `company.csv`, `group.csv`) và thiết lập ngân sách ngày (`daily_budget`) cho từng campaign.
**Steps:**

1. Dựa vào kết quả Task 2.1 (U9) trên file `campaign.csv`:
   - **Trường hợp A đã được xác nhận:** `campaign.csv` có sẵn ngân sách ngày thực tế hợp lệ (`budget_total` với `pacing_unit = 'Daily'`). Do đó, áp dụng trực tiếp Trường hợp A: đưa `campaign.csv` vào `dbt_cpc/seeds/` chuẩn hoá thành `dim_campaign_budget` (dùng dữ liệu thật theo R1, không cần sinh ngẫu nhiên `generate_budget_seed.py` nữa). Lưu ý: Campaign 1 có ngân sách 1 tỷ (dữ liệu test ngoại lai, cần lưu ý khi kiểm thử).

2. Nạp các bảng dimension nhỏ đã được duyệt ở Task 2.1 (ví dụ `master_publisher.csv`, `company.csv`, `group.csv`, `job.csv`) vào seeds hoặc external parquet để Dashboard hiển thị được **tên thật của Publisher, Campaign, Job, Company** thay vì chỉ hiện ID.

3. ADR-010: ghi rõ nguồn ngân sách (thật từ `campaign.csv` hay mô phỏng) và định nghĩa đường lý tưởng tuyến tính.
   **AC:** `dbt seed` / `dbt run` tạo đủ các bảng dimension đã chọn · mọi `campaign_id` xuất hiện trong dữ liệu sự kiện đều có `daily_budget` (test `relationships`) · chạy lại script cho cùng kết quả.

### Task 4.3 — Model staging \[S\] \[Depends: 4.1\]

**Steps:** `stg_cpc_events` (view): chọn đúng cột, ép kiểu, thêm `event_hour`; **không** chứa logic kinh doanh.
**AC:** `dbt build -s stg_cpc_events` pass kèm test `not_null`, `unique(event_id)`.

### Task 4.4 — Các model Gold \[L\] \[Depends: 4.2, 4.3\]

**Goal:** hiện thực hoá đúng Phần 2 thành 3 bảng gold, xuất ra Parquet trên MinIO (`materialized='external'`, `format='parquet'`, `location='s3://lakehouse/gold/<model>.parquet'`).
**Steps:**

1. `fct_campaign_hourly_pacing`: dùng **hour spine** (sinh đủ các giờ từ `0` đến giờ của `sim_now` trong ngày, để giờ không có event vẫn có dòng), `cum_spend` bằng window function, kết hợp `campaign_budget`. **Xử lý đúng giờ hiện tại chưa kết thúc** để khớp với Đường nhanh:

```
with bounds as (
  select max(event_time) as sim_now from {{ ref('stg_cpc_events') }}
), hourly as (
  select campaign_id, cast(event_time as date) as event_date,
         date_trunc('hour', event_time) as hour_ts, sum(spend) as spend_hour
  from {{ ref('stg_cpc_events') }}
  group by 1, 2, 3
), cum as (
  select h.*, b_now.sim_now,
         sum(spend_hour) over (partition by campaign_id, event_date order by hour_ts) as cum_spend,
         case
           when h.hour_ts = date_trunc('hour', b_now.sim_now)
             then extract(epoch from (b_now.sim_now - date_trunc('day', b_now.sim_now))) / 86400.0
           else (extract(hour from h.hour_ts) + 1) / 24.0
         end as elapsed_day_fraction
  from hourly h cross join bounds b_now
)
select c.campaign_id, c.event_date, c.hour_ts, c.cum_spend, b.daily_budget,
       b.daily_budget * c.elapsed_day_fraction as ideal_cum_spend,
       c.cum_spend / nullif(b.daily_budget * c.elapsed_day_fraction, 0) as pacing_ratio,
       c.cum_spend / nullif(b.daily_budget, 0) as budget_utilization
from cum c join {{ ref('campaign_budget') }} b using (campaign_id)

```

2. `fct_publisher_quality`: theo publisher × giờ; `click_to_apply_rate`; `fraud_flag_R1`, `fraud_flag_R2` theo Phần 2 (median toàn publisher trong cùng giờ bằng window `median(...) over (partition by hour_ts)`).

3. `fct_recruitment_efficiency`: theo job × campaign × ngày; `cpc`, `cpa`, `cpqa`, `qualified_rate` (dùng `NULLIF`).
   **AC:** `dbt build` tạo 3 file Parquet trong `gold/` · số liệu khớp cả 2 trường hợp (giờ đã đóng và giờ đang chạy dở) ở mục 2.3 khi chạy trên fixture · không có `SELECT *` trong gold.
   **Pitfalls:** giờ thiếu event làm đường tích luỹ đứt; dùng `(hour + 1)/24` cho giờ đang chạy dở làm lệch parity với dashboard; chia cho 0; join làm nhân dòng khi `campaign_budget` trùng khoá.
   **Learn:** F12, F11, T05, T06.

### Task 4.5 — Data Quality Tests \[M\] \[Depends: 4.4\]

**Steps:**

1. Generic: `not_null`, `unique` cho khoá, `accepted_values` cho `event_type`, `relationships` (campaign ↔ budget).

2. Singular/custom: `spend >= 0`; `pacing_ratio` không âm; không có `hour_ts` ngoài khoảng mô phỏng.

3. **Lưu ý ngữ nghĩa:** điều kiện `clicks >= applies >= qualified` **không đúng ở grain theo giờ/ngày** vì hồ sơ của click hôm qua có thể nộp hôm nay. Chỉ kiểm tra ở grain **tích luỹ toàn thời gian theo campaign** (severity `warn`), và ghi giải thích trong test.

4. Source freshness: `processed_at` quá 2 phút ➔ `warn`.

5. Test đối soát: số `event_id` khác nhau của `stg_cpc_events` = số `event_id` khác nhau của Silver.
   **AC:** `dbt test` pass trên lần chạy sạch · cố ý tiêm 1 bản ghi `spend` âm vào fixture ➔ test tương ứng FAIL.
   **Learn:** F13.

### Task 4.6 — Lịch chạy dbt không cần Airflow \[S\] \[Depends: 4.4\]

**Steps:** `scripts/dbt_loop.sh` (`make dbt-loop`) chạy `dbt build` mỗi 60 giây, có khoá `flock` chống chạy chồng, log thời lượng, ghi `gold/_last_run.json` (để dashboard hiển thị "cập nhật lúc"). Ghi rõ đây là lựa chọn thay thế nhẹ; Airflow là tuỳ chọn ở Task 7.1.
**AC:** hai lần chạy chồng không xảy ra · file `_last_run.json` cập nhật đều.

### Task 4.7 — Test "metric parity" giữa đường nhanh và đường sâu \[M\] \[Depends: 4.4, 5.2\]

**Goal:** chặn lệch công thức giữa SQL dashboard và dbt.
**Steps:** dựng fixture Delta cục bộ (tạo bằng thư viện `deltalake`, không cần Spark); kiểm tra cả một giờ đã đóng và giờ hiện tại tại `sim_now`; so sánh `spend`, `cum_spend`, `ideal_cum_spend`, `pacing_ratio` từ truy vấn dashboard và từ model gold.
**AC:** `pytest tests/integration/test_metric_parity.py` pass, sai lệch ≤ 1e-6 · chạy được trong CI không cần Docker.

## 📊 EPIC 5 — Serving Dashboard và Cảnh báo

**Mục tiêu:** biến dữ liệu thành sản phẩm cho Linh, Minh, Hùng, Hạnh và chính bạn (mục 1.3).
**Rủi ro chính:** dùng chung một connection DuckDB giữa nhiều thread; gửi alert từ trong dashboard (lặp vô hạn, chỉ chạy khi có người xem); truy vấn quét toàn bộ S3 mỗi lần làm tươi.

### Task 5.1 — Lớp kết nối DuckDB (ADR-004) \[M\] \[Depends: 3.1\]

**Steps:**

1. `src/dashboard/db.py`: `get_root_connection()` được cache một lần (`st.cache_resource`) với `duckdb.connect(":memory:")`; đặt `memory_limit` (\~256MB) và `threads` nhỏ; `extension_directory` trỏ thư mục bền (bind mount) và **cài sẵn extension lúc build image** để không cần mạng khi chạy.

2. Tạo secret S3 từ biến môi trường bằng **`cfg.s3_endpoint_host_port`** (không chứa `http://`), không bao giờ log câu lệnh chứa credential.

3. **Mỗi truy vấn dùng `root.cursor()` riêng rồi đóng** (không dùng chung một connection giữa các thread/session):

```
def run_query(sql: str, params: list | None = None) -> pd.DataFrame:
    cur = get_root_connection().cursor()
    t0 = time.perf_counter()
    try:
        return cur.execute(sql, params or []).df()
    finally:
        cur.close()
        log.info("query_ms=%.0f", (time.perf_counter() - t0) * 1000)

```

4. Retry có backoff cho lỗi mạng S3 tạm thời; trả lỗi có cấu trúc để UI hiển thị "mất kết nối" thay vì sập.

5. ADR-004: vì sao serving trực tiếp từ lakehouse, giới hạn quy mô, ngưỡng để chuyển sang OLAP server (xem F15).
   **AC:** test đơn vị trên Delta cục bộ · 5 thread chạy truy vấn đồng thời không lỗi · không có credential trong log (grep).
   **Learn:** F11, F15, T05, T07.

### Task 5.2 — Lớp truy vấn (query layer) có test \[M\] \[Depends: 5.1, 3.4\]

**Steps:** `src/dashboard/queries/{sim_now,pacing,publisher_quality,funnel,health}.sql` tham số hoá; `repository.py` trả DataFrame. Mọi truy vấn lọc `event_date` trước để cắt tỉa partition; dùng `EXPLAIN ANALYZE` kiểm tra thực tế. Test bằng fixture Delta tạo bởi `deltalake`, kỳ vọng khớp ví dụ số ở mục 2.3. `sim_now` = `MAX(event_time)` (mục 2.4).
**AC:** mỗi file `.sql` có ít nhất 1 test với con số kỳ vọng tính tay · thời gian truy vấn ghi lại.

### Task 5.3 — Panel 1: Budget Pacing Monitor \[M\] \[Depends: 5.2\]

**Hiển thị:** KPI (tổng chi hôm nay theo `sim_now`, số campaign vượt 90% ngân sách); chọn campaign ➔ đường chi tiêu thực tế so với đường lý tưởng theo giờ; bảng xếp hạng theo `pacing_ratio` tô màu (đỏ > 1.2, vàng < 0.6); `projected_eod_spend`; nhãn nguồn ngân sách (từ `campaign.csv` hoặc `"SIMULATED BUDGET"` tùy kết quả Task 2.1/4.2).
**AC:** với fixture của mục 2.3, campaign `C1` hiển thị `pacing_ratio = 1.6` và màu cảnh báo.

### Task 5.4 — Panel 2: Publisher Fraud & Quality Radar \[M\] \[Depends: 5.2\]

**Hiển thị:** bảng publisher (kèm tên publisher từ `master_publisher.csv` nếu có) với `clicks_60m`, `applies_60m`, `click_to_apply_rate`, cờ R1/R2 (tô đỏ), sắp xếp theo mức nghi ngờ; ngưỡng đọc từ `config/alerts.yaml`; ghi chú "heuristic, không phải ML".
**AC:** publisher `P9` ở fixture (`130` click, `0` apply) bị cờ R1; `99` click không bị cờ.

### Task 5.5 — Panel 3: Hiring Funnel và CPA \[M\] \[Depends: 4.4\]

**Hiển thị:** phễu (Impression nếu có ➔ Click ➔ Apply ➔ Qualified) kèm tỷ lệ chuyển giữa các bước; bảng job/campaign (kèm tên job/công ty từ `job.csv`/`company.csv`) tốt nhất và tệ nhất theo `CPQA`; đọc từ Gold Parquet, hiển thị "gold cập nhật lúc …".
**AC:** số liệu khớp `fct_recruitment_efficiency` · hiển thị đúng khi mẫu số = 0 ("n/a", không phải 0).

### Task 5.6 — Panel 4: Pipeline Health và DLQ Inspector \[M\] \[Depends: 3.9, 5.2\]

**Hiển thị:** `data_freshness_s`; `e2e_latency_s` p50/p95; `dedup_rate`, `late_rate`, `dlq_rate`; bảng DLQ theo `error_type`; mẫu payload lỗi (cắt ngắn, che dữ liệu nhạy cảm); thông số Spark từ `ops/latest.json`; số file Delta của Bronze/Silver.
**AC:** `docker kill` Spark ➔ `data_freshness_s` chuyển trạng thái đỏ trong ≤ 2 phút · số liệu DLQ khớp manifest.

### Task 5.7 — Tự làm tươi và hiệu năng \[S\] \[Depends: 5.3–5.6\]

**Steps:** dùng `@st.fragment(run_every="10s")` cho từng panel; `st.cache_data(ttl=5)` cho truy vấn dùng chung để N người xem không nhân N lần truy vấn S3; hiển thị "cập nhật lúc" và "truy vấn mất X ms"; giới hạn số dòng trả về.
**AC:** 5 tab trình duyệt mở 10 phút: RAM ổn định dưới `mem_limit`, p95 truy vấn ≤ 1.5s, không có lỗi khoá file (Lab L6).
**Pitfalls:** làm tươi cả trang thay vì từng fragment; cache theo tham số sai làm hiển thị số cũ.

### Task 5.8 — Alert Worker (process riêng) \[L\] \[Depends: 5.2\]

**Goal:** cảnh báo **đúng lúc, đúng một lần, có kết thúc**, độc lập với việc có ai mở dashboard.
**Steps:**

1. `src/alerting/worker.py`: vòng lặp mỗi N giây (cấu hình), lấy số liệu qua cùng `repository.py` (không viết lại SQL).

2. `config/alerts.yaml`: mỗi luật có `id`, `query`, `condition`, `threshold`, `clear_threshold` (hysteresis), `severity`, `cooldown_min`, `min_consecutive` (số lần liên tiếp mới báo), `max_renotify`.

3. `src/alerting/state.py`: lưu trạng thái trong SQLite (`DATA_ROOT/alerts/state.db`), khoá = `rule_id + entity_id`; máy trạng thái `OK ➔ FIRING ➔ (nhắc lại sau cooldown) ➔ RESOLVED ➔ OK`.

4. `src/alerting/sinks.py`: console (mặc định, `ALERT_DRY_RUN=true`), Telegram, Discord; retry + backoff; giới hạn tối đa số tin/phút; thông điệp có luật, đối tượng, giá trị, ngưỡng, thời gian mô phỏng, liên kết dashboard; **gửi cả tin "đã hết cảnh báo"**.

5. Luật khởi đầu: `budget_utilization > 0.90`; `pacing_ratio > 1.5` liên tiếp 2 lần; `fraud_flag_R1`; `data_freshness_s > 120`; `dlq_rate > 5%`.

6. Cooldown dùng đồng hồ thật; cửa sổ tính toán dùng đồng hồ mô phỏng (mục 2.4).
   **AC:** test với **đồng hồ giả và sink giả**: điều kiện vi phạm liên tục qua 100 lượt đánh giá ➔ **đúng 1** tin FIRING; sau cooldown vẫn vi phạm ➔ 1 tin nhắc lại; hết vi phạm (qua ngưỡng hysteresis) ➔ 1 tin RESOLVED; dao động quanh ngưỡng không gây spam · chạy thật ở chế độ dry-run in ra console.
   **Pitfalls:** gửi alert trong `st.fragment` (lặp mỗi 10s, và chỉ khi có người xem); không lưu trạng thái nên restart là báo lại; token nằm trong log.
   **Learn:** F14, F15.

### Task 5.9 — Đóng gói dashboard và alert worker \[S\] \[Depends: 5.1–5.8\]

**Steps:** Dockerfile nhỏ cho `dashboard` và `alert-worker` (profile `apps`), healthcheck, `mem_limit` theo 4.4, extension DuckDB cài sẵn.
**AC:** `make ui` mở `http://localhost:8501` · RAM mỗi service trong ngân sách · khởi động lại container không cần mạng để tải extension.

## 🏆 EPIC 6 — Hoàn thiện chuẩn đi phỏng vấn (Engineering Polish)

**Mục tiêu:** đóng gói để người lạ chạy được trong 5 lệnh, tin được số liệu, và bảo vệ được từng quyết định.

### Task 6.1 — Makefile chuẩn hoá thao tác \[S\] \[Depends: các epic trước\]

Các target tối thiểu (mỗi target có dòng mô tả cho `make help`):

| Target                                   | Việc làm                                                                |
| ---------------------------------------- | ----------------------------------------------------------------------- |
| `up` / `down`                            | Dựng/tắt nhóm `core` (và `apps` khi có cờ)                              |
| `topics`                                 | Tạo topic (idempotent)                                                  |
| `stream`                                 | Chạy Spark streaming (Q1, Q2, maintenance, listener)                    |
| `replay`                                 | Chạy producer (`SPEED_FACTOR`, `SEED`, `NO_FAULTS` đọc từ biến)         |
| `reconcile`                              | Chạy bộ đối soát với manifest mới nhất                                  |
| `compact`                                | Chạy compaction Delta theo yêu cầu                                      |
| `dbt-build` / `dbt-test` / `dbt-loop`    | Chạy dbt                                                                |
| `ui` / `alerts`                          | Chạy dashboard / alert worker                                           |
| `profile` / `s3-check` / `smoke-interop` | Các kiểm tra ở Epic 1–3                                                 |
| `test` / `lint`                          | `pytest` / `ruff`                                                       |
| `bench`                                  | Chạy kịch bản benchmark (6.4)                                           |
| `disk-report` / `clean` / `nuke-data`    | Dọn dẹp và báo cáo đĩa                                                  |
| `e2e`                                    | Từ trạng thái sạch: `up` ➔ `topics` ➔ `stream` ➔ `replay` ➔ `reconcile` |

**AC:** từ checkout sạch, `cp .env.example .env && make e2e` kết thúc bằng `reconcile: PASS` (có bơm lỗi).

### Task 6.2 — Unit test (pytest) \[M\]

Tối thiểu các nhóm test (đều chạy không cần Docker):

| Nhóm                              | Ví dụ case bắt buộc                                                                                    |
| --------------------------------- | ------------------------------------------------------------------------------------------------------ |
| `time_mapper`                     | `speed_factor=60` ➔ 1 phút = 1 giờ; biên đầu/cuối dữ liệu                                              |
| `fault_injector`                  | cùng seed ➔ cùng kết quả; tỷ lệ trong dung sai; mỗi loại lỗi sinh đúng `error_type`                    |
| `validate_event` và `validate_df` | parity trên cùng fixture; mỗi `error_type` (kể cả `malformed_decimal`) có ca hợp lệ biên và ca vi phạm |
| `event_id`                        | cùng dòng ➔ cùng id; khác dòng ➔ khác id                                                               |
| Metric (SQL)                      | cả 2 trường hợp (giờ đã đóng & giờ đang chạy dở) ở mục 2.3; mẫu số bằng 0 ➔ `NULL`                     |
| `alert rules/state`               | không spam, nhắc lại sau cooldown, resolved có hysteresis (đồng hồ giả)                                |
| Contract                          | JSON Schema chấp nhận/loại đúng                                                                        |

**AC:** `make test` < 3 phút, không cần Docker/Spark thật (Spark-based test gắn marker `spark`, chạy riêng).

### Task 6.3 — CI (GitHub Actions) \[S\]

**Steps:** `.github/workflows/ci.yml`: cài Python, cache pip, `ruff check`, `pytest` (bỏ marker `spark`), `dbt parse` (hoặc `dbt compile` trên fixture), có thể thêm kiểm tra link markdown. Marker `spark` chạy tay hoặc theo lịch.
**AC:** workflow xanh trên nhánh chính; badge trong README.

### Task 6.4 — Benchmark có phương pháp \[L\] \[Depends: 3.8, 5.7\]

**Goal:** mọi con số trong README đều đo được và lặp lại được.
**Phương pháp:** ghi cấu hình máy và phiên bản; khởi động ấm 2 phút rồi đo ≥ 10 phút; lặp 3 lần, báo trung vị; mỗi kịch bản một file CSV thô trong `docs/benchmarks/raw/`.

| Kịch bản | Mô tả                                | Số đo chính                                                 |
| -------- | ------------------------------------ | ----------------------------------------------------------- |
| B1       | `speed_factor=60`, có bơm lỗi        | e2e latency p50/p95, freshness                              |
| B2       | `speed_factor=600`                   | msg/s producer, `processedRowsPerSecond`, RAM mỗi container |
| B3       | Tăng dần tốc độ đến khi lag tăng đều | Thông lượng bền vững tối đa; nút thắt đầu tiên là gì        |
| B4       | Trước/sau compaction                 | Số file, thời gian truy vấn mỗi panel p50/p95               |
| B5       | Hồi phục sau `kill -9`               | Thời gian về trạng thái bình thường, kết quả reconcile      |
| B6       | Đĩa                                  | Tăng trưởng GB/giờ trên ổ D, mức thay đổi ổ C               |

**AC:** `docs/benchmarks/results.md` đầy đủ; bảng SLO ở mục 1.4 được cập nhật bằng số thật (R14).

### Task 6.5 — README chuẩn portfolio \[M\]

**Nội dung bắt buộc:** (1) một đoạn nêu bài toán và người dùng; (2) sơ đồ kiến trúc; (3) "Chạy trong 5 lệnh"; (4) GIF dashboard nhảy số + ảnh tin nhắn cảnh báo; (5) bảng benchmark thật; (6) bảng ADR kèm liên kết; (7) mục **"Limitations & what I'd do at 100×"** thẳng thắn; (8) ghi rõ phần dữ liệu thật từ 17 file CSV và phần mô phỏng (thời gian tua nhanh, lỗi bơm); (9) liên kết `docs/lessons/`.
**AC:** người ngoài làm theo "5 lệnh" được trên máy sạch (nhờ một người khác kiểm tra nếu có thể).

### Task 6.6 — Gói ôn phỏng vấn \[M\]

Viết `docs/lessons/interview_prep.md`: mỗi câu hỏi kèm **câu trả lời ngắn + bằng chứng trong repo** (ADR, benchmark, file:dòng).

1.  Vì sao Redpanda thay Kafka? (tiêu chí, đo RAM, giới hạn license)

2.  Vì sao Delta Lake trên MinIO (S3-compatible), không phải Postgres/MySQL?

3.  Vì sao không tính Gold trong Spark Streaming mà tách sang dbt + DuckDB?

4.  Dữ liệu tăng 100 lần thì nghẽn ở đâu trước và nâng cấp thế nào?

5.  Vì sao serving trực tiếp bằng DuckDB thay vì ClickHouse? Khi nào đổi? (dẫn số đo Task 7.3 nếu làm)

6.  Hệ thống đảm bảo "exactly-once" ở đoạn nào, chính xác là bảo đảm gì và dựa trên cơ chế nào?

7.  Event đến trễ thì sao? (watermark, ADR-006, số đo)

8.  Làm sao biết số liệu đúng? (reconciliation harness, manifest)

9.  Vì sao chọn khoá partition đó? Chuyện gì nếu lệch?

10. Small file problem là gì, bạn đo và xử lý ra sao để không đụng độ với luồng MERGE đang chạy?

11. Vì sao dedup bằng cả watermark lẫn MERGE?

12. Xử lý schema thay đổi (v1 ➔ v2) thế nào?

13. Vì sao tách cấu hình S3 qua biến môi trường dù đã chọn MinIO?

14. Cảnh báo tránh spam bằng cách nào?

15. Nếu chạy production, bạn giám sát những gì và đặt cảnh báo ở đâu?

16. Backfill/replay lại dữ liệu cũ thế nào?

17. Điều gì trong dự án này **không** giống production và vì sao chấp nhận?

## 🧪 EPIC 7 — Mở rộng tuỳ chọn (làm sau khi Epic 1–6 xong)

### Task 7.1 — Airflow dạng profile tuỳ chọn \[M\]

DAG: `dbt_build`, `quality_checks`, nhắc bảo trì. **Cảnh báo RAM** (Airflow + metadata DB có thể thêm 1.5–2GB): chỉ bật khi demo, không tính vào ngân sách 4.4. Ghi trong ADR vì sao v2 mặc định không dùng Airflow (job streaming không do Airflow restart; việc lập lịch nhẹ dùng `dbt-loop`).

### Task 7.2 — `reconcile_silver` (phương án P2 cho dữ liệu trễ) \[M\]

Job batch so Bronze với Silver, chèn các `event_id` bị loại do trễ vượt watermark bằng MERGE insert-only. **AC:** sau khi chạy, `make reconcile` báo "late dropped = 0".

### Task 7.3 — A/B: DuckDB trực tiếp so với ClickHouse \[L\]

Thêm profile `ch` (ClickHouse + Materialized View theo phút); nạp cùng dữ liệu; đo ở 1× và 10× khối lượng: latency truy vấn, RAM, độ phức tạp vận hành, độ trễ end-to-end. **AC:** phụ lục cho ADR-004 với số đo và khuyến nghị theo quy mô.

### Task 7.4 — Thí nghiệm tiến hoá schema \[M\]

Producer phát `v2` (thêm một trường) lẫn với `v1`; Bronze/Silver dùng schema evolution của Delta; DuckDB vẫn đọc đúng. **AC:** reconcile PASS, ghi lại các lỗi gặp và cách xử lý.

# 6. DEFINITION OF DONE VÀ BỘ THÍ NGHIỆM (LABS)

## 6.1 Definition of Done

**Cho một task:** (1) AC đã chạy, output dán vào `docs/progress/<task>.md`; (2) `make lint` và `make test` xanh; (3) `docs/code_map.md` cập nhật; (4) lesson liên quan đã bổ sung phần "Trong repo này" bằng đoạn code thật; (5) commit theo quy ước.

**Cho một epic:** (1) mọi task `[x]`; (2) `make disk-report` và `docker stats` nằm trong ngân sách; (3) ADR của epic đã viết; (4) lab của epic đã chạy và có ghi chép **dự đoán trước, quan sát, giải thích**.

**Cho cả dự án:** (1) `make e2e` từ máy sạch kết thúc bằng `reconcile: PASS` có bơm lỗi; (2) SLO ở 1.4 được cập nhật bằng số đo thật; (3) README đủ 9 mục ở Task 6.5; (4) có ghi chép ≥ 3 lần "phá hệ thống" (Labs) và cách hệ thống phản ứng; (5) gói ôn phỏng vấn hoàn chỉnh.

## 6.2 Labs: học bằng cách phá hệ thống

**Quy trình mỗi lab:** viết **dự đoán** vào `docs/lessons/labs.md` _trước khi chạy_ ➔ chạy ➔ ghi **quan sát** ➔ viết **giải thích bằng nguyên lý** (trỏ về F/T tương ứng) ➔ nếu dự đoán sai, nói rõ hiểu lầm nằm ở đâu.

| Lab    | Tình huống                                                                                                                | Điều cần quan sát                                                                      | Nguyên lý liên quan |
| ------ | ------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------- | ------------------- |
| **L0** | Dừng/khởi động lại container; tạo log lớn; chờ retention                                                                  | Dữ liệu còn ở ổ D; log xoay vòng; segment đóng rồi mới bị xoá                          | T01, T08, F07       |
| **L1** | `docker kill` Spark giữa batch rồi bật lại                                                                                | Không mất, không trùng; thời gian hồi phục                                             | F06, F16, T02       |
| **L2** | Dừng producer rồi chạy lại lệch vài dòng                                                                                  | Có message trùng; hệ thống phía sau xử lý ra sao                                       | F06                 |
| **L3** | Watermark 5/10/20 phút với dữ liệu trễ 5–15 phút                                                                          | Số event bị loại, kích thước state                                                     | F04, F05            |
| **L4** | Chạy không compaction nhiều giờ rồi `OPTIMIZE`                                                                            | Số file, thời gian truy vấn trước/sau                                                  | F10, T03            |
| **L5** | Đổi khoá partition `campaign_id` ↔ `job_id`                                                                               | Phân phối message, consumer lag                                                        | F07, T01            |
| **L6** | Mở 5 tab dashboard khi Spark đang ghi. Sau đó thử **cố ý** dùng file `.duckdb` bền để ghi từ Spark-side tool và đọc từ UI | Không lỗi khi đọc Delta; lỗi khoá khi dùng file `.duckdb` ➔ hiểu vì sao chọn zero-copy | F11, F15, T05       |
| **L7** | Tắt cooldown của alert                                                                                                    | Bão tin nhắn (dry-run) ➔ hiểu alert fatigue                                            | F14                 |
| **L8** | Xoá Silver + checkpoint Q2 (dev) rồi dựng lại từ Bronze                                                                   | Kết quả giống hệt lần đầu ➔ tính tất định/idempotent                                   | F06, F12            |
| **L9** | Gửi message khổng lồ hoặc `schema_version` lạ                                                                             | Đi vào DLQ với `error_type` đúng, pipeline không dừng                                  | F13                 |

# 7. GIÁO TRÌNH PHẦN I — KIẾN THỨC NỀN TẢNG (không gắn với công cụ nào)

> **Cách dùng:** đọc theo thứ tự F01 ➔ F16 trước hoặc song song với Epic tương ứng. Với mỗi module, Agent phải tạo `docs/lessons/Fxx_<tên>.md` theo mẫu ở Phụ lục A, **mở rộng** phần "Cơ chế" bằng sơ đồ và ví dụ, và hoàn thiện mục **"Trong repo này"** bằng đường dẫn `file:dòng` thật cùng đoạn code ngắn sau khi code đã tồn tại.
> **Quy ước mỗi module:** Câu hỏi gốc ➔ Trực giác ➔ Cơ chế ➔ Ví dụ số ➔ Hiểu lầm phổ biến ➔ Trong repo này ➔ Tự kiểm tra.
> Đường dẫn trong mục "Trong repo này" là **kế hoạch**; Agent cập nhật số dòng thật vào `docs/code_map.md`.

## F01 — Batch và Streaming

**Câu hỏi gốc:** khi dữ liệu đến liên tục và không bao giờ "hết", "xử lý xong" nghĩa là gì?
**Trực giác:** batch giống chụp ảnh cả hồ rồi đếm cá; streaming giống đứng ở cửa ra đếm từng con khi nó bơi qua. Dữ liệu **bounded** có đầu và cuối (một file CSV); **unbounded** thì không (luồng click).
**Cơ chế:**

- _Micro-batch_ (Spark Structured Streaming): chia luồng thành các lô nhỏ chạy theo chu kỳ (`trigger`). Mỗi lô là một job batch nhỏ trên dữ liệu mới. Dễ tái sử dụng API batch, chịu lỗi tốt, độ trễ cỡ giây.

- _Event-at-a-time_ (Flink, Kafka Streams): xử lý từng event, độ trễ mili-giây, vận hành phức tạp hơn.

- "Real-time" không phải một con số: phải định nghĩa bằng SLO (mục 1.4).
  **Ví dụ số:** trigger 10 giây. Event đến ở giây 0.1 phải đợi tới giây 10 mới vào lô tiếp theo ➔ chờ trung bình \~5s + thời gian xử lý \~2s ➔ độ trễ ở chặng này ≈ 7s, tối đa ≈ 12s.
  **Hiểu lầm:** streaming không đồng nghĩa với "nhanh hơn batch về thông lượng"; thường tốn tài nguyên hơn vì phải chạy liên tục.
  **Trong repo này:** `config/pipeline.yaml` (`trigger_seconds`); `src/streaming/ingest.py` (`.trigger(processingTime=...)`); `src/streaming/refine.py`.
  **Tự kiểm tra:** (1) Vì sao hạ trigger xuống 1 giây không làm hệ thống "real-time hơn" một cách miễn phí? (2) Dữ liệu nào trong project là bounded, dữ liệu nào là unbounded? (3) Nếu SLO là p95 ≤ 45s thì micro-batch 10s có phù hợp không, vì sao?

## F02 — Ba loại thời gian

**Câu hỏi gốc:** "bây giờ là mấy giờ" của một sự kiện được tính theo đồng hồ nào?
**Trực giác:** bạn nhấn nút lúc 10:00 trên điện thoại mất sóng; tin đến server lúc 10:07; server xử lý lúc 10:07:05. Cùng một sự kiện, ba thời điểm.
**Cơ chế:**

| Loại                | Ai gán               | Dùng để                                                      |
| ------------------- | -------------------- | ------------------------------------------------------------ |
| **Event time**      | nguồn phát sinh      | tính chỉ số đúng nghĩa kinh doanh (chi tiêu theo giờ xảy ra) |
| **Ingestion time**  | broker/hệ thống nhận | truy vết, đo độ trễ vào hệ thống                             |
| **Processing time** | engine xử lý         | vận hành, freshness, SLA                                     |

Tổng hợp theo _processing time_ cho kết quả thay đổi theo độ trễ mạng; theo _event time_ cho kết quả đúng nhưng phải xử lý dữ liệu đến trễ (xem F04).
**Ví dụ số:** click lúc 10:59:58 đến sau 5 giây. Theo event time nó thuộc giờ 10; theo processing time nó rơi vào giờ 11 ➔ `spend` của giờ 10 bị thiếu.
**Hiểu lầm:** dùng `current_timestamp()` để gán giờ cho event "cho tiện".
**Trong repo này:** `src/producer/time_mapper.py` (ánh xạ sang đồng hồ mô phỏng); cột `event_time`, `produced_at`, `processed_at` ở mục 2.1; `src/streaming/refine.py` (`processed_at`).
**Tự kiểm tra:** (1) Vì sao dashboard dùng `sim_now = MAX(event_time)` thay vì giờ máy? (2) Đại lượng nào cần processing time? (3) Điều gì sai nếu tính `spend` theo giờ bằng `processed_at`?

## F03 — Cửa sổ thời gian (Windows)

**Câu hỏi gốc:** muốn "tổng chi tiêu theo giờ" hay "click trong 60 phút gần nhất" trên dữ liệu vô hạn thì cắt dữ liệu thế nào?
**Cơ chế:**

- **Tumbling:** các cửa sổ liền kề không chồng (mỗi giờ một ô). Mỗi event thuộc đúng 1 ô.

- **Sliding (hopping):** cửa sổ trượt, chồng nhau (60 phút, trượt mỗi 5 phút). Mỗi event thuộc nhiều ô.

- **Session:** cửa sổ theo khoảng lặng (nhóm hành vi của một người cho tới khi im lặng > N phút).
  **Quyết định thiết kế của project:** **không** dùng aggregation có trạng thái trong Spark. Silver giữ event sạch; việc chia ô giờ (`date_trunc('hour', event_time)`) và cửa sổ 60 phút trượt được làm bằng SQL ở DuckDB/dbt. Lý do: giữ state Spark nhỏ, và đổi công thức không cần dừng luồng (xem câu hỏi phỏng vấn số 3).
  **Ví dụ số:** event 10:59 và 11:01 ➔ hai ô giờ khác nhau (tumbling) nhưng cùng một cửa sổ 60 phút kết thúc 11:30 (sliding).
  **Hiểu lầm:** "cửa sổ" luôn là việc của engine streaming; thực tế có thể tính ở tầng truy vấn.
  **Trong repo này:** `dbt_cpc/models/gold/fct_campaign_hourly_pacing.sql` (tumbling theo giờ); `src/dashboard/queries/publisher_quality.sql` (cửa sổ 60 phút kết thúc tại `sim_now`).
  **Tự kiểm tra:** (1) Khác biệt giữa tumbling và sliding với ví dụ click? (2) Vì sao cờ gian lận dùng cửa sổ trượt thay vì ô giờ cố định? (3) Cái giá của việc đẩy aggregation vào Spark là gì?

## F04 — Watermark và dữ liệu đến trễ

**Câu hỏi gốc:** event có thể đến muộn và lộn xộn; đợi bao lâu thì được phép "chốt" và dọn bộ nhớ?
**Trực giác:** watermark là lời tuyên bố "tôi tin rằng sẽ không còn event nào cũ hơn mốc này nữa". Đó là một **heuristic**, không phải sự thật.
**Cơ chế:** `watermark = (event_time lớn nhất đã thấy) − độ trễ cho phép`. Engine dùng nó để (a) biết cửa sổ/khoá nào có thể đóng và xoá state; (b) quyết định event nào là "quá trễ" (có thể bị loại ở toán tử có state). Watermark chỉ tiến lên khi có dữ liệu mới. Đánh đổi: watermark lớn ➔ ít mất event trễ nhưng state lớn và kết quả chốt chậm; watermark nhỏ ➔ state nhỏ nhưng mất nhiều event trễ.
**Ví dụ số:** độ trễ cho phép 10 phút. Đã thấy event_time lớn nhất là 10:30 ➔ watermark = 10:20. Event `event_time=10:25` đến ➔ chấp nhận. Event `event_time=10:15` đến ➔ **quá trễ** (có thể bị loại khỏi dedup có state). Event này vẫn còn ở Bronze vì Bronze không dùng watermark.
**Hiểu lầm:** watermark sắp xếp lại event (không); watermark "chờ" event (nó chỉ là mốc để dọn dẹp và loại).
**Trong repo này:** `config/pipeline.yaml` (`watermark_delay`); `src/streaming/refine.py` (`withWatermark`, `dropDuplicatesWithinWatermark`); manifest `late_ids.jsonl` (Task 3.5); ADR-006; Lab L3.
**Tự kiểm tra:** (1) Vì sao watermark không đảm bảo "không mất event"? (2) Với dữ liệu trễ 5–15 phút và watermark 10 phút, khoảng bao nhiêu phần trăm event trễ bị loại (giả sử trễ phân bố đều)? (3) Tại sao Bronze là "lưới an toàn"?

## F05 — Trạng thái (State) trong streaming

**Câu hỏi gốc:** muốn biết "event này đã thấy chưa" thì hệ thống phải nhớ gì và nhớ bao lâu?
**Cơ chế:** toán tử có trạng thái (dedup, aggregation, join) lưu **state** theo khoá. State nằm trong **state store** (bộ nhớ, tuỳ chọn RocksDB) và được sao lưu định kỳ vào **checkpoint** để khôi phục sau lỗi. State **chỉ co lại** khi có watermark cho phép xoá khoá cũ. Không có watermark ➔ state tăng vô hạn ➔ hết RAM.
**Ví dụ số:** 1.000 event/giây × 600 giây (watermark 10 phút) = 600.000 `event_id` đang nhớ; mỗi khoá cỡ \~100 byte ➔ khoảng 60MB. Nhân watermark lên 20 phút ➔ khoảng 120MB.
**Hiểu lầm:** checkpoint chỉ lưu offset (còn lưu cả state và metadata của query).
**Trong repo này:** `src/streaming/spark_session.py` (cấu hình state store); `src/streaming/listener.py` (ghi `numRowsTotal`, `memoryUsedBytes`); ADR-006.
**Tự kiểm tra:** (1) Vì sao dedup bắt buộc đi kèm watermark? (2) Điều gì xảy ra nếu xoá thư mục checkpoint? (3) Ước lượng state nếu tốc độ tăng 10 lần?

## F06 — Bảo đảm giao nhận và Idempotency

**Câu hỏi gốc:** nếu hệ thống sập giữa chừng, mỗi event được xử lý 0, 1 hay nhiều lần?
**Cơ chế:**

- **At-most-once:** có thể mất, không trùng. **At-least-once:** không mất, có thể trùng (thử lại). **Exactly-once:** hiệu ứng như đúng một lần.

- Exactly-once đầu–cuối thực chất = _at-least-once_ + _sink idempotent_ (hoặc giao dịch). Ghi lại cùng một kết quả nhiều lần cho cùng hiệu ứng.

- **Idempotent producer** (Kafka/Redpanda) ngăn trùng do _retry của chính producer_ trong một phiên; **không** ngăn việc ứng dụng gửi lại cùng dữ liệu sau khi restart.
  **Bảng bảo đảm trong project:**

| Chặng                    | Cơ chế                                 | Mức                         |
| ------------------------ | -------------------------------------- | --------------------------- |
| Producer ➔ Redpanda      | `enable.idempotence`, `acks=all`       | không trùng do retry nội bộ |
| Redpanda ➔ Spark         | offset lưu trong checkpoint của Spark  | không mất (trong retention) |
| Spark ➔ Bronze/DLQ Delta | `txnAppId` + `txnVersion=batch_id`     | idempotent theo batch       |
| Spark ➔ Silver           | MERGE chèn nếu chưa có theo `event_id` | idempotent theo khoá        |
| Spark ➔ topic DLQ        | ghi Kafka                              | at-least-once (chấp nhận)   |

**Ví dụ:** batch 42 đã ghi Bronze nhưng sập trước khi Spark ghi checkpoint. Khởi động lại, Spark chạy lại batch 42; Delta thấy `txnVersion=42` của app này đã commit ➔ bỏ qua, không nhân đôi.
**Hiểu lầm:** "dùng Kafka là có exactly-once". Bảo đảm chỉ đúng trong phạm vi và cấu hình cụ thể.
**Trong repo này:** `src/producer/replay_producer.py` (cấu hình producer); `src/streaming/ingest.py` (`txnAppId`/`txnVersion`); `src/streaming/refine.py` (`upsert_silver`); `tests/integration/reconcile.py`; Lab L1, L2, L8.
**Tự kiểm tra:** (1) Vì sao producer idempotent không giải quyết event trùng do chạy lại script? (2) Chỉ ra điểm _không_ idempotent trong pipeline và vì sao chấp nhận. (3) Nếu bỏ điều kiện `event_id` trong MERGE thì chuyện gì xảy ra khi Q2 đọc lại Bronze?

## F07 — Log nhắn tin: topic, partition, offset, consumer group

**Câu hỏi gốc:** làm sao nhiều bên đọc cùng luồng dữ liệu, theo thứ tự, mà vẫn song song và chịu lỗi?
**Cơ chế:**

- Khác _queue_ (đọc xong xoá), **log** giữ message theo retention; mỗi người đọc tự nhớ vị trí (**offset**) và có thể đọc lại.

- **Topic** chia thành **partition**; thứ tự **chỉ được đảm bảo trong một partition**. Message có **key** ➔ `hash(key) mod số_partition` ➔ cùng key luôn vào cùng partition.

- **Consumer group:** các consumer cùng nhóm chia nhau partition (tối đa một consumer/partition). **Rebalance** khi thành viên vào/ra.

- **Lag** = offset cuối của log − offset đã xử lý.

- Spark Structured Streaming **tự quản lý offset trong checkpoint**, không dựa vào commit của consumer group (nên đừng dùng lag của group để đoán tiến độ Spark).

- **Retention** xoá theo thời gian/dung lượng, và chỉ xoá segment đã đóng.
  **Ví dụ số (lệch khoá):** 3 partition; khoá `campaign_id` có campaign top-1 chiếm 60% event ➔ một partition nhận ≈ 60% tải, hai partition còn lại ≈ 20% mỗi cái ➔ partition nóng là điểm nghẽn.
  **Hiểu lầm:** thêm partition luôn tăng tốc (không, nếu khoá lệch); thứ tự toàn cục (không có).
  **Trong repo này:** `scripts/` tạo topic (Task 2.4); `src/producer/replay_producer.py` (`key=...`); ADR-005; Lab L5.
  **Tự kiểm tra:** (1) Vì sao cần đảm bảo cùng `campaign_id` vào cùng partition? (2) Tăng từ 3 lên 12 partition có giúp khi một khoá chiếm 60%? (3) Điều gì xảy ra với dữ liệu khi Spark dừng lâu hơn retention?

## F08 — Object storage và tách rời tính toán/lưu trữ

**Câu hỏi gốc:** vì sao data lake hiện đại đặt dữ liệu trên S3-compatible (MinIO) thay vì ổ đĩa gắn vào máy tính toán?
**Cơ chế:**

- **Object** = key + bytes + metadata, truy cập qua HTTP. Không gian tên **phẳng**: "thư mục" chỉ là tiền tố của key.

- Object **bất biến**: sửa = ghi lại cả object. **Không có rename nguyên tử** (rename = copy + delete). Liệt kê (`LIST`) tốn kém hơn đọc.

- **Disaggregated storage/compute:** nhiều engine (Spark, DuckDB, dbt) đọc chung một kho; mở rộng/tắt tính toán độc lập với lưu trữ; chi phí lưu trữ rẻ.

- Hệ quả: vì thiếu giao dịch và rename nguyên tử, cần **table format** (F10) để có ACID.
  **Ví dụ:** hai engine (Spark ghi, DuckDB đọc) làm việc trên cùng bảng trong MinIO mà không copy dữ liệu = "zero-copy".
  **Hiểu lầm:** mọi S3-compatible đều giống nhau (khác nhau ở độ phủ API, tính nhất quán, hiệu năng file nhỏ, license).
  **Trong repo này:** `.env.example` (`S3_*`); `src/streaming/spark_session.py` (`fs.s3a.*`); `src/dashboard/db.py` (secret S3); `docs/adr/001-object-store.md`; `scripts/s3_conformance.py`.
  **Tự kiểm tra:** (1) Vì sao "đổi tên thư mục" trên object store không nguyên tử? (2) Lợi ích cụ thể của tách tính toán/lưu trữ trong project này? (3) Đổi từ MinIO sang AWS S3 cần sửa những file nào (đáp án mong đợi: chỉ cấu hình `.env`)?

## F09 — Lưu trữ theo dòng/theo cột và Parquet

**Câu hỏi gốc:** vì sao truy vấn phân tích trên Parquet nhanh hơn CSV nhiều lần?
**Cơ chế:** lưu **theo cột** gom giá trị cùng cột cạnh nhau ➔ nén tốt (dictionary, run-length) và chỉ đọc cột cần (**column pruning**). Parquet chia file thành **row group** ➔ **column chunk** ➔ **page**, có **footer** chứa schema và thống kê min/max ➔ bỏ qua cả row group không khớp điều kiện (**predicate pushdown**). Mỗi file có overhead footer ➔ nhiều file nhỏ rất tệ (F10).
**Ví dụ số:** bảng 20 cột, truy vấn chỉ dùng 3 cột ➔ đọc ≈ 15% dữ liệu so với đọc theo dòng.
**Hiểu lầm:** Parquet luôn nhanh; thực tế file quá nhỏ hoặc quá nhiều làm hiệu năng sụp.
**Trong repo này:** file Parquet dưới `silver/cpc_events/event_date=.../`; `scripts/profile_tracking.py` (so CSV với Parquet); `docs/benchmarks/small_files.md`.
**Tự kiểm tra:** (1) Column pruning là gì, ví dụ trên truy vấn pacing? (2) Min/max thống kê giúp bỏ qua dữ liệu thế nào? (3) Vì sao 5.000 file 100KB chậm hơn 5 file 100MB?

## F10 — Table format và ACID trên object storage (Delta Lake)

**Câu hỏi gốc:** một thư mục đầy file Parquet thì làm sao có "giao dịch", "phiên bản", "ghi đồng thời an toàn"?
**Cơ chế (Delta):**

- Bảng = file dữ liệu Parquet + **`_delta_log/`**: dãy file JSON đánh số (`000...0042.json`) ghi các **action** (`add` file, `remove` file, `metaData`, `protocol`, `txn`, `commitInfo`). Cứ \~10 commit có **checkpoint** Parquet để đọc nhanh.

- **Snapshot** = phát lại log đến phiên bản N ➔ danh sách file hợp lệ. Người đọc luôn thấy một snapshot nhất quán (**snapshot isolation**), nên đọc khi đang ghi không lỗi.

- **Ghi:** ghi file dữ liệu mới trước, rồi cố tạo file log phiên bản N+1 (**optimistic concurrency**: nếu đã có người commit N+1 thì kiểm tra xung đột hoặc thử lại).

- **MERGE** = đọc file bị ảnh hưởng, viết lại (copy-on-write): `remove` file cũ + `add` file mới. **Time travel** = đọc snapshot cũ. **OPTIMIZE** gộp file nhỏ; **VACUUM** xoá file không còn được tham chiếu sau thời gian giữ.

- **Small file problem:** micro-batch 10s ➔ mỗi batch ≥ 1 file ➔ \~8.640 file/ngày/partition ➔ chậm LIST, chậm mở file.

- Giới hạn: nếu vừa MERGE vừa OPTIMIZE trên cùng एक partition sẽ gây xung đột optimistic concurrency (xem Task 3.8).
  **Ví dụ số:** sau 3 commit, `_delta_log` có `...0.json`, `...1.json`, `...2.json`; bảng ở phiên bản 2.
  **Hiểu lầm:** Delta là CSDL (không, là cách sắp xếp file + log); MERGE sửa file tại chỗ (không, viết lại).
  **Trong repo này:** `src/streaming/refine.py` (`DeltaTable.merge`); `src/streaming/maintenance.py` (`optimize`, `vacuum`); `tests/integration/smoke_delta_interop.py`; Lab L4 (liệt kê `_delta_log` sau vài commit).
  **Tự kiểm tra:** (1) Vì sao DuckDB đọc thấy dữ liệu nhất quán khi Spark đang ghi? (2) MERGE ảnh hưởng thế nào đến `_delta_log`? (3) Vì sao không được `vacuum(0)` khi đang có reader?

## F11 — OLTP và OLAP, engine vector hoá, embedded và server

**Câu hỏi gốc:** vì sao có CSDL cho giao dịch và CSDL cho phân tích, và DuckDB thuộc nhóm nào?
**Cơ chế:**

- **OLTP** (Postgres/MySQL): nhiều giao dịch nhỏ, đọc/ghi từng dòng, lưu theo dòng, ràng buộc toàn vẹn. **OLAP**: ít truy vấn nhưng quét lượng lớn, tổng hợp theo cột, lưu theo cột.

- **Vector hoá:** xử lý theo lô giá trị (cỡ vài nghìn) vừa bộ nhớ đệm CPU thay vì từng dòng ➔ tận dụng CPU. DuckDB còn song song hoá theo từng mảnh dữ liệu.

- **Embedded vs server:** DuckDB chạy **trong tiến trình** của ứng dụng; không có daemon. Với file `.duckdb`: một tiến trình đọc-ghi **hoặc** nhiều tiến trình chỉ-đọc (single-writer). Với chế độ `:memory:` đọc file ngoài (Parquet/Delta) thì không có file khoá nào để tranh chấp.

- Server (ClickHouse): chạy nền, nhận ghi và đọc đồng thời từ nhiều client, có cơ chế tổng hợp khi ghi (Materialized View).
  **Hiểu lầm:** "DuckDB nhanh gấp hàng chục lần Postgres" là đúng cho _một số_ truy vấn phân tích trên một máy, không phải quy luật; và DuckDB không thay thế server khi cần nhiều người dùng đồng thời quy mô lớn.
  **Trong repo này:** `src/dashboard/db.py` (`:memory:`, `cursor()` mỗi truy vấn); `docs/adr/004-serving.md`; Lab L6.
  **Tự kiểm tra:** (1) Vì sao dùng `:memory:` tránh được khoá file? (2) Hai điểm mà server như ClickHouse làm tốt hơn DuckDB nhúng? (3) Vì sao không dùng một connection chung giữa các thread?

## F12 — Mô hình hoá dữ liệu và kiến trúc Medallion

**Câu hỏi gốc:** tổ chức bảng thế nào để số liệu đúng, dễ truy vấn, dễ sửa?
**Cơ chế:**

- **Grain** (độ mịn): một dòng đại diện cho cái gì (event? campaign-giờ?). Xác định grain trước khi viết SQL.

- **Fact** (số đo) và **Dimension** (ngữ cảnh từ `campaign.csv`, `job.csv`, `master_publisher.csv`, `company.csv`). **Số đo cộng được** (`spend`, `clicks`) khác **tỷ lệ** (`CTR`, `CPA`): **tính tỷ lệ từ tổng**, không lấy trung bình của các tỷ lệ.

- **Medallion:** _Bronze_ = dữ liệu thô, bất biến, đủ để dựng lại mọi thứ; _Silver_ = đã làm sạch, đúng kiểu, khử trùng, chưa gắn nghiệp vụ; _Gold_ = bảng đã gắn nghiệp vụ, tối ưu cho đọc.
  **Ví dụ số:** giờ A: 10 click/2 apply (20%); giờ B: 1.000 click/50 apply (5%). Trung bình tỷ lệ = 12.5% (sai). Tính từ tổng: 52/1.010 ≈ 5.15% (đúng).
  **Hiểu lầm:** Silver chứa logic kinh doanh (không; công thức nằm ở Gold/dbt).
  **Trong repo này:** `dbt_cpc/models/`; mục 3.5 (grain của từng bảng); `docs/data_dictionary.md`.
  **Tự kiểm tra:** (1) Grain của `fct_publisher_quality`? (2) Vì sao không `AVG(click_to_apply_rate)`? (3) Vì sao Bronze giữ cả `payload_raw`?

## F13 — Chất lượng dữ liệu và Data Contract

**Câu hỏi gốc:** làm sao tin con số trên dashboard?
**Cơ chế:** các chiều chất lượng: đầy đủ, hợp lệ, duy nhất, đúng hạn (freshness), nhất quán, chính xác. **Data contract** = thoả thuận bằng văn bản và máy đọc được về schema, ý nghĩa, ràng buộc và SLA giữa bên sinh và bên dùng dữ liệu. **Shift-left:** bắt lỗi càng gần nguồn càng tốt (validate ở ingest ➔ DLQ), rồi kiểm lại ở từng tầng (dbt test) và **đối soát với sự thật** (manifest). **DLQ/quarantine:** cô lập bản ghi lỗi, không chặn luồng, vẫn truy vết và xử lý lại được.
**Ví dụ:** event `spend="-5.0000"` đi vào DLQ với `error_type=negative_amount` thay vì làm tổng chi tiêu giảm âm thầm.
**Hiểu lầm:** test cuối đường ống là đủ (khi đó lỗi đã lan ra nhiều bảng).
**Trong repo này:** `src/common/schemas/cpc_event.v1.json`; `src/streaming/validation.py`; `dbt_cpc/tests/`; `tests/integration/reconcile.py`; Panel 4.
**Tự kiểm tra:** (1) Vì sao validator Python và Spark phải có parity test? (2) DLQ khác "bỏ bản ghi lỗi" ở điểm nào? (3) Thế nào là "đối soát với sự thật" và vì sao mô phỏng cho phép làm điều đó?

## F14 — Observability, SLO và nguyên tắc cảnh báo

**Câu hỏi gốc:** làm sao biết pipeline đang khoẻ trước khi người dùng phàn nàn?
**Cơ chế:** **SLI** (chỉ số đo) ➔ **SLO** (mục tiêu) ➔ cảnh báo khi vi phạm. Tín hiệu vàng của pipeline: **freshness, volume, latency, error rate (DLQ), lag/saturation**. Nguyên tắc cảnh báo: dựa trên triệu chứng người dùng cảm nhận; **hành động được**; chống **alert fatigue** bằng cooldown, hysteresis (ngưỡng bật khác ngưỡng tắt), gộp trùng, mức độ nghiêm trọng, và tin "đã hết cảnh báo".
**Ví dụ số:** `pacing_ratio` dao động quanh 1.5 (1.49, 1.51, 1.49 ...). Không có hysteresis ➔ báo/tắt liên tục; với `bật >1.5`, `tắt <1.3` và `min_consecutive=2` ➔ chỉ 1 tin.
**Hiểu lầm:** càng nhiều cảnh báo càng an toàn (ngược lại: mọi người bỏ qua tất cả).
**Trong repo này:** `src/streaming/listener.py`; Panel 4 (`src/dashboard/panels/health.py`); `config/alerts.yaml`; `src/alerting/state.py`; Lab L7.
**Tự kiểm tra:** (1) Phân biệt SLI/SLO? (2) Hysteresis giải quyết vấn đề gì? (3) Vì sao alert không nên nằm trong dashboard?

## F15 — Mẫu phục vụ dữ liệu: Push và Pull

**Câu hỏi gốc:** dữ liệu nên được "đẩy" vào một máy chủ phục vụ trước, hay được "kéo" trực tiếp từ lake khi cần?
**Cơ chế:**

|                            | **Push** (OLAP server, ví dụ ClickHouse + Grafana) | **Pull** (DuckDB + Streamlit đọc lake)                         |
| -------------------------- | -------------------------------------------------- | -------------------------------------------------------------- |
| Độ trễ truy vấn            | rất thấp, ổn định                                  | phụ thuộc quét S3 và số file                                   |
| Nhiều người dùng đồng thời | tốt                                                | mỗi người dùng tự quét ➔ tải tăng tuyến tính (giảm bằng cache) |
| Độ phức tạp vận hành       | thêm một hệ thống phải chạy và nạp                 | gần như không thêm                                             |
| Tài nguyên                 | tốn RAM server                                     | rất nhẹ                                                        |
| Khi hợp                    | nhiều người dùng, dashboard thời gian thực dày đặc | vài người dùng, dữ liệu vừa, ngân sách tài nguyên nhỏ          |

**Ngưỡng chuyển** (ghi ADR-004 và kiểm chứng ở Task 7.3): số người dùng đồng thời, kích thước dữ liệu quét mỗi truy vấn, p95 vượt SLO.
**Hiểu lầm:** pull "hiện đại hơn" push; thực ra là hai đánh đổi.
**Trong repo này:** `docs/adr/004-serving.md`; `src/dashboard/db.py`, `src/dashboard/panels/*`; Task 5.7; Task 7.3.
**Tự kiểm tra:** (1) Cache `ttl=5` giúp ích thế nào khi 20 người mở dashboard? (2) Dấu hiệu đầu tiên cho thấy cần chuyển sang push? (3) Điều gì mất đi nếu chuyển?

## F16 — Mô hình lỗi và hồi phục

**Câu hỏi gốc:** thành phần nào chết thì chuyện gì xảy ra, và làm sao chứng minh hệ thống tự lành?
**Cơ chế:** với mỗi thành phần, hỏi bốn câu: _chết thì ảnh hưởng gì? dữ liệu nào có thể mất/trùng? tự hồi phục bằng cách nào? kiểm chứng bằng test nào?_ Các khái niệm: **checkpoint** (offset + state + metadata để chạy tiếp), **backpressure** (`maxOffsetsPerTrigger` giới hạn lượng đọc mỗi lô), **retry + backoff**, **poison pill** (message làm job chết ➔ DLQ), đĩa đầy, đồng hồ lệch.
**Bảng lỗi của project:**

| Sự cố                    | Hệ quả           | Hồi phục                               | Kiểm chứng          |
| ------------------------ | ---------------- | -------------------------------------- | ------------------- |
| Spark bị kill giữa batch | batch dang dở    | chạy lại từ checkpoint; ghi idempotent | Task 3.6 S1, Lab L1 |
| Spark dừng 5 phút        | Kafka dồn lag    | catch-up khi bật lại                   | S2                  |
| Redpanda restart         | producer tạm lỗi | retry của producer                     | S4                  |
| Xoá checkpoint Q2        | đọc lại Bronze   | MERGE chặn trùng                       | S3, Lab L8          |
| MinIO mất kết nối        | batch lỗi        | Spark retry; dashboard hiển thị lỗi    | thử tắt `minio`     |
| Message hỏng             | không parse được | DLQ, không dừng luồng                  | Lab L9              |

**Hiểu lầm:** có checkpoint nghĩa là không bao giờ trùng (chỉ khi sink idempotent).
**Trong repo này:** `tests/integration/test_recovery.py` (tự động hoá S1–S4 nếu khả thi); `src/streaming/ingest.py` (`maxOffsetsPerTrigger`); `src/alerting/sinks.py` (retry/backoff).
**Tự kiểm tra:** (1) Checkpoint của Q1 chứa những gì? (2) Vì sao S3 an toàn còn việc xoá checkpoint của Q1 thì không? (3) Hãy thêm một dòng vào bảng lỗi cho trường hợp "ổ D đầy".

# 8. GIÁO TRÌNH PHẦN II — GIẢI PHẪU CÔNG CỤ

> **Cách dùng:** mỗi công cụ có 7 mục: _Là gì_ ➔ _Thành phần_ ➔ _Cơ chế bên trong_ ➔ _Điểm mạnh_ ➔ _Điểm yếu và bẫy_ ➔ _Khi nào không dùng / thay thế_ ➔ _Trong repo này_ (bảng trỏ tới file/symbol + đoạn code mẫu).
> Agent tạo `docs/lessons/Txx_<tên>.md`, mở rộng bằng sơ đồ, và **thay đoạn code mẫu bằng đoạn code thật trích từ repo (kèm `file:dòng`)** khi đã hiện thực. Các snippet dưới đây chỉ minh hoạ pattern (R10).

## T01 — Redpanda (Kafka API)

**Là gì:** broker log phân tán tương thích Kafka API, viết bằng C++ (kiến trúc _thread-per-core_ trên Seastar), một binary duy nhất, không cần JVM và Zookeeper.
**Thành phần:** _broker_ · _topic ➔ partition ➔ segment_ · _Raft group_ cho mỗi partition · `rpk` (CLI) · _Admin API_ (cổng 9644) · _Console_ (UI) · Schema Registry tích hợp (v2 không dùng).
**Cơ chế:** mỗi partition là một log chia thành file _segment_; replication bằng Raft (ở dev dùng replication factor 1). Thread-per-core: mỗi core giữ phần dữ liệu và bộ nhớ riêng, hạn chế khoá chung; `--smp` chọn số core, `--memory` chặn bộ nhớ, `--overprovisioned` giảm tranh CPU khi chạy cùng nhiều dịch vụ khác. Retention xoá theo segment đã đóng.
**Điểm mạnh:** nhẹ và khởi động nhanh · dùng được client/thư viện Kafka sẵn có · ít thành phần cần vận hành · có Console.
**Điểm yếu và bẫy:** license kiểu source-available (BSL), một số tính năng chỉ ở bản trả phí · hệ sinh thái nhỏ hơn Kafka (Connect, ksqlDB, ...) · chế độ `dev-container` không phản ánh production · `advertise-kafka-addr` sai là lỗi kết nối phổ biến nhất · retention không chạy nếu segment chưa đóng (Task 1.4).
**Khi nào không dùng:** cần Kafka Connect/hệ sinh thái Confluent đầy đủ ➔ Kafka hoặc dịch vụ managed; cần SLA cụm lớn ➔ dịch vụ managed.
**Trong repo này:**

| File                              | Symbol / vị trí                     | Nhìn vào đâu để hiểu                                   |
| --------------------------------- | ----------------------------------- | ------------------------------------------------------ |
| `docker-compose.yml`              | service `redpanda`                  | Cờ `--smp`, `--memory`, hai listener internal/external |
| `scripts/create_topics.sh`        | `rpk topic create`                  | Số partition, `retention.ms`, `segment.ms`             |
| `src/producer/replay_producer.py` | `build_producer()`, `on_delivery()` | Cấu hình idempotent producer, vòng `poll`              |
| `src/streaming/ingest.py`         | `read_kafka()`                      | Các option nguồn Kafka của Spark                       |

```
producer = Producer({
    "bootstrap.servers": cfg.kafka_bootstrap_external,
    "enable.idempotence": True,     # chống trùng do retry nội bộ của producer
    "acks": "all",
    "compression.type": "lz4",
    "linger.ms": 20,
})
producer.produce(cfg.topic_raw, key=str(ev["campaign_id"]),
                 value=json.dumps(ev), on_delivery=on_delivery)
producer.poll(0)   # kích hoạt callback giao nhận; thiếu dòng này hàng đợi sẽ đầy

```

## T02 — Spark Structured Streaming (PySpark)

**Là gì:** engine xử lý phân tán; Structured Streaming coi luồng như một "bảng vô hạn" và chạy truy vấn DataFrame/SQL lặp lại theo từng lô (micro-batch).
**Thành phần:** _Driver_ và _Executor_ (local mode: cùng một JVM) · `SparkSession` · _Source_ (Kafka, Delta, file) · _Sink_ (Delta, Kafka, `foreachBatch`) · _Trigger_ · _Checkpoint_ · _State store_ · _Watermark_ · `StreamingQuery` · `StreamingQueryListener` · bộ tối ưu Catalyst.
**Cơ chế:** mỗi trigger Spark (1) ghi **dự kiến offset** của lô vào `checkpoint/offsets`, (2) chạy lô trên dữ liệu mới, (3) ghi sink, (4) ghi `checkpoint/commits`. Khi khởi động lại, lô đã có `offsets` mà chưa có `commits` sẽ **chạy lại** ➔ cần sink idempotent. Toán tử có trạng thái giữ state theo phiên bản và sao lưu vào `checkpoint/state`. `foreachBatch` cho phép dùng toàn bộ API batch (ghi nhiều sink, MERGE) cho mỗi lô.
**Điểm mạnh:** API quen thuộc (SQL/DataFrame) · cùng một code cho batch và stream · chịu lỗi nhờ checkpoint · hệ sinh thái connector (Kafka, Delta) · micro-batch dễ debug.
**Điểm yếu và bẫy:** nặng (JVM \~1.5–2GB ở local) · độ trễ tối thiểu ≈ trigger · khởi động chậm · cấu hình JAR/phiên bản dễ lệch · `spark.sql.shuffle.partitions` mặc định 200 gây nhiều file nhỏ · Python UDF chậm · state lớn cần RocksDB · quên `persist` khi dùng DataFrame hai lần.
**Khi nào không dùng:** dữ liệu nhỏ, chấp nhận độ trễ phút ➔ vòng lặp batch với DuckDB/dbt là đủ; cần mili-giây ➔ Flink/Kafka Streams.
**Trong repo này:**

| File                             | Symbol                                                          | Nhìn vào đâu                                 |
| -------------------------------- | --------------------------------------------------------------- | -------------------------------------------- |
| `src/streaming/spark_session.py` | `build_spark()`                                                 | `shuffle.partitions`, `s3a`, extension Delta |
| `src/streaming/ingest.py`        | `read_kafka()`, `process_batch()`                               | Parse, validate, ghi idempotent Bronze/DLQ   |
| `src/streaming/validation.py`    | `validate_df()`                                                 | Quy tắc validate bằng Column expression      |
| `src/streaming/refine.py`        | `build_silver_stream()`, `upsert_silver()`                      | Watermark, dedup, MERGE                      |
| `src/streaming/listener.py`      | `MetricsListener.onQueryProgress()`                             | Số đo tiến trình                             |
| `config/pipeline.yaml`           | `trigger_seconds`, `watermark_delay`, `max_offsets_per_trigger` | Tham số điều khiển                           |

```
raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", cfg.kafka_bootstrap_internal)
       .option("subscribe", cfg.topic_raw)
       .option("startingOffsets", "earliest")
       .option("maxOffsetsPerTrigger", cfg.max_offsets_per_trigger)   # backpressure
       .option("failOnDataLoss", cfg.fail_on_data_loss)
       .load())
parsed = (raw.select(F.col("value").cast("string").alias("payload"),
                     "partition", "offset", F.col("timestamp").alias("kafka_timestamp"))
          .withColumn("j", F.from_json("payload", EVENT_SCHEMA,
                      {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt_record"})))
query = (parsed.writeStream.foreachBatch(process_batch)
         .option("checkpointLocation", cfg.ckpt_ingest)
         .trigger(processingTime=f"{cfg.trigger_seconds} seconds").start())

```

## T03 — Delta Lake

**Là gì:** lớp _table format_ mã nguồn mở xây trên Parquet, thêm giao dịch ACID, phiên bản và hợp nhất dữ liệu lên object storage.
**Thành phần:** file dữ liệu Parquet · thư mục `_delta_log/` (commit JSON, checkpoint Parquet, `_last_checkpoint`) · các action (`add`, `remove`, `metaData`, `protocol`, `txn`, `commitInfo`) · _table features_ · API `DeltaTable` (`merge`, `optimize`, `vacuum`, `history`) · thư viện đọc không cần Spark (delta-kernel/delta-rs, là thứ DuckDB và test fixture dùng).
**Cơ chế:** xem F10. Ghi = tạo file dữ liệu rồi commit phiên bản mới bằng cách tạo file log tiếp theo (optimistic concurrency). Luồng đọc từ bảng Delta đọc các commit mới; commit chỉ gộp file (`dataChange=false`) được bỏ qua.
**Điểm mạnh:** ACID và snapshot isolation (đọc khi đang ghi) · MERGE/UPDATE/DELETE · time travel · schema enforcement/evolution · nhiều engine đọc được.
**Điểm yếu và bẫy:** nhiều file nhỏ khi ghi micro-batch · MERGE viết lại file (tốn I/O) · chạy OPTIMIZE đồng thời trên cùng partition đang MERGE sẽ gây xung đột · reader cũ có thể không đọc được bảng bật table feature mới (kiểm tra ở Task 3.1) · `VACUUM` với retention bằng 0 sẽ làm hỏng truy vấn DuckDB đang đọc · chọn cột partition có cardinality cao.
**Khi nào không dùng:** dữ liệu nhỏ, không cần cập nhật/ACID ➔ Parquet thuần; cần hệ sinh thái catalog đa-engine rộng ➔ cân nhắc Iceberg.
**Trong repo này:**

| File                                       | Symbol                  | Nhìn vào đâu                                       |
| ------------------------------------------ | ----------------------- | -------------------------------------------------- |
| `src/streaming/ingest.py`                  | `process_batch()`       | `txnAppId`/`txnVersion` cho ghi idempotent         |
| `src/streaming/refine.py`                  | `upsert_silver()`       | MERGE insert-if-not-matched có điều kiện partition |
| `src/streaming/maintenance.py`             | `compact()`, `vacuum()` | OPTIMIZE các partition đã đóng và xử lý xung đột   |
| `tests/integration/smoke_delta_interop.py` | `main()`                | Spark ghi ➔ DuckDB đọc, kể cả đồng thời            |

```
dt = DeltaTable.forPath(spark, cfg.silver_path)
# Chỉ nén các partition của ngày đã khép lại khi Q2 đang chạy để tránh xung đột MERGE
dt.optimize().where(f"event_date < '{current_sim_date}'").executeCompaction()
dt.vacuum()  # giữ retention mặc định (168h), không dùng vacuum(0) khi đang có reader
print(dt.history(5).select("version", "operation", "operationMetrics").toPandas())

```

## T04 — MinIO (S3-compatible Object Store)

**Là gì:** máy chủ lưu trữ object tương thích chuẩn Amazon S3, nhẹ, chạy bằng một binary Go duy nhất, đi kèm giao diện quản trị MinIO Console.
**Thành phần:** MinIO server (cổng S3 API `9000`, Console `9001`) · `mc` (MinIO Client CLI, dùng cho `minio-init`) · xác thực (`MINIO_ROOT_USER`/`MINIO_ROOT_PASSWORD`) · _bucket_ (`lakehouse`) · multipart upload · SDK client (`boto3`, `s3a`, `httpfs`).
**Cơ chế:** key phẳng + HTTP (`PUT`, `GET`, `HEAD`, `DELETE`, `LIST`); object bất biến; multipart cho file lớn; không có rename nguyên tử (xem F08). Toàn bộ endpoint và credential được đọc từ `.env` để không khóa chặt code vào một implementation.
**Điểm mạnh:** cài đặt đơn giản, RAM thấp, tương thích tốt với `hadoop-aws` (`s3a://`) và DuckDB (`httpfs`), có sẵn giao diện Web UI để duyệt file Delta và `_delta_log`.
**Điểm yếu và bẫy:** nhiều file nhỏ và `LIST` chậm trên bind mount NTFS · bắt buộc bật `path-style` (`path.style.access=true` ở Spark, `URL_STYLE 'path'` ở DuckDB) khi chạy local · phải pin rõ phiên bản image thay vì dùng `latest`.
**Khi nào không dùng:** cần ghi cập nhật dòng thường xuyên với độ trễ thấp ➔ CSDL OLTP.
**Trong repo này:**

| File                             | Symbol                | Nhìn vào đâu                                             |
| -------------------------------- | --------------------- | -------------------------------------------------------- |
| `scripts/s3_conformance.py`      | `run_checks()`        | Các thử nghiệm S3 API của ADR-001                        |
| `docker-compose.yml`             | `minio`, `minio-init` | Image MinIO đã pin, volume ở ổ D, tạo bucket `lakehouse` |
| `src/streaming/spark_session.py` | `fs.s3a.*`            | endpoint, `path.style.access`, credentials               |
| `src/dashboard/db.py`            | `CREATE SECRET`       | Cấu hình `httpfs` của DuckDB với `s3_endpoint_host_port` |

```
s3 = boto3.client("s3", endpoint_url=cfg.s3_endpoint,
                  aws_access_key_id=cfg.s3_access_key, aws_secret_access_key=cfg.s3_secret_key,
                  region_name=cfg.s3_region, config=Config(s3={"addressing_style": "path"}))
s3.upload_file(big_file, cfg.s3_bucket, "_conf/big.bin")   # kiểm tra multipart trên MinIO

```

## T05 — DuckDB

**Là gì:** CSDL phân tích **nhúng** (chạy trong tiến trình ứng dụng), lưu/xử lý theo cột, đọc trực tiếp CSV/Parquet/Delta (cả trên MinIO/S3).
**Thành phần:** parser và optimizer · bộ thực thi vector hoá, song song · hệ thống **extension** (`httpfs`, `delta`, ...) · **secrets manager** · lưu trữ (file `.duckdb` hoặc `:memory:`) · client Python · thiết lập `memory_limit`, `threads`, `extension_directory`.
**Cơ chế:** pipeline _push-based_ xử lý theo lô giá trị, chia công việc cho nhiều luồng. `delta_scan(path)` đọc snapshot từ `_delta_log` rồi quét các Parquet hợp lệ qua `httpfs` và đẩy điều kiện lọc xuống khi có thể.
**Điểm mạnh:** không cần server · rất nhanh cho truy vấn phân tích trên một máy · SQL phong phú (window, `FILTER`, ...) · đọc thẳng lake ➔ không phải sao chép · dễ viết test và dùng để quét nhanh cả 17 file CSV ở Task 2.1.
**Điểm yếu và bẫy:** file `.duckdb` là single-writer · không phải server nhiều người dùng · hỗ trợ Delta là extension (kiểm tra tính năng/phiên bản đã pin) · `ENDPOINT` trong `CREATE SECRET` **không được có `http://`** · phải đặt giới hạn bộ nhớ · extension tải từ mạng (nên cài sẵn trong image).
**Khi nào không dùng:** nhiều người dùng đồng thời với yêu cầu độ trễ rất thấp hoặc cần tổng hợp liên tục khi ghi ➔ ClickHouse (xem F15, Task 7.3).
**Trong repo này:**

| File                          | Symbol                                     | Nhìn vào đâu                                                             |
| ----------------------------- | ------------------------------------------ | ------------------------------------------------------------------------ |
| `src/dashboard/db.py`         | `get_root_connection()`, `run_query()`     | `:memory:`, secret dùng `s3_endpoint_host_port`, `cursor()` mỗi truy vấn |
| `src/dashboard/queries/*.sql` | `pacing.sql`, `publisher_quality.sql`, ... | Công thức Phần 2 dưới dạng SQL                                           |
| `scripts/profile_tracking.py` | `profile()`                                | DuckDB đọc và phân loại 17 file CSV                                      |
| `dbt_cpc/profiles.yml`        | `extensions`, `secrets`                    | dbt chạy trên DuckDB đọc Delta                                           |

```
con = duckdb.connect(":memory:")                     # không có file ➔ không có khoá file
con.execute("SET memory_limit='256MB'")
con.execute("SET threads=2")
con.execute("LOAD httpfs")
con.execute("LOAD delta")
con.execute(f"""CREATE OR REPLACE SECRET lake (TYPE S3, KEY_ID '{cfg.s3_access_key}',
                SECRET '{cfg.s3_secret_key}', ENDPOINT '{cfg.s3_endpoint_host_port}',
                URL_STYLE 'path', USE_SSL false, REGION '{cfg.s3_region}')""")
df = con.cursor().execute(
    "SELECT campaign_id, SUM(spend) AS spend FROM delta_scan('s3://lakehouse/silver/cpc_events') "
    "WHERE event_date = ? AND event_type = 'click' GROUP BY 1", [day]).df()

```

(Không bao giờ log câu lệnh `CREATE SECRET`; `cfg.s3_endpoint_host_port` ở dạng `minio:9000`, không kèm `http://`.)

## T06 — dbt (với adapter dbt-duckdb)

**Là gì:** công cụ biến đổi dữ liệu bằng SQL + Jinja: biên dịch project thành các câu SQL rồi chạy theo thứ tự phụ thuộc trên engine đích. dbt **không** lưu hay di chuyển dữ liệu.
**Thành phần:** `dbt_project.yml` · _models_ · _seeds_ (CSV tĩnh từ các file dimension) · _sources_ · _tests_ (generic và singular) · _macros_ · `ref()`/`source()` tạo DAG · `profiles.yml` · _materialization_ (`view`, `table`, `incremental`, `external` của dbt-duckdb) · tài liệu và lineage · thư mục `target/`.
**Cơ chế:** parse ➔ xây DAG ➔ render Jinja ra SQL ➔ chạy theo thứ tự. Mỗi test là một câu `SELECT` trả về các dòng _vi phạm_; có dòng ➔ test thất bại.
**Điểm mạnh:** SQL có cấu trúc, tái sử dụng · test và lineage tích hợp · logic kinh doanh tách khỏi luồng streaming · dễ review qua git.
**Điểm yếu và bẫy:** hoạt động theo lô (độ trễ = chu kỳ chạy) · mỗi lần chạy tốn vài giây khởi động · Jinja khó debug · tuỳ chọn đọc Delta/ghi external của dbt-duckdb phụ thuộc phiên bản · test chạy thêm truy vấn.
**Khi nào không dùng:** logic cần tính theo từng event trong luồng, hoặc độ trễ giây.
**Trong repo này:**

| File                                                 | Symbol        | Nhìn vào đâu                                  |
| ---------------------------------------------------- | ------------- | --------------------------------------------- |
| `dbt_cpc/models/staging/stg_cpc_events.sql`          | model         | ép kiểu, không có logic kinh doanh            |
| `dbt_cpc/models/gold/fct_campaign_hourly_pacing.sql` | model         | window `cum_spend`, `pacing_ratio`            |
| `dbt_cpc/seeds/`                                     | seed          | ngân sách và các bảng danh mục từ 17 file CSV |
| `dbt_cpc/models/schema.yml`                          | `tests:`      | generic test                                  |
| `dbt_cpc/tests/*.sql`                                | singular test | `spend >= 0`, đối soát                        |

```
# dbt_cpc/models/schema.yml (minh hoạ)
models:
  - name: stg_cpc_events
    columns:
      - name: event_id
        tests: [not_null, unique]
      - name: event_type
        tests:
          - accepted_values:
              values: [impression, click, apply, qualified, unqualified]

```

## T07 — Streamlit

**Là gì:** framework Python dựng ứng dụng dữ liệu; mô hình **chạy lại script từ trên xuống** mỗi khi người dùng tương tác.
**Thành phần:** script ứng dụng · widget · `st.session_state` · `st.cache_data` (cache kết quả) · `st.cache_resource` (cache đối tượng dùng chung) · `st.fragment` (chạy lại một phần, có `run_every`) · layout (`columns`, `tabs`).
**Cơ chế:** mỗi phiên trình duyệt chạy script trong một thread riêng, nói chuyện qua WebSocket. `cache_resource` chia sẻ **giữa mọi phiên** nên đối tượng được cache phải an toàn đa luồng (lý do dùng `cursor()` mỗi truy vấn). Fragment chỉ chạy lại hàm được trang trí.
**Điểm mạnh:** dựng UI bằng Python rất nhanh · tích hợp pandas/DuckDB · tự làm tươi một phần trang.
**Điểm yếu và bẫy:** không phải công cụ BI hay alerting (không có lập lịch, phân quyền hạn chế) · mô hình rerun dễ gây truy vấn thừa · mỗi phiên tốn RAM và một thread · làm tươi cả trang gây giật.
**Khi nào không dùng:** nhiều người dùng doanh nghiệp, cần phân quyền/lịch gửi báo cáo ➔ Superset/Metabase/Grafana.
**Trong repo này:**

| File                             | Symbol                  | Nhìn vào đâu           |
| -------------------------------- | ----------------------- | ---------------------- |
| `src/dashboard/app.py`           | `main()`                | bố cục 4 panel         |
| `src/dashboard/panels/pacing.py` | `pacing_panel()`        | fragment tự làm tươi   |
| `src/dashboard/db.py`            | `get_root_connection()` | `st.cache_resource`    |
| `src/dashboard/repository.py`    | các hàm truy vấn        | `st.cache_data(ttl=5)` |

```
@st.fragment(run_every="10s")        # chỉ chạy lại hàm này, không tải lại cả trang
def pacing_panel():
    sim_now = repo.sim_now()
    st.dataframe(repo.pacing(sim_now))
    st.caption(f"Cập nhật lúc {sim_now:%H:%M:%S} (giờ mô phỏng)")

```

## T08 — Docker, Compose và WSL2 trên Windows

**Là gì:** container runtime; Docker Desktop trên Windows chạy trong một máy ảo **WSL2**, toàn bộ image/layer/named volume nằm trong file đĩa ảo (`ext4.vhdx`/`docker_data.vhdx`) mặc định ở ổ C.
**Thành phần:** _image_ · _container_ · _named volume_ · _bind mount_ · _network_ · _Compose_ (`services`, `profiles`, `healthcheck`, `depends_on`, `mem_limit`, `logging`) · `.wslconfig` (giới hạn RAM/CPU, `sparseVhd`) · cài đặt Docker Desktop (vị trí disk image).
**Cơ chế:** named volume ở trong vhdx (nhanh, nhưng làm vhdx phình và không tự co); bind mount từ ổ Windows đi qua lớp chia sẻ file (đặt dữ liệu ở ổ D nhưng chậm hơn, đặc biệt với nhiều file nhỏ). Hai cách giải: bind mount sang D, hoặc đổi vị trí disk image sang D (Task 1.5).
**Điểm mạnh:** môi trường tái lập · cô lập phụ thuộc · profiles để bật/tắt nhóm dịch vụ · giới hạn tài nguyên từng dịch vụ.
**Điểm yếu và bẫy:** vhdx chỉ phình · I/O bind mount NTFS chậm · `localhost` trong container khác máy host (dùng tên service) · healthcheck/thứ tự khởi động · log không xoay vòng sẽ đầy đĩa · điều khoản license của Docker Desktop với tổ chức lớn.
**Khi nào không dùng:** môi trường production lớn ➔ Kubernetes/dịch vụ managed.
**Trong repo này:**

| File                     | Symbol                                            | Nhìn vào đâu                      |
| ------------------------ | ------------------------------------------------- | --------------------------------- |
| `docker-compose.yml`     | `profiles`, `healthcheck`, `mem_limit`, `logging` | kỷ luật tài nguyên                |
| `Makefile`               | `up`, `down`, `clean`, `nuke-data`, `disk-report` | thao tác chuẩn hoá                |
| `scripts/disk_report.sh` | —                                                 | số đo ổ C/D và `docker system df` |
| `infra/spark/Dockerfile` | `RUN`/`COPY` jars                                 | image Spark pin phiên bản         |

```
x-logging: &default-logging
  driver: json-file
  options: { max-size: "10m", max-file: "3" }
services:
  minio:
    image: minio/minio:<PIN_VERSION>
    mem_limit: 448m
    logging: *default-logging
    volumes: ["${DATA_ROOT}/minio:/data"]
    healthcheck: { test: ["CMD", "mc", "ready", "local"], interval: 5s, retries: 5 }

```

## T09 — Công cụ Python: confluent-kafka, pytest, ruff, pydantic, deltalake

**Là gì:** bộ công cụ quanh code Python của project.
**Thành phần:** `confluent-kafka` (binding của librdkafka: `Producer`, hàng đợi nội bộ, `poll`, `flush`, delivery report) · `pytest` (fixture, marker) · `ruff` (lint/format) · `pydantic` (model/validate) · `deltalake` (delta-rs: tạo fixture Delta không cần Spark).
**Cơ chế:** `produce()` chỉ **xếp vào hàng đợi nội bộ**; callback giao nhận chỉ được gọi khi bạn `poll()`/`flush()`; hàng đợi đầy gây `BufferError`. `pytest` marker (`spark`) tách test nặng. `deltalake` tạo bảng Delta trong thư mục tạm để test SQL của dashboard và parity.
**Điểm mạnh:** librdkafka nhanh · test chạy không cần Docker · lint nhanh.
**Điểm yếu và bẫy:** GIL giới hạn thông lượng một process · quên `flush()` mất message cuối · phiên bản protocol Delta do `deltalake` tạo có thể khác Spark (chỉ dùng cho fixture, không thay smoke test thật).
**Trong repo này:**

| File                              | Symbol                         | Nhìn vào đâu                      |
| --------------------------------- | ------------------------------ | --------------------------------- |
| `src/producer/replay_producer.py` | `main()`, `on_delivery()`      | vòng `produce`/`poll`/`flush`     |
| `tests/unit/test_time_mapper.py`  | `test_speed_factor_60()`       | ví dụ số của F01/F02              |
| `tests/conftest.py`               | `delta_fixture()`              | tạo Delta cục bộ bằng `deltalake` |
| `pyproject.toml`                  | `[tool.ruff]`, `[tool.pytest]` | cấu hình lint/test                |

```
@pytest.fixture
def delta_fixture(tmp_path):
    from deltalake import write_deltalake
    path = tmp_path / "silver"
    write_deltalake(str(path), make_events_df(), partition_by=["event_date"])
    return str(path)       # DuckDB delta_scan(path) đọc được ➔ test SQL không cần Docker

```

# 9. PHỤ LỤC

## Phụ lục A — Mẫu tài liệu mà Agent phải tạo

### A.1 Mẫu lesson (`docs/lessons/Fxx_*.md` và `Txx_*.md`)

```
# <Mã> — <Tên module>
## 1. Câu hỏi gốc            (vấn đề gì khiến khái niệm này tồn tại?)
## 2. Trực giác              (ẩn dụ đời thường, 3–5 câu)
## 3. Cơ chế                 (sơ đồ ASCII/mermaid + các bước; với công cụ: thành phần + luồng bên trong)
## 4. Ví dụ số               (con số cụ thể, tính tay được)
## 5. Điểm mạnh / Điểm yếu / Khi nào không dùng   (chỉ cho module công cụ)
## 6. Hiểu lầm phổ biến
## 7. Trong repo này
   | File:dòng | Symbol | Đoạn code (≤ 15 dòng, trích từ repo thật) | Giải thích dòng nào quan trọng |
## 8. Tự kiểm tra            (3 câu hỏi + đáp án ở cuối file)
## 9. Đọc thêm               (liên kết tới tài liệu chính thức của đúng phiên bản đã pin)

```

**Quy tắc:** mục 7 chỉ được hoàn thành khi code đã tồn tại; đoạn code phải **trích nguyên văn** từ repo (không viết lại), kèm `file:dòng`. Nếu code thay đổi, cập nhật lesson trong cùng commit.

### A.2 Mẫu `docs/code_map.md`

| Khái niệm        | Module | File:dòng                    | Symbol                | Cập nhật ở task |
| ---------------- | ------ | ---------------------------- | --------------------- | --------------- |
| Idempotent write | F06    | `src/streaming/ingest.py:NN` | `process_batch`       | 3.3             |
| Watermark        | F04    | `src/streaming/refine.py:NN` | `build_silver_stream` | 3.4             |
| ...              |        |                              |                       |                 |

### A.3 Mẫu ADR (`docs/adr/NNN-*.md`)

```
# ADR-NNN — <Quyết định>
- Trạng thái: Proposed | Accepted | Superseded by ADR-XXX
- Ngày: YYYY-MM-DD
## Bối cảnh        (ràng buộc, rủi ro)
## Các phương án   (≥ 2, kèm ưu/nhược)
## Bằng chứng       (số đo, link tới benchmark/progress)
## Quyết định
## Hệ quả           (được gì, mất gì)
## Điều kiện xem xét lại   (khi nào quyết định này hết hiệu lực)

```

### A.4 Mẫu `docs/progress/<task>.md`

```
# Task X.Y — <tên>
- Ngày/giờ hoàn thành:
- Lệnh AC đã chạy và output rút gọn:
- Số đo (nếu có): RAM / thời gian / số dòng
- Lệch so với backlog (Deviations) và lý do:
- Điều học được / điều làm tôi bất ngờ:

```

## Phụ lục B — Sổ rủi ro (Risk Register)

| ID  | Rủi ro                                                                                  | Xác suất            | Tác động   | Giảm thiểu                                                                                              | Task          |
| --- | --------------------------------------------------------------------------------------- | ------------------- | ---------- | ------------------------------------------------------------------------------------------------------- | ------------- |
| K1  | Cú pháp endpoint S3 khác nhau giữa Spark (`http://minio:9000`) và DuckDB (`minio:9000`) | Cao                 | Trung bình | Tách `s3_endpoint_host_port` trong `config.py`, test ở Task 1.1                                         | 1.1, 4.1, 5.1 |
| K2  | Lệch phiên bản Spark ↔ Delta ↔ hadoop-aws ↔ DuckDB ↔ MinIO                              | Cao                 | Cao        | Smoke test liên thông sớm, ma trận phiên bản, pin                                                       | 3.1           |
| K3  | I/O bind mount NTFS chậm với nhiều file nhỏ                                             | Trung bình          | Trung bình | Đo A vs B, có phương án đổi vị trí disk image                                                           | 1.5           |
| K4  | Nhiều file nhỏ làm truy vấn chậm dần & xung đột giữa `OPTIMIZE` với `MERGE`             | Cao                 | Trung bình | Chỉ `OPTIMIZE` tự động trên các partition ngày cũ đã đóng, không dùng `vacuum(0)`                       | 3.8           |
| K5  | Mất event trễ vượt watermark ở Silver                                                   | Trung bình          | Trung bình | ADR-006, đo, phương án P2/P3                                                                            | 3.7, 7.2      |
| K6  | dbt-duckdb không hỗ trợ trực tiếp cách đọc Delta/ghi external như mô tả                 | Trung bình          | Trung bình | Hook `on-run-start` tạo view; ghi Deviations                                                            | 4.1, 4.4      |
| K7  | Extension `delta` của DuckDB thiếu tính năng cần thiết ở bản đã pin                     | Trung bình          | Cao        | Kiểm tra table features ở 3.1; nâng/hạ phiên bản; tắt feature phía Spark                                | 3.1           |
| K8  | Vượt RAM ngân sách                                                                      | Trung bình          | Trung bình | `mem_limit`, đo baseline, R14                                                                           | 1.6, 6.4      |
| K9  | Bỏ sót dữ liệu quan trọng hoặc ôm đồm quá nhiều bảng trong 17 file CSV                  | Cao                 | Cao        | Cổng Task 2.1 quét và phân loại toàn bộ 17 file thành 3 nhóm (Stream/Dim/Ignore), trình chủ dự án duyệt | 2.1           |
| K10 | Người xem hiểu nhầm phần mô phỏng là thật                                               | Trung bình          | Trung bình | Phân biệt rõ bảng thật từ CSV và phần mô phỏng trong README/UI                                          | 4.2, 6.5      |
| K11 | License của Redpanda/MinIO ảnh hưởng nếu repo công khai                                 | Thấp                | Thấp       | Ghi trong ADR-001/002                                                                                   | 1.2, 1.3      |
| K12 | Image nền (ví dụ `bitnami/*`) đổi chính sách phát hành                                  | Trung bình          | Trung bình | Dùng image chính thức, pin phiên bản                                                                    | 3.1           |
| K13 | Dashboard Streamlit không scale khi nhiều người xem                                     | Thấp (phạm vi demo) | Thấp       | Cache TTL, ghi giới hạn, Task 7.3                                                                       | 5.7, 7.3      |
| K14 | Phạm vi phình to, không bao giờ xong                                                    | Cao                 | Cao        | Làm theo thứ tự epic; loại bỏ các file CSV không phục vụ P1–P4; Epic 7 chỉ sau khi 1–6 xong             | 2.1, tất cả   |

## Phụ lục C — Câu hỏi mở dành cho chủ dự án (Agent hỏi khi đến task liên quan)

1. Sau khi xem bảng phân loại 17 file CSV ở Task 2.1, bạn đồng ý với danh sách các file được giữ lại (Stream + Dimension) và danh sách các file bị loại bỏ (Ignore) chứ? _(Task 2.1)_

2. Có được commit dữ liệu mẫu (hoặc một phần nhỏ đã ẩn danh) vào repo công khai không? Nếu không, README hướng dẫn tải dữ liệu thế nào? _(Task 2.1)_

3. Bạn chấp nhận quy tắc `spend = bid` cho click (H1) hay muốn một quy tắc khác sau khi xem dữ liệu `tracking.csv` và `campaign.csv`? _(Task 2.1)_

4. Bạn muốn dùng Telegram hay Discord cho cảnh báo? Có chấp nhận chế độ dry-run làm mặc định? _(Task 5.8)_

5. Ngưỡng cảnh báo ban đầu (90% ngân sách, 100 click không có hồ sơ, ...) có phù hợp với quy mô dữ liệu sau khi xem phân phối thật không? _(Task 4.2, 5.4)_

6. Máy của bạn dùng bind mount sang ổ D hay sẵn sàng đổi vị trí disk image của Docker Desktop? _(Task 1.5)_

7. Bạn muốn bật Epic 7 nào (Airflow, ClickHouse A/B, schema evolution) sau khi xong Epic 6? _(Task 7.x)_

8. Dự án công khai hay riêng tư (ảnh hưởng đến cách ghi license, ảnh chụp, dữ liệu)? _(Task 6.5)_

## Phụ lục D — Từ điển thuật ngữ

| Thuật ngữ              | Giải thích ngắn                                                               |
| ---------------------- | ----------------------------------------------------------------------------- |
| **CPC**                | Cost Per Click: trả tiền theo mỗi click                                       |
| **CTR**                | Click-Through Rate: click / impression                                        |
| **CPA / CPQA**         | Chi phí mỗi hồ sơ nộp / mỗi hồ sơ đạt                                         |
| **Budget pacing**      | Theo dõi tốc độ tiêu ngân sách so với kế hoạch trong ngày                     |
| **Click fraud**        | Click không đến từ người thật quan tâm (bot, traffic rác)                     |
| **Publisher**          | Đối tác hiển thị quảng cáo                                                    |
| **Micro-batch**        | Xử lý luồng bằng các lô nhỏ chạy lặp lại                                      |
| **Trigger**            | Chu kỳ chạy một micro-batch                                                   |
| **Checkpoint**         | Dữ liệu lưu để query streaming chạy tiếp sau khi dừng                         |
| **State store**        | Nơi lưu trạng thái của toán tử streaming (dedup, aggregation)                 |
| **Watermark**          | Mốc event time dùng để dọn state và quyết định event quá trễ                  |
| **Idempotent**         | Thực hiện nhiều lần cho cùng kết quả như một lần                              |
| **DLQ**                | Dead Letter Queue: nơi chứa bản ghi lỗi để xử lý sau                          |
| **Partition / Offset** | Phân vùng của topic / vị trí message trong partition                          |
| **Consumer lag**       | Khoảng cách giữa cuối log và vị trí đã xử lý                                  |
| **Bronze/Silver/Gold** | Ba tầng dữ liệu: thô / đã làm sạch / phục vụ nghiệp vụ                        |
| **Table format**       | Lớp quản lý nhiều file dữ liệu thành bảng có giao dịch (Delta, Iceberg, Hudi) |
| **`_delta_log`**       | Nhật ký giao dịch của bảng Delta                                              |
| **Compaction**         | Gộp nhiều file nhỏ thành ít file lớn                                          |
| **Zero-copy**          | Nhiều engine đọc chung dữ liệu tại chỗ, không sao chép                        |
| **Single-writer lock** | Một tiến trình ghi tại một thời điểm (file DuckDB)                            |
| **SLI / SLO**          | Chỉ số đo / mục tiêu của chỉ số đó                                            |
| **Hysteresis**         | Ngưỡng bật khác ngưỡng tắt để tránh dao động                                  |
| **ADR**                | Architecture Decision Record                                                  |
| **Manifest**           | Biên bản "sự thật" do producer ghi, dùng để đối soát                          |
| **Sim clock**          | Đồng hồ mô phỏng sau khi tua nhanh thời gian                                  |

## Phụ lục E — Thay đổi từ v1 sang v2

| Chủ đề        | v1                                                 | v2                                                                                                                                                                                      |
| ------------- | -------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Object store  | MinIO mặc định chưa pin rõ                         | Chốt MinIO (pin version, có healthcheck & `minio-init`), tách cấu hình S3 qua `.env` và xử lý `s3_endpoint_host_port` cho DuckDB                                                        |
| Business      | Bốn bài toán nêu ngắn                              | Công ty giả định, người dùng, user story, SLO, ví dụ số                                                                                                                                 |
| Metric        | Liệt kê tên                                        | Danh mục công thức, grain, edge case, xử lý chính xác `ideal_cum_spend` cho giờ đang chạy dở                                                                                            |
| Dữ liệu       | Chỉ giả định 2 file `tracking.csv` và `search.csv` | Khảo sát & phân loại toàn bộ **17 file CSV** trong `data/raw/` (Task 2.1) thành 3 nhóm: Stream Source, Dimension/Seed (`campaign.csv`, `job.csv`, `master_publisher.csv`...), và Ignore |
| Spark         | Hai luồng riêng, chưa nêu RAM                      | Một JVM nhiều query, Bronze/DLQ ghi idempotent (`txnVersion`), ngân sách RAM, tiền tệ truyền dạng string trong JSON                                                                     |
| Dedup         | Watermark + MERGE                                  | Giữ cả hai, giải thích lý do, thêm thực nghiệm P1/P3 và bộ đối soát                                                                                                                     |
| Small files   | Nêu trong kiến thức                                | Task compaction chỉ chạy tự động trên các partition ngày đã đóng để tránh đụng độ MERGE, cấm `vacuum(0)` khi đang có reader                                                             |
| Dashboard     | `st.cache_resource` một connection                 | `cursor()` mỗi truy vấn; alert tách thành worker có trạng thái, cooldown, hysteresis                                                                                                    |
| Test dbt      | `clicks >= qualified` ở mọi grain                  | Chỉ ở grain tích luỹ, mức cảnh báo; giải thích vì sao sai ở grain giờ                                                                                                                   |
| Đo lường      | Bảng trống trong README                            | Phương pháp benchmark, kịch bản B1–B6, cập nhật SLO bằng số thật                                                                                                                        |
| Học tập       | Danh sách câu hỏi                                  | 16 module nền tảng + 9 module công cụ, có ví dụ số, mẫu lesson, code map, 10 lab "phá hệ thống"                                                                                         |
| Orchestration | Không                                              | Airflow là tuỳ chọn (Epic 7), có cảnh báo RAM                                                                                                                                           |
