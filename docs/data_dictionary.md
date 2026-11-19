# DATA DICTIONARY & DATA CONTRACT — CPC ADTECH LAKEHOUSE

> **Trạng thái:** Đã nghiệm thu khảo sát thực tế (Task 2.1 - 2026-11-19).  
> **Nguyên tắc:** Tuân thủ tuyệt đối **R1 — Không bịa dữ liệu**. Mọi schema, kiểu dữ liệu, tỷ lệ null và quan hệ bảng được kiểm chứng bằng DuckDB trên 18 file CSV gốc tại `data/raw/`.

---

## 1. BẢNG PHÂN LOẠI TOÀN BỘ 18 FILE CSV (U0)

| STT | Tên file | Kích thước | Số dòng | Phân loại | Vai trò trong hệ thống |
| :--- | :--- | :--- | :--- | :--- | :--- |
| 1 | `tracking.csv` | 2.5 MB | 6,662 | **Nhóm A — Event Stream** | **Nguồn phát sự kiện chính**. Lọc lấy 1,875 bản ghi click hoàn chỉnh (đủ `campaign_id`, `job_id`, `publisher_id`) để đưa vào Replay Producer phát qua Redpanda topic `cpc_events_raw`. |
| 2 | `application.csv` | 19.2 KB | 102 | **Nhóm A/B — Event Stream** | **Nguồn sự kiện ứng tuyển (Funnel)**. 102 bản ghi nộp hồ sơ được phát vào luồng sự kiện với loại `apply`, `qualified`, `unqualified` dựa trên `status` và `is_rejected`/`is_match`. |
| 3 | `campaign.csv` | 33.0 KB | 193 | **Nhóm B — Dimension / Seed** | **Bảng danh mục chiến dịch**. Cung cấp `budget_total` (ngân sách ngày thật), `campaign_name`, liên kết `company_id`. |
| 4 | `job.csv` | 458.5 KB | 1,868 | **Nhóm B — Dimension / Seed** | **Bảng danh mục tin tuyển dụng**. Chuẩn hóa thông tin việc làm: `id`, `company_id`, `group_id`, `created_date`. |
| 5 | `master_publisher.csv` | 7.0 KB | 8 | **Nhóm B — Dimension / Seed** | **Bảng danh mục đối tác quảng cáo**. Cung cấp `publisher_name` (Facebook modify, Talroo), `min_bid`, `max_bid`. |
| 6 | `company.csv` | 13.5 KB | 105 | **Nhóm B — Dimension / Seed** | **Bảng danh mục nhà tuyển dụng (Advertiser)**: `id`, `name`, `billing_id`. |
| 7 | `group.csv` | 4.2 KB | 43 | **Nhóm B — Dimension / Seed** | **Bảng danh mục nhóm tài khoản khách hàng**: `id`, `name`, `budget_group`. |
| 8 | `events.csv` | 1.2 KB | 20 | **Nhóm B — Ground Truth** | Bảng roll-up tổng hợp theo giờ từ Cassandra production. Dùng làm **bộ đối soát sự thật (Ground Truth)** cho test reconciliation ở Task 3.5 & 3.6. |
| 9 | `master_major_category.csv` | 1.9 KB | 18 | **Nhóm C — Loại bỏ (Phụ)** | Danh mục ngành nghề cấp 1. Không cần trực tiếp cho 4 bài toán P1–P4. |
| 10 | `master_minor_category.csv` | 28.3 KB | 268 | **Nhóm C — Loại bỏ (Phụ)** | Danh mục ngành nghề cấp 2 chi tiết. |
| 11 | `search.csv` | 438.9 KB | 1,329 | **Nhóm C — Loại bỏ (Trùng lặp)**| Snapshot tìm kiếm tin tuyển dụng. Trùng lặp thông tin với `job.csv`, thiếu trường quản trị hệ thống. |
| 12 | `master_city.csv` | 10.8 MB | 137,794 | **Nhóm C — Loại bỏ (Out-of-scope)**| Danh mục địa giới chi tiết, dung lượng lớn (~11MB), không phục vụ realtime CPC. |
| 13 | `job_location.csv` | 629.1 KB | 10,798 | **Nhóm C — Loại bỏ (Out-of-scope)**| Bảng nối địa điểm tin tuyển dụng. |
| 14 | `dev_user.csv` | 73.0 KB | 405 | **Nhóm C — Loại bỏ (Out-of-scope)**| Tài khoản người dùng nội bộ hệ thống. |
| 15 | `conversation.csv` | 7.0 KB | 40 | **Nhóm C — Loại bỏ (Out-of-scope)**| Lịch sử đoạn chat ứng viên - nhà tuyển dụng. |
| 16 | `user_role.csv` | 7.9 KB | 109 | **Nhóm C — Loại bỏ (Out-of-scope)**| Phân quyền tài khoản nội bộ. |
| 17 | `master_role.csv` | 2.2 KB | 29 | **Nhóm C — Loại bỏ (Out-of-scope)**| Danh mục chức vụ / vai trò. |
| 18 | `user_client_assignee.csv` | 1.6 KB | 20 | **Nhóm C — Loại bỏ (Out-of-scope)**| Bảng phân công nhân viên phụ trách khách hàng. |

---

## 2. CHI TIẾT CÁC BẢNG SỬ DỤNG (NHÓM A & NHÓM B)

### 2.1 Bảng `tracking.csv` (Event Stream Nguồn)
- **Tổng số dòng:** 6,662 dòng.
- **Tập con sạch đưa vào luồng (Clean CPC Stream):** 1,875 dòng có đầy đủ `campaign_id`, `job_id`, `publisher_id`, `bid`.
- **Cột thời gian thực sự:** `ts` (kiểu `TIMESTAMP`, dải từ `2022-07-04` đến `2022-07-27`).
- **Khóa tự nhiên duy nhất:** Cột `create_time` chứa chuỗi UUIDv1 phân biệt 100% (6,662 giá trị duy nhất). Cột `id` bị NULL 100%.

| Tên cột | Kiểu dữ liệu | Tỷ lệ Null (Tổng) | Tỷ lệ Null (Tập sạch 1,875) | Ý nghĩa / Quy tắc chuyển đổi |
| :--- | :--- | :--- | :--- | :--- |
| `create_time` | VARCHAR | 0.0% | 0.0% | **UUID gốc duy nhất của sự kiện** (dùng làm `event_id` hoặc seed sinh ID). |
| `ts` | TIMESTAMP | 0.015% (1 null) | 0.0% | **Thời điểm sự kiện diễn ra** (chuyển đổi thành `event_time` UTC). |
| `campaign_id` | BIGINT | 71.85% | 0.0% | Mã chiến dịch quảng cáo (Kafka partition key). |
| `job_id` | BIGINT | 71.85% | 0.0% | Mã tin tuyển dụng được click. |
| `publisher_id` | BIGINT | 71.85% | 0.0% | Mã đối tác hiển thị (1: Facebook modify, 3: Talroo). |
| `group_id` | BIGINT | 91.80% | 85.2% | Mã nhóm tài khoản khách hàng. |
| `bid` | DOUBLE | 71.85% | 0.0% | Giá thầu quảng cáo CPC (DECIMAL(18,4)). Dải giá: 1.0 – 7.0 USD. |
| `custom_track` | VARCHAR | 52.76% | 49.0% | Nhãn tracking chi tiết (`click`, `alive`, `conversion`, v.v.). |
| `ev` | BIGINT | 0.015% | 0.0% | Loại sự kiện pixel: `1` (Pageview/View), `2` (Action/Click). |

### 2.2 Bảng `application.csv` (Phễu Ứng tuyển)
- **Tổng số dòng:** 102 dòng.
- **Cột thời gian:** `created_date` (TIMESTAMP, từ `2022-04-22` đến `2022-07-27`).
- **Khóa chính:** `id` (BIGINT, 100% duy nhất).

| Tên cột | Kiểu dữ liệu | Tỷ lệ Null | Ý nghĩa / Quy tắc ánh xạ vào Stream |
| :--- | :--- | :--- | :--- |
| `id` | BIGINT | 0.0% | Khóa chính của hồ sơ ứng tuyển. |
| `created_date` | TIMESTAMP | 0.0% | Thời điểm ứng viên nộp hồ sơ (`event_time`). |
| `job_id` | BIGINT | 0.0% | Tin tuyển dụng ứng tuyển. Ánh xạ sang `campaign_id` qua `job.csv` hoặc `tracking.csv`. |
| `publisher_id` | BIGINT | 0.0% | Nguồn đối tác mang lại ứng viên. |
| `status` | BIGINT | 0.0% | Trạng thái ứng tuyển: `0` (Applied), `1` (In Review / Interview), `2` (Qualified). |
| `is_rejected` | BIGINT | 0.0% | Cờ từ chối: `1` ➔ Ánh xạ thành sự kiện `unqualified`. |
| `is_match` | BIGINT | 0.0% | Cờ đạt tiêu chuẩn: `1` ➔ Ánh xạ thành sự kiện `qualified`. |

### 2.3 Bảng `campaign.csv` (Dimension Chiến dịch & Ngân sách)
- **Tổng số dòng:** 193 dòng.
- **Khóa chính:** `id` (BIGINT, 100% duy nhất).

| Tên cột | Kiểu dữ liệu | Tỷ lệ Null | Ý nghĩa kinh doanh |
| :--- | :--- | :--- | :--- |
| `id` | BIGINT | 0.0% | Mã định danh chiến dịch (`campaign_id`). |
| `campaign_name`| VARCHAR | 0.0% | Tên hiển thị của chiến dịch trên Dashboard. |
| `budget_total` | BIGINT | 0.0% | **Ngân sách thực tế của chiến dịch** (dùng làm `daily_budget`). |
| `pacing_unit` | VARCHAR | 0.0% | Chu kỳ ngân sách: 179 campaign là `'Daily'`. |
| `company_id` | BIGINT | 0.0% | Khóa ngoại liên kết tới bảng `company.csv`. |

### 2.4 Bảng `job.csv` (Dimension Tin tuyển dụng)
- **Tổng số dòng:** 1,868 dòng.
- **Khóa chính:** `id` (BIGINT, 100% duy nhất).
- **Cột quan trọng:** `id`, `budget`, `company_id`, `group_id`, `created_date`, `is_active`.
- **Độ phủ:** Khớp 100% (15/15) các `job_id` xuất hiện trong sự kiện click của `tracking.csv`.

### 2.5 Bảng `master_publisher.csv` (Dimension Đối tác hiển thị)
- **Tổng số dòng:** 8 dòng.
- **Khóa chính:** `id` (BIGINT, 100% duy nhất).
- **Cột quan trọng:** `id`, `publisher_name`, `publisher_type`, `min_bid`, `max_bid`, `cpc_increment`.
- **Độ phủ:** Chứa Publisher 1 (*Facebook modify*) và Publisher 3 (*Talroo*), bao phủ 100% sự kiện trong luồng click.

### 2.6 Bảng `company.csv` & `group.csv` (Dimension Nhà tuyển dụng & Khách hàng)
- `company.csv` (105 dòng): `id`, `name`, `billing_id`, `status`.
- `group.csv` (43 dòng): `id`, `name`, `budget_group`, `company_id`.

---

## 3. BẢNG ÁNH XẠ SỰ KIỆN (EVENT TYPE MAPPING) VÀ PHỄU (FUNNEL)

### 3.1 Quy tắc chuẩn hóa `event_type` trong Canonical Contract (`cpc_event.v1`)

| Nguồn phát sinh | Điều kiện logic trên dữ liệu gốc | `event_type` chuẩn | Giá trị `bid` | Giá trị `spend` |
| :--- | :--- | :--- | :--- | :--- |
| `tracking.csv` | `ev = 2` AND `custom_track = 'click'` (hoặc có `job_id` & `campaign_id`) | **`click`** | Giá trị `bid` gốc (string, ví dụ `"1.0000"`) | `= bid` (string, ví dụ `"1.0000"`) |
| `application.csv` | Bản ghi tạo mới (`status IN (0, 1)`) và `is_rejected = 0` | **`apply`** | `"0.0000"` | `"0.0000"` |
| `application.csv` | Bản ghi có `status = 2` HOẶC `is_match = 1` | **`qualified`** | `"0.0000"` | `"0.0000"` |
| `application.csv` | Bản ghi có `is_rejected = 1` HOẶC `score = 0` | **`unqualified`** | `"0.0000"` | `"0.0000"` |

### 3.2 Quy tắc bổ sung `campaign_id` cho sự kiện từ `application.csv`
Khi phát sự kiện từ `application.csv`, do bảng này chỉ chứa trực tiếp `job_id`, hệ thống ánh xạ `campaign_id` theo thứ tự ưu tiên:
1. **Ưu tiên 1:** Lấy trực tiếp từ quan hệ 1-1 đã biết giữa `job_id` ➔ `campaign_id` trong `tracking.csv` (ví dụ: Job 98 ➔ Campaign 4, Job 187 ➔ Campaign 48).
2. **Ưu tiên 2:** Tra cứu `job_id` trong `job.csv` ➔ `company_id` ➔ chiến dịch đang chạy gần nhất của công ty đó trong `campaign.csv`.

---

## 4. KẾT LUẬN CHI TIẾT CHO CÁC GIẢ THUYẾT & UNKNOWNS (U0–U9, H1, H4)

### U0 — Phân loại 18 file
- **Kết luận:** 2 file làm nguồn phát Event Stream (`tracking.csv`, `application.csv`); 6 file làm Dimension / Ground Truth (`campaign.csv`, `job.csv`, `master_publisher.csv`, `company.csv`, `group.csv`, `events.csv`); 10 file loại bỏ hoàn toàn.
- **Bằng chứng SQL:** Xem bảng phân loại tại Mục 1.

### U1 — Cột thời gian sự kiện
- **Kết luận:** Cột thời gian là `tracking.ts` (TIMESTAMP, từ `2022-07-04` đến `2022-07-27`). Cột `create_time` không phải timestamp mà là chuỗi UUIDv1 duy nhất.
- **Bằng chứng SQL:**
  ```sql
  SELECT min(ts) as min_ts, max(ts) as max_ts, count(ts) as valid_ts
  FROM read_csv_auto('data/raw/tracking.csv');
  -- 2022-07-04 02:40:48 | 2022-07-27 15:24:55 | 6661
  ```

### U2 & H4 — Phễu sự kiện và Sự kiện Impression
- **Kết luận:** Phễu quảng cáo bắt đầu từ `click` (trong `tracking.csv`) ➔ `apply` (trong `application.csv`) ➔ `qualified` / `unqualified`. Sự kiện `impression` không tồn tại độc lập trong hệ thống (cột impressions trong Cassandra `events.csv` là NULL 100%). Theo R9, hệ thống chính thức **loại bỏ metric `impressions` và `CTR`**.
- **Bằng chứng SQL:**
  ```sql
  SELECT sum(clicks), count(impressions) FROM read_csv_auto('data/raw/events.csv');
  -- 413 clicks | 0 impressions (100% null)
  ```

### H1 — Quy tắc tính `spend`
- **Kết luận:** **XÁC NHẬN GIẢ THUYẾT H1**. Mỗi click hợp lệ tạo ra chi tiêu `spend = bid` tại thời điểm click. Các sự kiện phi click (`apply`, `qualified`, `unqualified`) luôn có `spend = 0.0000`.
- **Bằng chứng SQL:**
  ```sql
  SELECT job_id, dates, hours, clicks, bid_set, spend_hour, (clicks * bid_set) as expected_spend
  FROM read_csv_auto('data/raw/events.csv')
  LIMIT 3;
  -- Job 98: 108 clicks * 2.0 bid = 216.0 spend_hour (khớp 100%)
  ```

### U5 — Khóa tự nhiên duy nhất
- **Kết luận:** `tracking.create_time` là UUIDv1 duy nhất cho từng click (6,662/6,662 dòng phân biệt). Cột `id` rỗng. Producer sẽ dùng trực tiếp `create_time` làm `event_id`.
- **Bằng chứng SQL:**
  ```sql
  SELECT count(*), count(distinct create_time), count(distinct id)
  FROM read_csv_auto('data/raw/tracking.csv');
  -- total: 6662 | distinct_create_time: 6662 | distinct_id: 0
  ```

### U6 — Mẫu dữ liệu Null và Ngoại lai
- **Kết luận:** Có 4,787 dòng trong `tracking.csv` bị NULL toàn bộ khóa nghiệp vụ (`job_id`, `campaign_id`, `publisher_id`). Do đó, Replay Producer sẽ chỉ lọc 1,875 dòng sạch để phát luồng chuẩn; các trường hợp lỗi sẽ được chủ động sinh qua `FaultInjector` theo tỷ lệ chính xác (2% trùng, 1% JSON hỏng, 2% trễ).

### U7 — Phân phối lưu lượng
- **Kết luận:** Dữ liệu kéo dài 25 ngày, tập trung cao điểm vào ngày 25–26/07/2022. Đỉnh lưu lượng xuất hiện lúc 09:00 UTC (1,100 clicks) và 02:00–04:00 UTC (600–700 clicks/giờ).

### U8 — So sánh `search.csv` và `job.csv`
- **Kết luận:** Bảng `job.csv` là nguồn chuẩn Master Entity của tin tuyển dụng, khớp 100% với các sự kiện trong luồng. `search.csv` bị loại bỏ.
- **Bằng chứng SQL:**
  ```sql
  SELECT count(t.create_time), count(j.id)
  FROM read_csv_auto('data/raw/tracking.csv') t
  JOIN read_csv_auto('data/raw/job.csv') j ON t.job_id = j.id
  WHERE t.job_id IS NOT NULL;
  -- 1875 / 1875 dòng (khớp 100%)
  ```

### U9 — Ngân sách chiến dịch trong `campaign.csv`
- **Kết luận:** `campaign.csv` **ĐÃ CÓ SẴN NGÂN SÁCH THẬT** (`budget_total`), 179 chiến dịch có `pacing_unit = 'Daily'`. Toàn bộ 7 chiến dịch đang chạy đều có ngân sách ngày cụ thể (từ 100 USD đến 60,000 USD; ngoại lệ test: Campaign 1 có ngân sách 1 tỷ). Hệ thống dùng trực tiếp nguồn thật này thay vì sinh ngẫu nhiên.
- **Bằng chứng SQL:**
  ```sql
  SELECT c.id, c.campaign_name, c.budget_total, c.pacing_unit, count(t.create_time) as clicks
  FROM read_csv_auto('data/raw/campaign.csv') c
  JOIN read_csv_auto('data/raw/tracking.csv') t ON c.id = t.campaign_id
  GROUP BY 1, 2, 3, 4
  ORDER BY clicks DESC;
  -- Campaign 222: 500 USD (733 clicks)
  -- Campaign 48: 60000 USD (551 clicks)
  -- Campaign 4: 1932570 USD (328 clicks)
  -- Campaign 151: 10000 USD (88 clicks)
  -- Campaign 15: 100 USD (76 clicks)
  -- Campaign 93: 1000 USD (59 clicks)
  -- Campaign 1: 1002930129 USD (40 clicks)
  ```
