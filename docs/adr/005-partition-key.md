# ADR-005 — Chọn `campaign_id` làm Kafka Partition Key cho `cpc_events_raw`

- **Trạng thái:** Accepted
- **Ngày:** 2026-11-25
- **Tác giả:** Chủ dự án & AI Assistant
- **Liên quan:** Task 2.3, Task 2.4, Task 2.5, F07 (Partitioning & Consumer Group), Lab L5

---

## 1. Bối cảnh

Topic `cpc_events_raw` trên Redpanda được thiết kế với **3 partition** (Task 2.4). Khi Replay Producer (Task 2.5) gửi sự kiện lên Redpanda, giá trị **Kafka message key** quyết định partition đích qua công thức băm `hash(key) % 3` (`crc32` trong `librdkafka`/`confluent-kafka`).

Bài toán đặt ra cần cân bằng giữa hai mục tiêu đối nghịch:
1. **Giữ thứ tự sự kiện theo ngữ cảnh nghiệp vụ (Ordering Locality):** Bài toán P1 (*Budget Pacing*) và P2 (*Publisher Quality*) cần dòng sự kiện của từng chiến dịch/job đi theo thứ tự tương đối. Thứ tự trong Kafka chỉ được bảo đảm **bên trong 1 partition** (không phải toàn topic).
2. **Tránh lệch tải phân vùng (Hot Partition / Data Skew):** Nếu một khoá duy nhất chiếm tỉ trọng quá lớn (`> 40%`), partition chứa khoá đó bị quá tải trong khi các partition còn lại nhàn rỗi.

---

## 2. Các phương án ứng viên

Ta đánh giá 3 ứng viên khoá trên tập dữ liệu Event Stream thật đã chốt ở Task 2.1 (**1,977 sự kiện** = 1,875 click CPC từ `tracking.csv` + 102 hồ sơ ứng tuyển từ `application.csv` đã ánh xạ `campaign_id`):

- **Phương án A — Dùng `campaign_id` làm key**
- **Phương án B — Dùng `job_id` làm key**
- **Phương án C — Dùng `publisher_id` làm key**

---

## 3. Bằng chứng thực nghiệm (DuckDB + CRC32 trên `data/raw/`)

Kết quả trích từ `python scripts/analyze_partition_skew.py`:

| Ứng viên khoá | Số khoá duy nhất (`COUNT(DISTINCT)`) | Tỷ lệ NULL ở tập gốc | Khoá Top-1 (Key / Số event) | **Top-1 key share** | Phân phối 3 partition (`DuckDB hash % 3`) | **Max partition share (DuckDB)** | Phân phối 3 partition (`CRC32 % 3` — Kafka) | **Max partition share (CRC32)** |
| :--- | :---: | :---: | :--- | :---: | :--- | :---: | :--- | :---: |
| **`campaign_id`** | **11** | **0%** *(sau ánh xạ 2 tầng)* | `222` (733 / 1,977) | **37.08%** | P0: 58.32% · P1: 35.00% · P2: 6.68% | **58.32%** | P0: 69.45% · P1: 28.38% · P2: 2.18% | **69.45%** |
| `job_id` | 46 | 0% | `188` (410 / 1,977) | **20.74%** | P0: 49.82% · P1: 33.03% · P2: 17.15% | **49.82%** | P0: 62.87% · P1: 21.75% · P2: 15.38% | **62.87%** |
| `publisher_id` | 4 | 11.76% ở `application.csv` (12 dòng NULL) | `1` (1,961 / 1,977) | **99.19%** | P0: 0.00% · P1: 0.05% · P2: 99.95% | **99.95%** | P0: 0.66% · P1: 0.15% · P2: 99.19% | **99.19%** |

### Phân tích chi tiết từng ứng viên & Hậu quả Hot Partition (`> 40%`)

1. **Loại bỏ ngay `publisher_id` (thảm hoạ Hot Partition điển hình):**
   - `publisher_id = '1'` (*Facebook modify*) chiếm tới **99.19%** (1,961 / 1,977) tổng lưu lượng. Hơn nữa, `application.csv` có tới 12/102 dòng bị NULL `publisher_id`.
   - **Hậu quả nghiêm trọng khi một khoá chiếm > 40% sự kiện:**
     - **Mất song song của Consumer Group:** 2 trong 3 partition gần như trống trơn (0.00%–0.66%); chỉ **1 partition duy nhất gánh ~99.2% tải**. Dù tăng số consumer hoặc số core Spark cũng vô ích vì theo quy tắc Kafka, một partition chỉ được đọc bởi tối đa 1 consumer trong cùng group.
     - **Consumer Lag & Độ trễ p99 tăng vọt:** Luồng xử lý bị thắt cổ chai tại 1 thread/1 partition.
     - **Lệch đĩa và Retention Segment:** Partition nóng ghi đầy file segment liên tục trong khi 2 partition còn lại không đủ dữ liệu để đóng segment đúng chu kỳ.
2. **So sánh `campaign_id` vs `job_id`:**
   - Cả hai đều giữ Top-1 key share dưới ngưỡng 40% (`campaign_id`: **37.08%**, `job_id`: **20.74%**).
   - Do số lượng `campaign_id` thực tế ở tập mẫu sạch ít (`11` chiến dịch), nên khi băm ngẫu nhiên vào 3 ô, 2 chiến dịch lớn (`222` = 37.08% và `48` = 28.12%) vô tình rơi vào cùng partition P0 (tạo `max_partition_share` = 58.32% DuckDB / 69.45% CRC32). `job_id` có 46 khoá nên phân tán mượt hơn một chút (49.82% / 62.87%).
   - Tuy nhiên, **bài toán P1 (Hourly Budget Pacing)** nhóm chỉ số theo **`campaign_id`** và một campaign có nhiều job: chọn `campaign_id` giữ **toàn bộ click + application của cùng 1 chiến dịch đi tuần tự trong cùng 1 partition**, giảm tối đa độ lệch thời gian tương đối giữa các event cùng chiến dịch và giữ đúng thiết kế Data Contract mục 3.3. Ở tải phát lại local (~50–200 events/s), độ lệch 58%–69% trên 3 partition vẫn dư sức xử lý dưới 1% công suất Redpanda/Spark.

---

## 4. Quyết định

**Chọn `campaign_id`** làm Kafka message partition key cho topic `cpc_events_raw`.

- Nếu một chiến dịch cá biệt chiếm tỷ trọng lớn ở bước benchmark tải cao hoặc mở rộng cụm, có sẵn 2 hướng xử lý dự phòng đã đo sẵn: đổi key sang `job_id` (giảm Top-1 share xuống `20.74%`) hoặc thêm key salting `f"{campaign_id}:{event_id[:2]}"`.

---

## 5. Hệ quả

- **Được:**
  - Thứ tự sự kiện theo từng `campaign_id` được bảo toàn hoàn toàn từ Producer tới Kafka partition.
  - Khớp với bảng ánh xạ Data Dictionary (0% NULL `campaign_id` trên toàn bộ 1,977 sự kiện sau ánh xạ 2 tầng) và thiết kế Data Contract mục 3.3.
  - Có bộ số liệu thực nghiệm làm nền đối chứng trực tiếp cho **Lab L5** (đổi key `campaign_id` ↔ `job_id` và quan sát Consumer Lag).
- **Mất / Đánh đổi:**
  - Vì chỉ có 11 chiến dịch và Top-2 (`222`, `48`) cộng lại chiếm 65.2%, partition P0 nhận ~58%–69% số message trên tập dữ liệu mẫu này (chấp nhận được ở quy mô local).

---

## 6. Điều kiện xem xét lại

Quyết định này cần đánh giá lại (chuyển sang `job_id` hoặc bật key salting) nếu:
1. Tập dữ liệu mở rộng có một `campaign_id` đơn lẻ vượt ngưỡng **> 40% tổng lưu lượng** ở thông lượng cao (`> 5,000 events/s`) gây **hot partition** và làm `consumer lag` của partition đó tăng liên tục vượt ngưỡng SLO (45s).
2. Số lượng partition của `cpc_events_raw` tăng lên (ví dụ 12–32 partition) để scale cụm consumer trong production mà cardinality của `campaign_id` quá nhỏ (`< 3×` số partition).
