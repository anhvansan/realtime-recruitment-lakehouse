# ADR-009 — Quy tắc tính chi tiêu (Spend Rule) từ Giá thầu (Bid) và Loại sự kiện

- **Trạng thái:** Accepted
- **Ngày:** 2026-11-19
- **Tác giả:** Chủ dự án & AI Agent

## 1. Bối cảnh
Trong hệ thống Near Real-Time CPC AdTech Lakehouse, việc tính toán chính xác tổng chi tiêu (`spend`) theo thời gian thực có ý nghĩa sống còn đối với bài toán kiểm soát tốc độ tiêu ngân sách (Budget Pacing - P1) và đối soát tài chính (Finance Reconciliation - P4). Event trùng hoặc sai quy tắc tính tiền có thể làm lệch số liệu hàng nghìn USD. Cần chốt một quy tắc duy nhất, minh bạch và có thể kiểm chứng toán học để xác định giá trị `spend` cho từng sự kiện trong luồng streaming.

## 2. Các phương án
- **Phương án A (Khuyến nghị): Quy tắc CPC tĩnh trực tiếp (`spend = bid` với click, `0` với phi click)**  
  Mỗi sự kiện có `event_type = 'click'` sẽ trừ tiền advertiser một khoản chính xác bằng `bid` được ghi nhận tại thời điểm click. Mọi sự kiện khác trong phễu (`apply`, `qualified`, `unqualified`) không làm phát sinh chi tiêu (`spend = 0.0000`).
  - *Ưu điểm:* Idempotent tuyệt đối, có thể tính toán tức thì độc lập cho từng event mà không cần join với bảng trạng thái bên ngoài; khớp chính xác với bảng đối soát lịch sử của hệ thống.
  - *Nhược điểm:* Không mô phỏng được cơ chế đấu giá nhị phân động (Second-price auction / VCG).
- **Phương án B: Quy tắc đấu giá động (Dynamic Second-Price Auction)**  
  Tính `spend = giá_thầu_đối_thủ_cao_thứ_hai + 0.01`.
  - *Ưu điểm:* Phản ánh chân thực cơ chế sàn Ad Exchange phức tạp.
  - *Nhược điểm:* Đòi hỏi lưu trữ stateful đấu giá phức tạp trong Spark Streaming, làm tăng độ trễ và vượt quá phạm vi bài toán local lakehouse; dữ liệu gốc không có log các lượt bid thua.
- **Phương án C: Tính chi tiêu tổng hợp theo giờ từ `events.csv`**  
  Chỉ tính `spend` khi kết thúc giờ dựa trên bảng tổng hợp.
  - *Ưu điểm:* Đơn giản cho báo cáo tĩnh.
  - *Nhược điểm:* Đánh mất tính near real-time (không phục vụ được cảnh báo pacing trong vài phút).

## 3. Bằng chứng
Kiểm chứng trên dữ liệu thực tế bằng DuckDB tại Task 2.1:
1. Trong bảng kiểm soát lịch sử từ Cassandra `events.csv`:
   - `SELECT job_id, dates, hours, clicks, bid_set, spend_hour FROM 'data/raw/events.csv' LIMIT 3;`
   - Bản ghi: `Job 98, 108 clicks, bid_set = 2.0, spend_hour = 216.0`.
   - Ta thấy: `108 * 2.0 = 216.0` (khớp chính xác 100% công thức `spend = clicks * bid`).
2. Trong `tracking.csv`:
   - Toàn bộ 1,875 click hợp lệ đều có `bid` cố định cho từng click (từ 1.0 đến 7.0 USD).

## 4. Quyết định
**Chọn Phương án A**:
1. Với sự kiện `event_type = 'click'`:
   - `spend = bid` (biểu diễn dạng số `DECIMAL(18,4)` trong database và dạng chuỗi `string` khi truyền qua JSON).
   - Nếu `bid` bị null hoặc âm, sự kiện được coi là lỗi và đẩy vào DLQ (`error_type = malformed_decimal` hoặc `negative_amount`).
2. Với các sự kiện phi click (`apply`, `qualified`, `unqualified`):
   - Bắt buộc gán `spend = "0.0000"`.
3. Chỉ sự kiện `click` mới làm tăng tổng chi tiêu tích lũy (`cum_spend`).

## 5. Hệ quả
- *Được:*
  - Đảm bảo tính nhất quán dữ liệu (Data Consistency) và khả năng tái lập (Reproducibility): Replay lại luồng sự kiện luôn cho cùng một con số doanh thu và chi phí.
  - Phục vụ hoàn hảo cho unit test và đối soát tài chính với bảng `events.csv` ở Task 3.5 & 3.6.
- *Mất / Đánh đổi:*
  - Chấp nhận mô hình CPC đơn giản (First-price/Fixed bid), không mô phỏng đấu thầu thời gian thực (RTB).

## 6. Điều kiện xem xét lại
Quyết định này chỉ được xem xét lại nếu dự án nâng cấp thêm module mô phỏng Ad Exchange với log thầu đa đối thủ.
