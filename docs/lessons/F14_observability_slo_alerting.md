# F14 — Observability, SLO và Đối soát Đúng–Sai (Reconciliation)

## 1. Câu hỏi gốc
Làm sao biết pipeline đang chạy đúng hay đang âm thầm làm mất/nhân đôi tiền quảng cáo *trước khi* người dùng trên Dashboard phát hiện số liệu sai?

## 2. Trực giác
Chạy một pipeline dữ liệu giống như lái máy bay: bạn không đợi đến khi hết xăng mới biết hỏng hóc mà phải nhìn bảng đồng hồ (**SLI/SLO**), và cuối mỗi chuyến bay phải kiểm đếm lại hành khách + hàng hóa (**Reconciliation**) so với danh sách xuất phát (**Manifest**).
- **SLI (Service Level Indicator):** Con số đo được (độ trễ p95, tỷ lệ DLQ, RAM Spark, chênh lệch `SUM(spend)`).
- **SLO (Service Level Objective):** Ngưỡng mục tiêu chấp nhận được (ví dụ: tổng trễ <= 45s, chênh lệch tiền ngoài phần trễ vượt watermark = `0.0000`).
- **Reconciliation Harness (`make reconcile`):** Biến câu hỏi mơ hồ *"hệ thống chạy ổn chưa?"* thành đúng 1 lệnh trả về `PASS` hoặc `FAIL` (exit code `0` / `1`).

## 3. Cơ chế 5 Hạng mục Đối soát & Tín hiệu Vàng
1. **5 tín hiệu vàng (Golden Signals) của pipeline:** Freshness (dữ liệu mới nhất cách hiện tại bao lâu), Volume (số dòng Bronze/Silver), Latency (thời gian xử lý mỗi batch), Error rate (tỷ lệ DLQ), và Resource saturation (RAM/Disk).
2. **Kiến trúc đối soát 5 điểm `(a)-(e)` trong `tests/integration/reconcile.py`:**
   - **(a) DLQ theo `error_type`:** Khớp tuyệt đối với `injected_corrupt.by_error_type` trong manifest.
   - **(b) Bronze count:** `COUNT(*) == sent_valid_unique + injected_duplicates`.
   - **(c) Silver uniqueness:** `COUNT(*) == COUNT(DISTINCT event_id)`.
   - **(d) Bronze − Silver:** Mọi `event_id` vắng mặt ở Silver bắt buộc phải thuộc `late_ids.jsonl` với `delay_minutes > watermark_minutes` (nếu thiếu bất kỳ bản ghi thường nào ➔ `FAIL`).
   - **(e) Tài chính (`SUM(spend)`):** `SUM(spend)` ở Silver phải bằng `expected_spend_total` trừ đúng tổng `spend` của các event bị loại do trễ ở `(d)`.

## 4. Ví dụ số
Với đợt chạy `ac_3_3` (`seed=42`, `rows_read=1977`):
- Manifest ghi nhận: `sent_valid_unique=1958`, `injected_duplicates=28`, `injected_corrupt=19`, `expected_spend_total="1589.5000"`.
- Nếu vô tình xóa 1 dòng `click` bình thường trị giá `1.5000` khỏi Silver:
  - Mục `(d)` báo `FAIL` (`unexpected_missing_count: 1`).
  - Mục `(e)` báo `FAIL` (`actual_silver_spend: "1588.0000"` vs `expected_silver_spend: "1589.5000"`).
  - Exit code trả về `1` để CI/Automation dừng ngay lập tức.

## 5. Điểm mạnh / Điểm yếu / Khi nào không dùng
- **Điểm mạnh:** Phát hiện tức thời mọi lỗi mất dữ liệu im lặng, lỗi logic watermark, hoặc nhân đôi tiền nhờ kiểm chứng chéo bằng DuckDB đọc trực tiếp MinIO.
- **Điểm yếu:** Chỉ kiểm tra được khi có **ground-truth manifest** (môi trường replay/test); ở production thực tế không có manifest thì phải chuyển sang đối soát chéo giữa Bronze và Silver (`Bronze - Silver` không giải thích được ➔ cảnh báo).
- **Khi nào không dùng:** Hệ thống thử nghiệm nhanh chưa chốt schema hay dữ liệu không cần cam kết toàn vẹn tài chính.

## 6. Hiểu lầm phổ biến
1. *Hiểu lầm:* Thấy job Spark Streaming không văng Exception nghĩa là dữ liệu đã đúng.  
   *Sự thật:* Mất event do đặt nhầm watermark, lọc nhầm `WHERE`, hay nhân đôi dòng khi `MERGE` sai điều kiện đều **không hề gây crash** — chỉ có bộ đối soát số học mới bắt được lỗi im lặng này.

## 7. Trong repo này

| File:dòng | Symbol | Giải thích |
| :--- | :--- | :--- |
| `tests/integration/reconcile.py:130-221` | `evaluate_reconciliation` | Hàm thuần kiểm tra đủ 5 hạng mục `(a)-(e)` và trả báo cáo JSON `PASS`/`FAIL`. |
| `tests/integration/reconcile.py:255-319` | `fetch_lakehouse_snapshot` | Dùng DuckDB `delta_scan` đọc trực tiếp Bronze/Silver/DLQ từ MinIO để đối soát độc lập với Spark. |
| `tests/unit/test_reconcile.py:55-162` | `test_reconcile_*` | Kiểm chứng logic đối soát trên host cho cả ca sạch, ca có event trễ >10m, và các ca cố tình làm hỏng dữ liệu. |
| `src/streaming/listener.py:157-263` | `MetricsListener` | Lắng nghe `onQueryProgress`: cập nhật tức thời `ops/latest.json` qua boto3 và đệm 60s ghi Delta `ops/stream_metrics`. |
| `tests/unit/test_listener.py:46-163` | `test_parse_query_progress_*`, `test_metrics_listener_*` | Kiểm chứng trích xuất `state_num_rows_total`, `state_memory_used_bytes`, `event_time_watermark`, và cơ chế buffer 60s. |
| `scripts/bench/resource_snapshot.sh:1` | `resource_snapshot.sh` | Ghi mẫu tài nguyên RAM/CPU Docker phục vụ theo dõi saturation (Task 1.6). |
| `docs/benchmarks/baseline.md:12` | `Resource Baseline` | Báo cáo đối chiếu RAM thực tế của các container so với ngân sách mục 4.4. |

## 8. Tự kiểm tra
1. *Câu hỏi:* Vì sao bộ đối soát ở `tests/integration/reconcile.py` dùng DuckDB thay vì dùng lại Spark `DataFrame` để tự kiểm tra mình?  
   *Đáp án:* Dùng một engine đọc độc lập (DuckDB `delta_scan`) tránh tình trạng "vừa đá bóng vừa thổi còi" — nếu cấu hình Spark đọc/ghi có lỗi tương thích Delta Log hay thiếu cột, DuckDB sẽ phát hiện ngay lập tức.
2. *Câu hỏi:* Khi cố tình xóa 1 dòng `click` bình thường khỏi Silver, những mục nào trong 5 mục `(a)-(e)` phải chuyển sang `FAIL`?  
   *Đáp án:* Mục `(d)` (vì dòng bị mất không nằm trong danh sách trễ vượt watermark) và mục `(e)` (vì `SUM(spend)` của Silver bị hụt đúng bằng `spend` của dòng vừa xóa).
