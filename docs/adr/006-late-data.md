# ADR-006 — Chính sách Watermark, Khử trùng và Dữ liệu Đến Trễ

- **Trạng thái:** Accepted
- **Ngày:** 2026-11-28
- **Tác giả:** Chủ dự án & AI Agent

## 1. Bối cảnh
Trong luồng CPC AdTech (`cpc_events_raw`), `FaultInjector` mô phỏng ~2% sự kiện đến trễ từ **5–15 phút** theo đồng hồ mô phỏng (`late_ids.jsonl`, giữ nguyên `event_time` gốc nhưng đẩy lùi `emit_sim_time`). Tầng **Q2 Refine** ([src/streaming/refine.py](file:///d:/realtime-recruitment-lakehouse/src/streaming/refine.py)) cần vừa:
1. Đảm bảo bảng **Silver** không có trùng `event_id` (`COUNT(*) == COUNT(DISTINCT event_id)`).
2. Giới hạn dung lượng RAM của state store trong ngân sách máy 16GB (`driver_memory: 1536m`, `mem_limit: 2304m`).
3. Không làm mất tiền quảng cáo (`spend`) ngoài phần được quy định và giải thích minh bạch khi đối soát (`make reconcile`).

## 2. Các phương án
- **Phương án P1 — Stateful Watermark Dedup + Global Delta `MERGE` (đo ở 5m / 10m / 20m):**
  - Dùng `.withWatermark("event_time", delay).dropDuplicatesWithinWatermark(["event_id"])` kết hợp `MERGE WHEN NOT MATCHED` trong `foreachBatch`.
  - *Ưu điểm:* Chặn trùng trong RAM với state bị giới hạn kích thước theo `watermark_delay`; minh họa chuẩn cơ chế stateful streaming + thu hồi state (F04/F05).
  - *Nhược điểm:* Sự kiện trễ vượt quá ngưỡng watermark đang mở có thể bị loại bỏ ở lớp stateful (dù vẫn còn nguyên ở tầng Bronze).
- **Phương án P3 — Stateless Stream + Global Delta `MERGE` (bỏ dedup stateful):**
  - Bỏ `withWatermark` / `dropDuplicatesWithinWatermark`, chỉ `dropDuplicates(["event_id"])` trong từng micro-batch rồi dựa hoàn toàn vào Delta `MERGE` trên bảng Silver.
  - *Ưu điểm:* Không mất bất kỳ sự kiện trễ nào (`0` loss), `state = 0 bytes` (không cần checkpoint state store hay RocksDB). Ở quy mô bảng nhỏ (~2k dòng), bỏ qua bước shuffle stateful giúp nhanh hơn cả P1.
  - *Nhược điểm:* Khi bảng Silver phình to qua nhiều tháng/triệu dòng, mọi micro-batch đều phải kiểm tra trùng bằng `MERGE` trực tiếp lên các partition liên quan trên S3/MinIO.
- **Phương án P2 — Hybrid Backfill (`reconcile_silver`, mở rộng ở Task 7.2):**
  - Chạy P1 ở đường realtime để giữ state nhỏ, kết hợp một batch job định kỳ đọc ngược từ **Bronze** ➔ `MERGE` bù các bản ghi trễ vượt watermark vào **Silver**.
  - *Ưu điểm:* Vừa giữ stream nhẹ vừa bảo đảm Silver đầy đủ 100% sau mỗi chu kỳ bù.
  - *Nhược điểm:* Tăng độ phức tạp vận hành (thêm 1 job backfill định kỳ).

## 3. Bằng chứng thực nghiệm
Thực nghiệm chạy bằng [scripts/bench/exp_watermark.py](file:///d:/realtime-recruitment-lakehouse/scripts/bench/exp_watermark.py) trên dữ liệu `replay_ac_3_9.json` (`bronze_rows=1986`, `bronze_unique=1958`, `late_injected=35` trễ 5–15 phút), chia tuần tự thành **`40` micro-batch** (`maxFilesPerTrigger=1`) và đo trực tiếp bằng `MetricsListener` ([src/streaming/listener.py](file:///d:/realtime-recruitment-lakehouse/src/streaming/listener.py)):

| Cấu hình | Watermark | Silver Rows | Bị loại (Late / Bất ngờ) | Tiền hụt (`spend_loss`) | Max State Rows | Max State Bytes | Avg Batch (ms) | Tổng Wall (s) |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `P1_wm_5m` | `5 minutes` | 1,951 | **7** (`7 / 0`) | **`12.4000`** | 242 | 78,568 | 5,026.3 | 201.99 |
| `P1_wm_10m` | `10 minutes` | 1,958 | **0** (`0 / 0`) | **`0.0000`** | 339 | 97,144 | 4,788.2 | 192.83 |
| `P1_wm_20m` | `20 minutes` | 1,958 | **0** (`0 / 0`) | **`0.0000`** | 421 | 116,120 | 4,928.5 | 197.83 |
| `P3_merge_only` | Không dùng | 1,958 | **0** (`0 / 0`) | **`0.0000`** | **0** | **0** | **4,119.8** | **165.17** |

**Nhận xét số đo:**
1. Khi hạ watermark xuống **`5 minutes`**, hệ thống bắt đầu loại **7 event trễ** (100% đều nằm trong `late_ids.jsonl`), gây hụt **`12.4000`** tiền quảng cáo ở Silver; đồng thời giảm state từ 339 xuống 242 dòng.
2. Tại **`10 minutes`** và **`20 minutes`**, toàn bộ 1,958 event hợp lệ đều được giữ nguyên (`spend_loss = 0.0000`) vì khoảng cách thời gian giữa các event thực tế trong luồng đủ rộng và phần lớn trễ 5–15 phút chưa bị cắt trước khi batch kết thúc; state cực nhỏ (~95–113 KB, < 0.01% của `1536m` heap).
3. Bất ngờ thú vị: ở quy mô ~2k dòng, **`P3_merge_only` nhanh hơn ~14%** (`4,119.8 ms/batch` so với `4,788.2 ms/batch` của P1 10m) và dùng `0 byte` state vì không phải thực hiện shuffle stateful + ghi snapshot state store mỗi micro-batch.

## 4. Quyết định
Chọn mặc định **Phương án P1 với `watermark_delay: "10 minutes"`** trong [config/pipeline.yaml](file:///d:/realtime-recruitment-lakehouse/config/pipeline.yaml#L13), đồng thời giữ sẵn cờ `disable_stateful_dedup=True` (P3) trong [src/streaming/refine.py](file:///d:/realtime-recruitment-lakehouse/src/streaming/refine.py#L276) và hướng mở rộng P2 tại Task 7.2:
- **Lý do chọn P1 (10 phút) làm mặc định:**
  1. Trên dữ liệu thật của dự án, mốc `10 minutes` đạt **`spend_loss = 0.0000`** (nhờ `reconcile.py` vẫn đối soát được phần trễ `> 10 phút` nếu xảy ra trong luồng chạy liên tục dài hơn).
  2. Kích thước state tối đa chỉ **339 dòng (~95 KB)**, hoàn toàn an toàn trong driver heap `1536m` (không cần bật RocksDB ngay — khớp kết luận "đo trước, bật sau" ở [docs/progress/3.2.md](file:///d:/realtime-recruitment-lakehouse/docs/progress/3.2.md)).
  3. Bảo vệ hiệu năng Delta `MERGE` khi dữ liệu tích luỹ lớn dần (không phụ thuộc 100% vào quét S3) và giữ đúng giá trị sư phạm về stateful streaming + watermark của hệ thống Lakehouse.

## 5. Hệ quả
- **Được:** State store bị chặn trần kích thước theo cửa sổ 10 phút; `make reconcile` đối soát minh bạch mọi event trễ vượt watermark qua `late_ids.jsonl`; dễ dàng chuyển đổi sang P3 bằng 1 cờ cấu hình mà không sửa logic `MERGE`.
- **Mất / Đánh đổi:** Nếu chạy luồng liên tục có lưu lượng dày đặc (event liên tiếp cách nhau vài giây), các event trễ 10–15 phút có thể bị loại ở tầng Silver realtime (nhưng vẫn được bảo toàn 100% ở tầng **Bronze**).

## 6. Điều kiện xem xét lại
Quyết định này cần được xem xét lại và chuyển sang **P3** hoặc bật **P2 (`reconcile_silver`, Task 7.2)** khi xảy ra một trong các ngưỡng sau:
1. Tỷ lệ `spend_loss` do event trễ vượt watermark 10 phút vượt quá **0.5% tổng `spend` ngày** trên Dashboard/Reconciliation.
2. Lưu lượng dữ liệu tăng khiến `state_memory_used_bytes` từ `MetricsListener` vượt **40% `driver_memory` (~600 MB)** ➔ cần bật `spark.enable_rocksdb: true` hoặc chuyển sang P3 (nếu partition pruning của `MERGE` vẫn giữ `batch_duration_ms < 5s`).
