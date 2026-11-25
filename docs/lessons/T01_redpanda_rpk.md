# T01 — Redpanda và Công cụ Dòng lệnh `rpk` (Redpanda Keeper)

## 1. Câu hỏi gốc
Làm sao để cấu hình, kiểm soát tài nguyên và quản trị toàn diện Message Broker (tạo topic, đặt số partition, kiểm tra retention, thanh tra cluster health) mà không phải gõ các câu lệnh Java/Kafka cồng kềnh hay viết script cấu hình phức tạp?

## 2. Trực giác
Nếu Redpanda là chiếc xe đua F1 được tinh giản tối đa (viết bằng C++ thay vì Java), thì `rpk` chính là chiếc vô lăng thông minh tích hợp bảng điều khiển trung tâm: mọi thao tác từ nổ máy (`rpk redpanda start`), kiểm tra áp suất lốp (`rpk cluster status`) đến chia làn đường đua (`rpk topic create`) đều nằm trong một công cụ duy nhất với cú pháp ngắn gọn, trực quan.

## 3. Cơ chế
Redpanda và CLI `rpk` giao tiếp trực tiếp qua Admin API (cổng 9644) và Kafka API (cổng 9092):

```
[Developer / Script] ──► [rpk CLI]
                             │
                             ├─► Admin API (cổng 9644)  ──► Kiểm tra trạng thái Cluster, Node Health
                             └─► Kafka API (cổng 9092)  ──► Tạo Topic, chỉnh Partitions, Quản lý Offsets
```

**Các cờ cấu hình quan trọng trong Docker (`dev-container` mode):**
1. `--mode dev-container`: Bật chế độ tối ưu hóa cho môi trường phát triển cục bộ (tắt các kiểm tra phần cứng nghiêm ngặt của production).
2. `--smp 1`: Giới hạn Redpanda chỉ sử dụng 1 luồng xử lý CPU (Symmetric Multiprocessing) để tránh cạnh tranh CPU với Spark.
3. `--memory 512M`: Giới hạn vùng nhớ đệm heap tối đa 512MB RAM.
4. `--reserve-memory 0M`: Không giữ trước vùng nhớ dự phòng hệ điều hành.
5. `--overprovisioned`: Báo cho scheduler của Redpanda biết nó đang chạy trong môi trường chia sẻ tài nguyên (như Docker/WSL2).

## 4. Ví dụ số
- Cấu hình retention và segment cho topic `cpc_events_raw`:
  - `retention.ms = 3600000` (1 giờ).
  - `segment.ms = 600000` (10 phút).
- **Cạm bẫy Segment đóng:** Retention chỉ kích hoạt xóa các segment *đã đóng*. Nếu dữ liệu gửi ít và `segment.ms` để mặc định (7 ngày), segment đang ghi sẽ không bao giờ đóng ➔ Dữ liệu sẽ không bị xóa và tiếp tục phình ổ đĩa. Đặt `segment.ms = 10m` giúp Redpanda đóng segment định kỳ và dọn dẹp đúng hẹn 1 giờ.

## 5. Điểm mạnh / Điểm yếu / Khi nào không dùng
- **Điểm mạnh:** Tốc độ phản hồi cực nhanh (< 10ms); bộ công cụ `rpk` duy nhất gói gọn toàn bộ tác vụ; không có độ trễ dọn rác (GC Pauses) của máy ảo Java.
- **Điểm yếu:** Là công nghệ mới hơn so với lịch sử 15 năm của Apache Kafka (dù tương thích hoàn toàn giao thức API).
- **Khi nào không dùng:** Các hệ thống phụ thuộc chặt chẽ vào Kafka Connect plugins độc quyền viết bằng Java chạy nội tại trong broker.

## 6. Hiểu lầm phổ biến
1. *Hiểu lầm:* Redpanda không phải Kafka nên các thư viện Python như `confluent-kafka` không thể kết nối được.  
   *Sự thật:* Redpanda tương thích 100% ở tầng giao thức nhị phân của Kafka (Kafka Wire Protocol). Mọi client viết bằng Python, Go, Java, Spark đều giao tiếp bình thường mà không cần biết phía sau là Redpanda.
2. *Hiểu lầm:* Có thể chạy Redpanda với số RAM bất kỳ dưới 256MB.  
   *Sự thật:* Mức RAM khuyến nghị tối thiểu cho dev container là 512MB để broker khởi tạo đủ các buffer bảng định tuyến nội bộ.

## 7. Trong repo này

| File:dòng | Symbol | Đoạn code | Giải thích |
| :--- | :--- | :--- | :--- |
| `docker-compose.yml:83-88` | `healthcheck` | `rpk cluster status` | Sử dụng `rpk cluster status` làm lệnh probe healthcheck cho container Redpanda. |
| `infra/redpanda/create_topics.sh:17-48` | `ensure_topic` | Idempotent `rpk topic` | Kiểm tra bằng `rpk topic describe`, tạo mới (`create`) hoặc đồng bộ config (`alter-config`) với `retention.ms=3600000`, `segment.ms=600000`. |
| `src/producer/replay_producer.py:369-403` | `build_producer`, `_produce_with_backpressure` | Idempotent `confluent-kafka` | Bật `enable.idempotence=True`, `acks='all'`, nén `lz4`, `linger.ms=20`, xử lý `BufferError` bằng `poll(0.1)` và gọi `flush()` khi thoát. |

```yaml
    healthcheck:
      test: ["CMD-SHELL", "rpk cluster status || exit 1"]
      interval: 10s
      timeout: 5s
      retries: 5
      start_period: 10s
```

## 8. Tự kiểm tra
1. *Câu hỏi:* Lệnh nào của `rpk` được dùng để kiểm tra cấu hình retention toàn cluster?  
   *Đáp án:* `rpk cluster config get log_retention_ms`.
2. *Câu hỏi:* Tại sao trong môi trường dev cục bộ ta phải bật cờ `--overprovisioned`?  
   *Đáp án:* Để Seastar engine của Redpanda chấp nhận chia sẻ thời gian CPU với các tiến trình khác trên hệ điều hành host thay vì độc chiếm 100% lõi CPU.
3. *Câu hỏi:* Cổng 9644 trên container Redpanda có nhiệm vụ gì?  
   *Đáp án:* Cung cấp Admin REST API cho Redpanda để kiểm tra trạng thái sức khỏe cluster và các tác vụ bảo trì.

## 9. Đọc thêm
- Redpanda Quickstart Guide: https://docs.redpanda.com/current/get-started/
- RPK Command Reference: https://docs.redpanda.com/current/reference/rpk/
