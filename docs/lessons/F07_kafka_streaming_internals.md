# F07 — Nguyên lý Message Broker và Streaming Internals (Kafka / Redpanda)

## 1. Câu hỏi gốc
Làm sao để tiếp nhận hàng chục nghìn sự kiện click mỗi giây từ các website quảng cáo tuyển dụng mà không làm nghẽn hệ thống xử lý, đồng thời cho phép nhiều ứng dụng độc lập (Spark Streaming, Alerting Worker, Audit Logger) cùng tiêu thụ một luồng dữ liệu theo tốc độ riêng của chúng mà không làm mất mát thông tin khi một bên bị sự cố tạm thời?

## 2. Trực giác
Hãy tưởng tượng Message Broker giống như một băng chuyền bưu phẩm tại bưu điện trung tâm:
- **Topic:** Là các đường băng chuyền phân loại thư theo chủ đề (ví dụ băng chuyền thư thường, băng chuyền bưu phẩm chuyển phát nhanh).
- **Partition:** Để một băng chuyền không bị quá tải, người ta chia nó thành 3 làn song song.
- **Offset:** Mỗi gói hàng đặt lên làn sẽ được đóng một con dấu số thứ tự tăng dần liên tục (0, 1, 2, 3...).
- **Consumer Group:** Nhân viên bưu điện (các tiến trình đọc dữ liệu) đứng ở cuối mỗi làn. Họ chỉ cần nhớ con số offset cuối cùng mình đã nhặt. Nếu nhân viên phải đi uống nước (restart tiến trình), khi quay lại họ chỉ việc nhìn vào con số đã ghi sổ và tiếp tục nhặt gói hàng tiếp theo mà không lo bị sót hay lấy trùng.

## 3. Cơ chế
Bản chất của Kafka/Redpanda là một **Append-Only Commit Log** phân tán trên đĩa cứng:

```
[Producer (Python App)]
         │
         ▼  (Kafka Key: campaign_id ➔ hash(key) % 3)
┌────────────────────────────────────────────────────────────────────────┐
│ TOPIC: cpc_events_raw                                                  │
│                                                                        │
│ Partition 0: [offset 0] [offset 1] [offset 2] ... ──► [offset N]       │
│ Partition 1: [offset 0] [offset 1] [offset 2] ... ──► [offset M]       │
│ Partition 2: [offset 0] [offset 1] [offset 2] ... ──► [offset K]       │
└────────────────────────────────────────────────────────────────────────┘
         │                                       │
         ▼                                       ▼
[Spark Streaming (Consumer)]           [Alert Worker (Consumer)]
(Đọc độc lập, commit offset riêng)    (Đọc độc lập, commit offset riêng)
```

**Cạm bẫy sống còn: Dual Listeners (Internal vs External Advertised Listeners):**
Khi chạy broker trong container Docker, địa chỉ mạng bên trong container khác với mạng bên ngoài máy host:
- `internal://0.0.0.0:9092` được quảng bá (`advertise`) là `redpanda:9092` dành cho các container khác trong cùng mạng Docker (như Spark, Alert Worker).
- `external://0.0.0.0:19092` được quảng bá là `localhost:19092` dành cho các ứng dụng chạy ngoài máy host (như Replay Producer).
Nếu cấu hình sai `advertise-kafka-addr`, client ngoài host có thể kết nối được tới cổng nhưng sẽ nhận lại địa chỉ IP nội bộ không thể định tuyến, dẫn đến lỗi timeout bí ẩn khi gửi tin.

## 4. Ví dụ số
- Topic `cpc_events_raw` có 3 partition (0, 1, 2).
- Producer gửi 1,875 click với key `campaign_id`.
- Chiến dịch 222 (733 click) có `hash(222) % 3 = 1` ➔ Toàn bộ 733 click của Campaign 222 sẽ rơi vào đúng Partition 1 theo thứ tự thời gian tuyệt đối.
- Nhờ vậy, Spark khi đọc Partition 1 luôn bảo đảm được trật tự chuỗi thời gian của riêng Campaign 222 (in-order delivery per key).

## 5. Điểm mạnh / Điểm yếu / Khi nào không dùng
- **Điểm mạnh:** Thông lượng cực cao (hàng triệu message/giây); lưu trữ đĩa bền vững; giải phóng sự phụ thuộc trực tiếp giữa bên sản xuất và bên tiêu thụ dữ liệu (decoupling).
- **Điểm yếu:** Cần quản lý cấu hình partition hợp lý; việc xử lý dữ liệu đến trễ hoặc out-of-order đòi hỏi cơ chế Watermark ở tầng xử lý downstream.
- **Khi nào không dùng:** Khi chỉ cần giao tiếp gọi hàm đồng bộ 1-1 đơn giản (dùng gRPC hoặc REST API).

## 6. Hiểu lầm phổ biến
1. *Hiểu lầm:* Khi Consumer đọc xong một message, broker sẽ xóa ngay message đó khỏi bộ nhớ.  
   *Sự thật:* Broker giữ lại toàn bộ message cho đến khi hết thời hạn lưu trữ (`retention.ms`), bất kể đã có bao nhiêu consumer đọc.
2. *Hiểu lầm:* Có thể đọc dữ liệu có thứ tự tuyệt đối trên toàn bộ Topic.  
   *Sự thật:* Thứ tự chỉ được đảm bảo **bên trong từng Partition**. Nếu muốn các event của một chiến dịch có thứ tự, bắt buộc phải dùng `campaign_id` làm khóa phân vùng (Kafka message key).

## 7. Trong repo này

| File:dòng | Symbol | Đoạn code | Giải thích |
| :--- | :--- | :--- | :--- |
| `docker-compose.yml:68-76` | `command` | Dual Listeners Redpanda | Cấu hình Dual Listeners tách bạch mạng nội bộ container và cổng gắn ngoài host. |
| `scripts/analyze_partition_skew.py:73-170` | `analyze_candidates` | Phân tích skew DuckDB + CRC32 | Kiểm chứng phân phối 3 partition của `campaign_id`, `job_id`, `publisher_id` trên 1,977 sự kiện thật. |
| `docs/adr/005-partition-key.md:55-60` | `ADR-005` | Chốt `campaign_id` làm key | Giải thích bẫy hot partition qua số liệu `publisher_id = '1'` (99.19%). |

```yaml
    command:
      - redpanda start
      - --mode dev-container
      - --smp 1
      - --memory 512M
      - --reserve-memory 0M
      - --overprovisioned
      - --kafka-addr internal://0.0.0.0:9092,external://0.0.0.0:19092
      - --advertise-kafka-addr internal://redpanda:9092,external://localhost:19092
```

## 8. Tự kiểm tra
1. *Câu hỏi:* Tại sao `campaign_id` được chọn làm Kafka key thay vì `event_id`?  
   *Đáp án:* Để toàn bộ sự kiện của cùng một chiến dịch luôn được đưa vào cùng một partition, đảm bảo thứ tự thời gian và tránh phân tán dữ liệu khi tính toán Pacing.
2. *Câu hỏi:* Điều gì xảy ra nếu Replay Producer ngoài host kết nối vào cổng `9092` thay vì `19092`?  
   *Đáp án:* Kết nối bị từ chối vì cổng 9092 chỉ mở nội bộ cho mạng container và không được map ra host; ngoài host bắt buộc kết nối qua cổng 19092.
3. *Câu hỏi:* Consumer Group đem lại lợi ích gì cho tính sẵn sàng cao?  
   *Đáp án:* Cho phép nhiều worker chia sẻ tải đọc các partition song song và tự động cân bằng tải (rebalance) nếu một worker bị sự cố.

## 9. Đọc thêm
- Confluent Kafka Internals: https://developer.confluent.io/courses/apache-kafka/events/
- Redpanda Networking Guide: https://docs.redpanda.com/current/manage/networking/
