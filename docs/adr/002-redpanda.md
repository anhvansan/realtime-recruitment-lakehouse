# ADR-002 — Sử dụng Redpanda thay thế Apache Kafka + ZooKeeper cho Môi trường Local

- **Trạng thái:** Accepted
- **Ngày:** 2026-11-19
- **Tác giả:** Chủ dự án & AI Agent

## 1. Bối cảnh
Hệ thống streaming cần một Message Broker tương thích Kafka API để nhận sự kiện từ Replay Producer và cấp dữ liệu cho Spark Structured Streaming. Trên máy tính cá nhân 16GB RAM với tài nguyên hạn chế, việc chạy một cụm Apache Kafka truyền thống (thường đòi hỏi ít nhất 1 node ZooKeeper/KRaft và 1 broker Kafka chạy trên máy ảo Java - JVM) thường ngốn từ 1.5GB đến 2.5GB RAM và thời gian khởi động lâu.

## 2. Các phương án
- **Phương án A (Khuyến nghị): Redpanda (Official image pin tag `redpandadata/redpanda:v24.2.14`)**
  - *Ưu điểm:* Viết bằng C++ (Seastar framework), không cần JVM, không cần ZooKeeper/KRaft ngoài; khởi động cực nhanh (< 2 giây); tương thích 100% với Kafka API v2.0+ (dùng chung thư viện `confluent-kafka` và Spark Kafka connector); tích hợp sẵn CLI quản trị `rpk` cực kỳ mạnh mẽ; tiêu thụ RAM giới hạn ở mức 704MB (`--memory 512M --smp 1`).
  - *Nhược điểm:* Khác biệt về một số lệnh cấu hình nội bộ nâng cao so với Kafka gốc (nhưng các API giao tiếp producer/consumer/admin client là đồng nhất).
- **Phương án B: Apache Kafka đơn node (KRaft mode)**
  - *Ưu điểm:* Mã nguồn chuẩn Apache gốc.
  - *Nhược điểm:* Chạy trên JVM, tốn RAM cơ sở lớn (> 1.2GB); khởi động chậm hơn; công cụ dòng lệnh phân mảnh.
- **Phương án C: Kafka image `bitnami/kafka`**
  - *Ưu điểm:* Phổ biến trên Docker Hub.
  - *Nhược điểm:* Vi phạm quy tắc R4 của dự án (cấm image Bitnami do kích thước nặng và chính sách bảo trì đóng gói phức tạp).

## 3. Bằng chứng
1. Ngân sách tài nguyên (Mục 4.4): Redpanda chạy với cờ `--mode dev-container --smp 1 --memory 512M` chỉ chiếm ổn định ~400-500MB RAM, nằm gọn trong ngân sách 704MB.
2. Công cụ quản sát Web: Redpanda Console (`redpandadata/console:v2.7.2`) chỉ tốn ~50-80MB RAM (ngân sách 192MB), cung cấp giao diện trực quan tại cổng 8080 để theo dõi partitions, consumer lag và xem trực tiếp nội dung message trong topic.

## 4. Quyết định
**Chọn Phương án A (Redpanda)** làm message broker cho toàn bộ dự án:
- Sử dụng image chính thức `redpandadata/redpanda:v24.2.14` kết hợp `redpandadata/console:v2.7.2`.
- Cấu hình dual-listener: Listener nội bộ trong mạng Docker (`redpanda:9092`) cho Spark, listener ngoại vi gắn ra host (`127.0.0.1:19092`) cho Replay Producer.
- Toàn bộ code Python và Spark chỉ sử dụng Kafka client tiêu chuẩn, không phụ thuộc vào thư viện riêng của Redpanda, giữ tính tương thích tuyệt đối.

## 5. Hệ quả
- *Được:*
  - Tiết kiệm hơn 1GB RAM cho máy tính cá nhân so với Kafka JVM.
  - Tốc độ khởi động hạ tầng bằng lệnh `make up` đạt dưới 10 giây.
  - Trải nghiệm phát triển mượt mà với Redpanda Console tại `http://localhost:8080`.
- *Mất / Đánh đổi:*
  - Cần nắm vững cú pháp công cụ `rpk` (Redpanda Keeper) để tạo topic và cấu hình retention (đã được ghi chép trong bài học T01).

## 6. Điều kiện xem xét lại
Quyết định chỉ xem xét lại khi môi trường Production của doanh nghiệp bắt buộc sử dụng dịch vụ Kafka đám mây quản lý sẵn (như Confluent Cloud, AWS MSK).
