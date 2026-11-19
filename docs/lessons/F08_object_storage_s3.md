# F08 — Lưu trữ Đối tượng S3 (Object Storage) và Nền tảng Zero-Copy Lakehouse

## 1. Câu hỏi gốc
Tại sao các hệ thống dữ liệu hiện đại (Lakehouse) lại từ bỏ hệ thống tệp truyền thống (HDFS, POSIX File System) để chuyển sang lưu trữ trên Object Storage (như AWS S3, MinIO, GCS), và kiến trúc này giải quyết bài toán tách biệt điện toán khỏi lưu trữ (Decoupling Compute & Storage) như thế nào?

## 2. Trực giác
Hãy so sánh:
- **Hệ thống tệp truyền thống (POSIX/HDFS):** Giống như phòng lưu trữ hồ sơ giấy với vô số ngăn kéo, cặp đựng và thư mục lồng nhau. Mỗi khi muốn thêm giấy hay đổi tên ngăn kéo, bạn phải khóa cả phòng lại (lock metadata), và tủ hồ sơ phải được gắn chết vào bàn làm việc của bạn (Compute gắn liền với Storage).
- **Object Storage (S3):** Giống như dịch vụ ký gửi đồ thông minh ở siêu thị. Bạn không quan tâm đồ nằm ở tầng mấy; bạn chỉ đưa món đồ vào, nhận lại một chiếc mã vạch (Object Key). Không gian chứa là vô hạn, và bất kỳ ai có chìa khóa đều có thể đến lấy món đồ ra mà không làm ảnh hưởng đến người bên cạnh.

## 3. Cơ chế
Object Storage không tổ chức theo cây thư mục vật lý thật mà là một cấu trúc phẳng (Flat Namespace):

```
Bucket: lakehouse
  Key: bronze/cpc_events/event_date=2022-07-25/part-001.parquet ──► [Metadata + Immutable Bytes]
  Key: silver/cpc_events/_delta_log/000000.json                  ──► [Delta Log Entry]
```

**Các đặc tính cốt lõi của S3 API:**
1. **Tính bất biến của Object (Immutable):** Một khi đã ghi một file Parquet, bạn không thể mở file ra để sửa 1 dòng bên trong (như sửa file văn bản). Bạn chỉ có thể ghi đè toàn bộ (overwrite) hoặc ghi file mới. Đây chính là lý do Delta Lake ra đời (sử dụng commit log ghi thêm `append-only` để quản lý các phiên bản dữ liệu).
2. **Path-Style vs Virtual-Hosted Style Addressing:**
   - *Path-Style:* `http://minio:9000/lakehouse/silver/...` (Phù hợp tuyệt đối cho môi trường local, IP nội bộ).
   - *Virtual-Hosted Style:* `http://lakehouse.s3.amazonaws.com/...` (Chuẩn mặc định của AWS S3 trên Cloud).
3. **Multipart Upload (Tải lên nhiều phần):** Cho phép chia các file lớn (≥ 5MB, trong bài test là 32MB) thành nhiều phần để tải lên song song, giúp tối ưu băng thông và cho phép truyền lại chỉ phần bị lỗi nếu rớt mạng.

## 4. Ví dụ số
- Ghi 1 tệp Delta Parquet dung lượng 32MB vào bucket `lakehouse`:
  - Thay vì gửi 1 khối liên tục dễ bị nghẽn mạng, client chia thành 4 phần (parts), mỗi phần 8MB (`8 * 1024 * 1024` bytes).
  - 4 phần được gửi song song và MinIO ghép lại thành một object duy nhất khi hoàn tất (`CompleteMultipartUpload`).
  - Thao tác `HEAD` kiểm tra kích thước object xác nhận chính xác `33,554,432` bytes.

## 5. Điểm mạnh / Điểm yếu / Khi nào không dùng
- **Điểm mạnh:** Chi phí lưu trữ rẻ gấp 5–10 lần ổ SSD block; khả năng mở rộng không giới hạn; tách rời hoàn toàn điện toán (Spark/DuckDB có thể tắt đi khi không dùng mà dữ liệu vẫn nguyên vẹn).
- **Điểm yếu:** Độ trễ đọc ghi ban đầu (TTFB) cao hơn ổ đĩa cục bộ (10–50ms so với micro-giây); thao tác `LIST` thư mục lớn tốn thời gian.
- **Khi nào không dùng:** Ổ đĩa hệ điều hành cho máy chủ, hoặc cơ sở dữ liệu giao dịch ghi liên tục ngẫu nhiên (Random Write IOPS cao).

## 6. Hiểu lầm phổ biến
1. *Hiểu lầm:* Thao tác đổi tên thư mục trong S3 (`RENAME dir`) diễn ra tức thì như trên Windows/Linux.  
   *Sự thật:* S3 không có thư mục thật! Đổi tên một thư mục chứa 10,000 file thực chất là S3 phải chạy 10,000 thao tác `COPY` từng file sang key mới rồi `DELETE` key cũ, cực kỳ chậm và tốn kém. Delta Lake giải quyết vấn đề này bằng cách không bao giờ rename thư mục dữ liệu mà chỉ ghi commit log mới.
2. *Hiểu lầm:* Có thể nối tiếp (append) thêm dữ liệu vào đuôi một file đang có sẵn trên S3.  
   *Sự thật:* Object Storage không hỗ trợ append stream; muốn thêm dữ liệu phải tạo file Parquet mới.

## 7. Trong repo này

| File:dòng | Symbol | Đoạn code | Giải thích |
| :--- | :--- | :--- | :--- |
| `scripts/s3_conformance.py:32-41` | `get_s3_client` | Trích cấu hình client S3: | Bắt buộc thiết lập `addressing_style: path` để tương thích hoàn toàn với MinIO cục bộ. |

```python
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
        config=Config(
            s3={"addressing_style": "path"},
            signature_version="s3v4",
            retries={"max_attempts": 3, "mode": "standard"},
        ),
    )
```

## 8. Tự kiểm tra
1. *Câu hỏi:* Tại sao trong môi trường MinIO cục bộ, client bắt buộc phải cấu hình `addressing_style: 'path'`?  
   *Đáp án:* Vì nếu dùng `virtual-hosted`, client sẽ cố phân giải domain dạng `http://lakehouse.minio:9000`, gây lỗi DNS không tìm thấy domain nếu không cấu hình DNS wildcard nội bộ.
2. *Câu hỏi:* Kiến trúc Zero-Copy Lakehouse tận dụng đặc tính nào của Object Storage?  
   *Đáp án:* Dữ liệu chỉ được lưu một lần duy nhất trên S3/MinIO dưới định dạng chuẩn mở (Delta/Parquet); cả Spark, DuckDB và dbt cùng đọc chung một vị trí mà không cần sao chép dữ liệu sang hệ thống lưu trữ khác.
3. *Câu hỏi:* Thao tác ghi đè (Overwrite) trên MinIO/S3 có đảm bảo tính nhất quán đọc ngay lập tức không?  
   *Đáp án:* Có, chuẩn S3 hiện đại đảm bảo tính nhất quán cao (Strong Read-After-Write Consistency) cho mọi thao tác PUT và DELETE.

## 9. Đọc thêm
- AWS S3 Strong Consistency Announcement: https://aws.amazon.com/blogs/aws/amazon-s3-update-strong-read-after-write-consistency/
- MinIO Architecture Overview: https://min.io/product/architecture
