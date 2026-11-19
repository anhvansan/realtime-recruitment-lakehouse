# T04 — MinIO, Boto3 và Bộ Kiểm chuẩn S3 Conformance

## 1. Câu hỏi gốc
Làm sao để biết chắc chắn một dịch vụ Object Storage cục bộ (MinIO) đáp ứng đầy đủ và chuẩn xác các đặc tả kỹ thuật của AWS S3 API mà Spark, Delta Lake và DuckDB đòi hỏi (nhất quán ghi đè, multipart upload dung lượng lớn, phân trang danh sách object) trước khi khởi chạy các pipeline dữ liệu chính thức?

## 2. Trực giác
Hãy tưởng tượng MinIO giống như một chiếc xe điện được sản xuất theo chuẩn cổng sạc quốc tế (S3 API). Trước khi cắm xe vào các trạm sạc công suất lớn (Spark, DuckDB), bạn dùng một chiếc đồng hồ đo vạn năng thông minh (`boto3` script) để kiểm tra: điện áp có ổn định không, phích cắm có vừa khít không, các tính năng ngắt tự động khi quá nhiệt có hoạt động không. Nếu toàn bộ đèn kiểm tra đều sáng màu xanh (PASS), bạn hoàn toàn yên tâm vận hành trên đường đua.

## 3. Cơ chế
Kịch bản kiểm thử S3 Conformance tương tác trực tiếp với MinIO qua REST API:

```
[scripts/s3_conformance.py] ──► [boto3 S3 Client (s3v4)]
                                        │
                                        ▼ (HTTP Path-Style: http://localhost:9000/lakehouse/...)
                                  [MinIO Server]
                                        ├── Test 1: Bucket Existence (head_bucket / create_bucket)
                                        ├── Test 2: Basic CRUD & Path-Style Addressing
                                        ├── Test 3: ListObjectsV2 Pagination (MaxKeys & ContinuationToken)
                                        ├── Test 4: Multipart Upload >= 32MB (Parallel Part Upload)
                                        ├── Test 5: Overwrite Semantics (Immediate Read-After-Write)
                                        └── Test 6: Concurrent Multi-Threaded Writes (Race Condition Check)
```

**Sự khác biệt cốt lõi giữa MinIO và AWS S3 Cloud:**
1. **Endpoint Resolution:** AWS S3 sử dụng các domain toàn cầu cố định (`s3.us-east-1.amazonaws.com`). MinIO cục bộ yêu cầu khai báo rõ cổng (ví dụ `http://127.0.0.1:9000` hoặc `http://minio:9000`).
2. **Cấu hình Secret cho DuckDB:** DuckDB hỗ trợ S3 nhưng cú pháp `CREATE SECRET` của DuckDB đòi hỏi trường `ENDPOINT` chỉ nhận `host:port` (cắt bỏ `http://`).
3. **Container Khởi tạo (`minio-init`):** Sử dụng image `minio/mc` để tạo sẵn bucket `lakehouse` ngay khi MinIO vừa khởi động xong, giúp các ứng dụng khác không bị lỗi "Bucket not found".

## 4. Ví dụ số
- Bài test Multipart Upload:
  - Payload tổng: **33,554,432 bytes (32MB)**.
  - Chia làm 4 phần, mỗi phần **8,388,608 bytes (8MB)**.
  - Thời gian tải lên hoàn tất: ~0.4–0.8 giây ➔ Thông lượng ghi đạt xấp xỉ **40–80 MB/s** ngay trên máy cá nhân.

## 5. Điểm mạnh / Điểm yếu / Khi nào không dùng
- **Điểm mạnh:** Kiểm chứng độc lập, không phụ thuộc vào Spark; phát hiện ngay các lỗi mạng, quyền truy cập hoặc cấu hình sai endpoint; chạy cực nhanh (< 2 giây).
- **Điểm yếu:** Chỉ kiểm tra ở tầng API lưu trữ S3, chưa kiểm tra tính tương thích của table format (như Delta transaction log - sẽ kiểm tra ở Task 3.1).
- **Khi nào không dùng:** Khi kiểm tra tải cực đại phân tán hàng chục node (cần dùng các bộ benchmark chuyên dụng như Warp).

## 6. Hiểu lầm phổ biến
1. *Hiểu lầm:* Chỉ cần container MinIO đang chạy (status `Up`) là bucket đã sẵn sàng để ghi.  
   *Sự thật:* MinIO khởi động nhưng chưa chắc bucket đã được tạo. Đó là lý do container `minio-init` đóng vai trò quan trọng để đảm bảo bucket `lakehouse` luôn tồn tại trước khi các service khác bắt đầu.
2. *Hiểu lầm:* Multipart upload có thể chia thành các phần nhỏ tùy ý (như 1KB).  
   *Sự thật:* Chuẩn S3 quy định kích thước tối thiểu cho mỗi part (trừ part cuối cùng) là **5MB**. Nếu chia part nhỏ hơn 5MB, MinIO/S3 sẽ trả về lỗi `EntityTooSmall`.

## 7. Trong repo này

| File:dòng | Symbol | Đoạn code | Giải thích |
| :--- | :--- | :--- | :--- |
| `scripts/s3_conformance.py:161-175` | `test_multipart_upload_32mb` | Trích đoạn Multipart Upload: | Chia 32MB payload thành 4 parts 8MB, tải lên song song và hoàn tất multipart upload chuẩn S3. |

```python
            mpu = self.client.create_multipart_upload(Bucket=self.bucket, Key=key)
            upload_id = mpu["UploadId"]
            parts = []

            for part_num in range(1, num_parts + 1):
                chunk = os.urandom(part_size)
                res = self.client.upload_part(
                    Bucket=self.bucket,
                    Key=key,
                    UploadId=upload_id,
                    PartNumber=part_num,
                    Body=chunk,
                )
                parts.append({"PartNumber": part_num, "ETag": res["ETag"]})
```

## 8. Tự kiểm tra
1. *Câu hỏi:* Tại sao bộ test kiểm tra phân trang `ListObjectsV2` lại sử dụng tham số `MaxKeys=5`?  
   *Đáp án:* Để ép S3 API phải cắt nhỏ danh sách 15 object thành nhiều trang trả về, kiểm chứng rằng cơ chế `ContinuationToken` hoạt động chuẩn xác.
2. *Câu hỏi:* Bài kiểm tra "Overwrite Semantics" nhằm bảo vệ điều gì trong Lakehouse?  
   *Đáp án:* Bảo đảm rằng khi một tệp Delta metadata hoặc checkpoint được ghi đè, mọi truy vấn đọc ngay sau đó sẽ thấy ngay phiên bản mới nhất (Strong Consistency), không bị đọc phải phiên bản cũ lưu trong cache.
3. *Câu hỏi:* Lệnh nào trong `Makefile` được cấu hình để chạy toàn bộ bài kiểm tra này?  
   *Đáp án:* `make s3-check`.

## 9. Đọc thêm
- Boto3 S3 Client Documentation: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html
- MinIO Multipart Upload API: https://min.io/docs/minio/linux/developers/python/API.html
