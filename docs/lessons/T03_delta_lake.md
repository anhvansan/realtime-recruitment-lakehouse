# T03 — Delta Lake: Thao tác ACID, MERGE và Kiểm thử Liên thông

## 1. Câu hỏi gốc
Làm sao dùng API `DeltaTable` trong PySpark để vừa `append`, vừa `merge` (upsert) an toàn lên MinIO, và làm sao chắc chắn các tính năng nâng cao của Delta (như Deletion Vectors) không làm hỏng engine đọc phía sau (DuckDB)?

## 2. Trực giác
Nếu `F10` giải thích bản thiết kế của cuốn sổ cái `_delta_log`, thì `T03` là bộ dụng cụ thao tác trực tiếp trên sổ cái đó: `append` để ghi thêm dòng mới, `merge` để cập nhật/chèn không trùng khóa, `history()` để soi lịch sử phiên bản, và `DESCRIBE DETAIL` để kiểm tra "đời sổ cái" (`minReaderVersion`, `tableFeatures`) xem các đồng đội khác (DuckDB) có đọc hiểu hay không.

## 3. Cơ chế & Các thao tác cốt lõi
1. **Append & Merge (`DeltaTable.forPath`):**
   - `append`: Tạo file Parquet mới và ghi 1 commit `add` vào `_delta_log`.
   - `merge`: Đọc các file liên quan, so khớp điều kiện join (`t.id = s.id`), tách nhánh `whenMatchedUpdateAll()` / `whenNotMatchedInsertAll()`, viết file kết quả mới và commit nguyên tử (`remove` file cũ + `add` file mới).
2. **Kiểm soát Table Features:**
   - Khi bật `delta.enableDeletionVectors = true`, thao tác `DELETE`/`MERGE` không viết lại toàn bộ file Parquet mà chỉ ghi một file bitmap nhỏ đánh dấu các vị trí dòng đã xóa.
   - Bảng tự động nâng cấp lên `minReaderVersion = 3, minWriterVersion = 7`. Ta dùng `DESCRIBE DETAIL delta.\`path\`` để kiểm tra chính xác các feature đang bật trên bảng.

## 4. Ví dụ số
Trong [tests/integration/smoke_delta_interop.py](file:///d:/realtime-recruitment-lakehouse/tests/integration/smoke_delta_interop.py):
- **Test A (`run_test_a`):** Thực hiện 3 commit (`append` 3 dòng ➔ `append` 2 dòng ➔ `merge` sửa 1 dòng + thêm 1 dòng), xác minh `dt.history()` ghi nhận đủ 3 phiên bản `[0, 1, 2]`.
- **Test D (`run_test_d`):** Tạo bảng có `delta.enableDeletionVectors = true` với 5 dòng (`id` 1..5), gọi `dt.delete("id = 3")` để sinh Deletion Vector, rồi dùng DuckDB `delta_scan()` xác minh đọc ra đúng 4 dòng `[1, 2, 4, 5]`.

## 5. Điểm mạnh / Điểm yếu / Khi nào không dùng
- **Điểm mạnh:** Hỗ trợ đầy đủ DML (`MERGE`, `UPDATE`, `DELETE`) trên S3/MinIO; hỗ trợ kiểu `DECIMAL(18,4)` chính xác tuyệt đối cho dữ liệu tài chính (`spend`, `bid`).
- **Điểm yếu và bẫy:** `MERGE` không có điều kiện partition (`event_date`) sẽ quét toàn bộ bảng; bật table feature quá mới có thể làm gãy các reader phiên bản cũ (luôn phải smoke test như Task 3.1).
- **Khi nào không dùng:** Bảng tạm hoặc dữ liệu nhỏ không cần lịch sử giao dịch.

## 6. Hiểu lầm phổ biến
1. *Hiểu lầm:* Dùng đường dẫn `s3://` trong Spark hay `s3a://` trong DuckDB đều như nhau.  
   *Sự thật:* Hadoop/Spark yêu cầu scheme `s3a://lakehouse/...`, trong khi DuckDB `httpfs`/`delta` yêu cầu scheme `s3://lakehouse/...`.

## 7. Trong repo này

| File:dòng | Symbol | Giải thích |
| :--- | :--- | :--- |
| `tests/integration/smoke_delta_interop.py:121-167` | `run_test_a` | Ghi Delta `append`, `append`, `merge` bằng `DeltaTable.forPath` và kiểm tra `dt.history()`. |
| `tests/integration/smoke_delta_interop.py:264-295` | `run_test_d` | Bật `delta.enableDeletionVectors`, chạy `dt.delete()`, kiểm tra `DESCRIBE DETAIL` và xác minh DuckDB đọc đúng. |

## 8. Tự kiểm tra
1. *Câu hỏi:* Làm thế nào để xem một bảng Delta đang yêu cầu `minReaderVersion` và những `tableFeatures` nào?  
   *Đáp án:* Chạy câu SQL `DESCRIBE DETAIL delta.\`s3a://...\`` trong Spark.
2. *Câu hỏi:* Vì sao trong `run_test_a` ta khai báo tường minh `DecimalType(18, 4)` cho cột `spend` thay vì `DoubleType`?  
   *Đáp án:* Để tránh sai số dấu phẩy động (floating-point rounding) trong tính toán tài chính (ADR-007), và kiểm chứng DuckDB đọc về đúng kiểu `Decimal` chính xác từng chữ số thập phân.
