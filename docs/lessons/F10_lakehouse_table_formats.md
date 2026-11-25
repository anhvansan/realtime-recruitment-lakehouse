# F10 — Lakehouse Table Formats & Nhật ký giao dịch (`_delta_log`)

## 1. Câu hỏi gốc
Parquet lưu dữ liệu cột rất gọn và nhanh, nhưng tại sao chỉ lưu file Parquet thuần trên S3/MinIO lại **không đủ** để xây một Lakehouse cho phép vừa ghi streaming (append/merge) vừa đọc báo cáo đồng thời?

## 2. Trực giác
Hãy hình dung một cuốn sổ cái kế toán (S3/MinIO). Nếu bạn chỉ ném từng tờ giấy rời (file Parquet) vào tủ, người đọc mở tủ đúng lúc bạn đang thay thế một tờ giấy cũ bằng tờ mới sẽ gặp thảm họa: hoặc đọc thiếu, hoặc đọc trùng, hoặc lỗi `FileNotFound`. **Table Format** (như Delta Lake, Apache Iceberg) đặt thêm một **cuốn mục lục có đánh số phiên bản** (`_delta_log/`) ở đầu tủ: mọi thay đổi đều ghi giấy mới xong xuôi rồi mới ký một dòng mục lục mới một cách nguyên tử (atomic). Người đọc chỉ nhìn vào phiên bản mục lục mới nhất đã ký, nên không bao giờ thấy trạng thái nửa vời.

## 3. Cơ chế hoạt động của `_delta_log`

```
s3://lakehouse/_smoke/t1/
├── _delta_log/
│   ├── 00000000000000000000.json   ◄── Commit 0: protocol + metaData + add(part-000.parquet)
│   ├── 00000000000000000001.json   ◄── Commit 1: add(part-001.parquet)
│   ├── 00000000000000000002.json   ◄── Commit 2 (MERGE): remove(part-000) + add(part-002)
│   └── 00000000000000000010.checkpoint.parquet  ◄── Gộp trạng thái mỗi 10 commit
├── part-000.parquet  (còn trên đĩa cho Time Travel / reader cũ, chưa bị VACUUM)
├── part-001.parquet
└── part-002.parquet
```

1. **Optimistic Concurrency Control (OCC):** Writer ghi các file Parquet mới trước, sau đó tạo file `NNNNNN.json` kế tiếp trong `_delta_log/`. Nếu hai writer cùng tranh số phiên bản, một bên sẽ nhận lỗi xung đột và kiểm tra xem có retry an toàn được không.
2. **Snapshot Isolation (Đọc khi đang ghi):** Reader (DuckDB `delta_scan`) đọc các file log đến phiên bản $V$, dựng danh sách file Parquet hợp lệ tại thời điểm $V$ rồi quét đúng danh sách đó — hoàn toàn miễn nhiễm với các commit $V+1, V+2$ đang ghi song song.
3. **Table Features & Protocol Version:** Các tính năng mới như **Deletion Vectors** (đánh dấu dòng bị xóa bằng bitmap thay vì viết lại cả file Parquet) yêu cầu nâng `minReaderVersion=3, minWriterVersion=7`. Reader phải hỗ trợ Delta Kernel tương ứng mới đọc được.

## 4. Ví dụ số
Trong bài smoke test Task 3.1 (`tests/integration/smoke_delta_interop.py`):
- Commit 0 (`append`): 3 dòng, tổng `spend = 6.7500`.
- Commit 1 (`append`): +2 dòng, tổng 5 dòng.
- Commit 2 (`merge`): cập nhật `id=2` (+0.5000) và chèn `id=6` (+6.0000) ➔ tổng 6 dòng, `SUM(spend) = 23.0000`.
- Vòng lặp 20 commit (mỗi commit 5 dòng) chạy đồng thời với DuckDB đọc liên tục: mọi lần đọc của DuckDB chỉ thấy đúng bội số $6 + 5 \times k$ dòng (từ 6 đến 106), **0 lần đọc nửa vời**.

## 5. Điểm mạnh / Điểm yếu / Khi nào không dùng
- **Điểm mạnh:** ACID trên Object Storage giá rẻ; Time Travel; Idempotent writes (`txnAppId`/`txnVersion`); nhiều engine đọc chung không sao chép (Zero-copy).
- **Điểm yếu:** Sinh nhiều file nhỏ khi ghi micro-batch (cần `OPTIMIZE`); chi phí quét log nếu không có checkpoint.
- **Khi nào không dùng:** Dữ liệu tĩnh chỉ ghi 1 lần không cập nhật ➔ Parquet thuần là đủ; giao dịch OLTP mili-giây ➔ PostgreSQL.

## 6. Hiểu lầm phổ biến
1. *Hiểu lầm:* Khi `MERGE` hoặc `DELETE` xong, file Parquet cũ biến mất ngay khỏi MinIO.  
   *Sự thật:* Delta chỉ ghi action `remove` vào `_delta_log`; file vật lý vẫn nằm đó để phục vụ Time Travel và các reader đang đọc snapshot cũ cho đến khi chạy `VACUUM`.
2. *Hiểu lầm:* Chạy `VACUUM(0)` giúp tiết kiệm đĩa ngay mà không có hại gì.  
   *Sự thật:* `VACUUM(0)` xóa ngay file của snapshot trước đó, làm crash ngay lập tức mọi truy vấn DuckDB đang đọc dở (`FileNotFound`).

## 7. Trong repo này

| File:dòng | Symbol | Giải thích |
| :--- | :--- | :--- |
| `tests/integration/smoke_delta_interop.py:121-167` | `run_test_a` | Ghi 3 commit (`append`, `append`, `merge`) và xác minh 3 phiên bản trong `_delta_log`. |
| `tests/integration/smoke_delta_interop.py:193-261` | `run_test_c` | Kiểm chứng Snapshot Isolation: 20 commit Spark song song với DuckDB đọc liên tục. |
| `tests/integration/smoke_delta_interop.py:264-295` | `run_test_d` | Kiểm tra Table Features (`delta.enableDeletionVectors = true`, Protocol v3/v7). |
| `docs/adr/003-delta.md` | ADR-003 | So sánh Delta Lake với Apache Iceberg và Apache Hudi. |

## 8. Tự kiểm tra
1. *Câu hỏi:* Vì sao trong Test C, DuckDB không bao giờ đọc ra 8 dòng hay 13 dòng khi mỗi commit Spark ghi 5 dòng (bắt đầu từ 6 dòng)?  
   *Đáp án:* Nhờ cơ chế Snapshot Isolation của `_delta_log`: các file Parquet mới chỉ trở nên hữu hình với reader sau khi file JSON commit tương ứng được ghi nguyên tử.
2. *Câu hỏi:* File `00000000000000000010.checkpoint.parquet` dùng để làm gì?  
   *Đáp án:* Gộp toàn bộ trạng thái `add`/`remove` của 10 commit trước đó vào một file Parquet duy nhất để reader không phải đọc lại hàng nghìn file JSON từ commit 0.
