# ADR-008 — Kỷ luật ổ đĩa WSL2 và Chiến lược lưu trữ dữ liệu State

- **Trạng thái:** Accepted
- **Ngày:** 2026-11-19
- **Tác giả:** Chủ dự án & AI Assistant
- **Liên quan:** Task 1.5, R5 (Không phình ổ C), R6 (Dọn dẹp sạch sẽ)

---

## 1. Bối cảnh

Môi trường phát triển dự án chạy **Docker Engine trực tiếp trong Ubuntu WSL2** (không dùng Docker Desktop). Trong kiến trúc này:
1. **Rủi ro phình ổ C:** Ổ đĩa hệ điều hành Windows (C:) chứa file đĩa ảo của WSL2 (`ext4.vhdx` tại `%LOCALAPPDATA%\Packages\...\LocalState\ext4.vhdx`). Khi container ghi log hoặc dữ liệu vào filesystem nội bộ (`/var/lib/...` hoặc named volume mặc định), file `ext4.vhdx` sẽ phình to. Do cơ chế cấp phát sparse disk của Hyper-V/WSL2, file này **chỉ mở rộng chứ không tự thu hồi dung lượng** khi dữ liệu bên trong bị xoá nếu chưa kích hoạt tính năng `sparseVhd`.
2. **Đặc thù khối lượng dữ liệu Lakehouse:** CPC Realtime Lakehouse tạo ra lưu lượng liên tục gồm:
   - Redpanda message logs & commit logs.
   - MinIO S3 object storage (Bronze, Silver, Delta Lake Parquet files, Checkpoint metadata).
   - Cache trung gian của Spark Structured Streaming và DuckDB extensions.
3. **Phân vùng phần cứng:** Máy chủ phát triển có ổ D là SSD dung lượng lớn, mã nguồn đặt tại `D:\realtime-recruitment-lakehouse` (ánh xạ trong WSL2 là `/mnt/d/realtime-recruitment-lakehouse`).

---

## 2. Các phương án

### Phương án A: Bind mount toàn bộ dữ liệu có trạng thái sang ổ D (`/mnt/d/docker_data/cpc_lakehouse`)
- Toàn bộ service lưu trữ (MinIO, Redpanda) bind mount volume trực tiếp ra đường dẫn `${DATA_ROOT}` trên ổ D.
- *Ưu điểm:*
  - Dữ liệu tách rời hoàn toàn khỏi đĩa ảo WSL2, bảo vệ 100% dung lượng ổ C không bị phình.
  - Dễ dàng quan sát trực tiếp cấu trúc file (Parquet, metadata, logs) từ cả Windows Explorer và WSL terminal.
  - Quản trị vòng đời dữ liệu cực kỳ dễ dàng: có thể sao lưu, di chuyển hoặc xoá sạch (`make nuke-data`) mà không cần đụng tới Docker daemon hay VHDX.
- *Nhược điểm:*
  - Tốc độ truy cập file từ Linux container sang phân vùng Windows NTFS (`/mnt/d` qua 9P/drvfs) có độ trễ I/O cao hơn native ext4.

### Phương án B: Di chuyển toàn bộ Distro WSL2 sang ổ D (dùng `wsl --export` và `wsl --import`)
- Xuất bản phân phối Ubuntu và nạp lại hoàn toàn trên phân vùng ổ D, sử dụng native Docker named volumes bên trong ext4.
- *Ưu điểm:*
  - Tận dụng tối đa tốc độ I/O nguyên bản của Linux ext4.
- *Nhược điểm:*
  - Phức tạp trong thao tác thiết lập ban đầu của người dùng, dễ gây gián đoạn môi trường đang chạy.
  - Khó debug hoặc trực tiếp xem các file Parquet/Delta bằng công cụ Windows nếu không mount ngược lại.

---

## 3. Bằng chứng thực nghiệm (Empirical Evidence)

1. **Hiệu năng I/O thực tế của Phương án A (MinIO S3 Conformance trên ổ D):**
   - Kết quả đo tại Task 1.2: Multipart upload file 32MB vào bucket `lakehouse` trên ổ D đạt throughput **~34.7 MB/s**.
   - Thao tác ghi đồng thời 10 luồng (concurrent writes) hoàn thành trong **2.531s**.
   - Đọc/ghi cơ bản (CRUD) phản hồi tức thì **< 100ms**.
   - Với lưu lượng CPC mô phỏng của hệ thống (~50–200 events/giây, mỗi micro-batch Spark ghi ~ vài trăm KB Parquet), throughput 34.7 MB/s đáp ứng dư dả nhu cầu và không hề trở thành điểm nghẽn.
2. **Kỷ luật ổ C:**
   - Khi chạy toàn bộ container profile `core` và thực thi bộ test S3 Conformance, dung lượng trống ổ C không suy giảm (mọi byte object đều nằm dưới `${DATA_ROOT}/minio`).

---

## 4. Quyết định

**Chấp thuận Phương án A:**
1. Khai báo biến `DATA_ROOT=/mnt/d/docker_data/cpc_lakehouse` trong `.env` và bind mount toàn bộ volume của `minio` và `redpanda` vào đây.
2. Giới hạn kích thước log của Docker daemon và Docker Compose bằng driver `json-file` (`max-size: 10m`, `max-file: 3`).
3. Khuyến nghị người dùng cấu hình `%USERPROFILE%\.wslconfig` để bật tính năng `sparseVhd=true` (tự động giải phóng dung lượng đĩa ảo WSL2 khi xoá file) và giới hạn RAM của WSL2 để không làm nghẽn máy host.
4. Cung cấp các lệnh `make disk-report` (theo dõi dung lượng C, D, Docker), `make clean` (dọn cache an toàn), và `make nuke-data` (xoá trắng dữ liệu ổ D có cơ chế gõ xác nhận `"NUKE"`).

---

## 5. Hệ quả

- **Được:**
  - Ổ C được bảo vệ tuyệt đối khỏi nguy cơ tràn đĩa; tuân thủ quy tắc R5 và R6.
  - Phù hợp hoàn hảo với kiến trúc Docker Engine chạy trực tiếp trong Ubuntu WSL2.
  - Người dùng có thể kiểm tra trực tiếp các file vật lý của MinIO và Redpanda trên ổ D.
- **Đánh đổi:**
  - Chấp nhận overhead truy xuất qua filesystem driver `/mnt/d/` (đã chứng minh không ảnh hưởng tới tải của bài toán).

---

## 6. Hướng dẫn cấu hình `.wslconfig` chuẩn cho Windows Host

Tạo hoặc cập nhật file `C:\Users\<Tên_User>\.wslconfig` trên Windows:

```ini
[wsl2]
# Giới hạn RAM cấp cho toàn bộ máy ảo WSL2 (ví dụ 6GB trên máy 16GB RAM)
memory=6GB
# Giới hạn số CPU cores (ví dụ 4 cores)
processors=4
# Tự động giải phóng dung lượng đĩa ảo ext4.vhdx khi xoá dữ liệu bên trong Linux
sparseVhd=true

[experimental]
autoMemoryReclaim=gradual
```

---

## 7. Điều kiện xem xét lại

Quyết định này sẽ được đánh giá lại nếu:
- Tải streaming benchmark (Task 6.4) vượt ngưỡng 20.000 events/giây và độ trễ I/O wait trên ổ D vượt quá 30% tổng thời gian batch của Spark.
