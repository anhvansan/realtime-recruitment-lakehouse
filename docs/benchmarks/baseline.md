# Baseline Tài nguyên Hạ tầng (Resource Baseline)

- **Ngày đo:** 2026-11-19
- **Môi trường:** Docker Engine trên Ubuntu 24.04 (WSL2), Windows 11 host
- **Tập dữ liệu đo:** 12 mẫu liên tục chu kỳ 5 giây khi hạ tầng idle ([docs/benchmarks/resource_snapshot.csv](file:///d:/realtime-recruitment-lakehouse/docs/benchmarks/resource_snapshot.csv))

---

## 1. Bảng đối chiếu Ngân sách Tài nguyên (Mục 4.4 PROJECT_BACKLOG.md)

Theo quy tắc **R14 — Báo cáo tài nguyên chuẩn format**, bảng dưới đây so sánh chi tiết mức độ tiêu thụ RAM của từng container so với giới hạn định mức:

| Dịch vụ | Container ID | Profile | Mem Limit (Compose) | RAM Sử dụng Thực tế (Avg) | RAM Sử dụng Tối đa (Peak) | CPU Usage (Avg) | Đánh giá vs Ngân sách |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `minio` | `39da90900fc5` | `core` | 448 MiB | **92.5 MiB** | **96.3 MiB** | ~0.72% | Thấp hơn 355 MiB (-79%) |
| `redpanda` | `e7d8ee04c536` | `core` | 704 MiB | **165.1 MiB** | **165.4 MiB** | ~0.54% | Thấp hơn 538 MiB (-76%) |
| `redpanda-console`| `d13604019bb5` | `core` | 192 MiB | **21.3 MiB** | **21.3 MiB** | 0.00% | Thấp hơn 170 MiB (-89%) |
| `minio-init` | - | `core` | 64 MiB | 0 MiB (Đã dừng) | 0 MiB | 0.00% | Đã hoàn thành nhiệm vụ |
| **TỔNG PROFILE CORE** | - | - | **1.408 MiB (~1.4 GB)**| **~278.8 MiB** | **~283.0 MiB** | **~1.26%** | **Tiết kiệm 80% RAM định mức** |

---

## 2. Ghi chú độ lệch kỹ thuật (Deviations - Quy tắc R14)

1. **Nguyên nhân Redpanda tiêu thụ RAM cực thấp (~165MB so với limit 704MB):**
   - Redpanda được cấu hình với cờ `--memory 512M --reserve-memory 0M --smp 1 --overprovisioned`. Trong trạng thái khởi tạo và chưa có streaming tải cao liên tục, cơ chế Seastar reactor chỉ cấp phát memory pool tối thiểu theo nhu cầu thực tế.
2. **Nguyên nhân MinIO tiêu thụ RAM thấp (~92MB so với limit 448MB):**
   - Image `pgsty/minio` quản lý cache in-memory theo dung lượng object active. Khi idle và chỉ lưu trữ metadata bucket `lakehouse`, mức chiếm dụng RAM rất nhỏ gọn.
3. **Dung lượng đĩa ảo WSL2 an toàn:**
   - Ổ C chỉ giảm < 50MB (chủ yếu là metadata docker shim). Toàn bộ dữ liệu stateful đã nằm trọn vẹn trên ổ D tại `/mnt/d/docker_data/cpc_lakehouse` (17MB).
