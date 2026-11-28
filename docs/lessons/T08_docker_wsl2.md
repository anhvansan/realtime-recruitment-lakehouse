# T08 — Docker, Compose và WSL2 trên Windows

## 1. Câu hỏi gốc
Làm sao để chạy toàn bộ stack dữ liệu lớn (Kafka/Redpanda, Object Storage MinIO, Spark, DuckDB) trên một máy tính cá nhân 16GB RAM chạy hệ điều hành Windows mà không làm treo máy và không làm tràn ổ đĩa hệ thống (ổ C)?

## 2. Trực giác
Hãy tưởng tượng Docker trên Windows giống như bạn thuê một căn phòng chung cư mini (máy ảo WSL2) đặt trong ngôi nhà của bạn. Căn phòng này có một tủ đồ lớn cố định (file đĩa ảo `ext4.vhdx` ở ổ C). Nếu bạn nhét đồ vào tủ, tủ sẽ phình to ra và chiếm hết diện tích phòng ngủ. Ngay cả khi bạn vứt bớt rác ra ngoài, cái tủ vẫn giữ nguyên kích thước khổng lồ đó chứ không tự co lại. Giải pháp là bạn để dành riêng một kho chứa đồ ở ngoài sân (ổ D) và chỉ dẫn đường dây nối (bind mount) từ phòng trọ ra kho đó.

## 3. Cơ chế
Docker Desktop trên Windows sử dụng backend WSL2 (Windows Subsystem for Linux):

```
[Windows Host OS (C: & D:)]
      │
      ├── C:\Users\...\AppData\Local\Docker\wsl\data\ext4.vhdx (Mặc định: Phình to không tự co)
      │
      └── [WSL2 Linux VM Engine]
            │
            ├── Named Volumes ──► Nằm bên trong ext4.vhdx (Nhanh nhưng đầy ổ C)
            │
            └── Bind Mounts   ──► Gắn trực tiếp vào D:\docker_data\... (Giữ an toàn ổ C, I/O qua 9P/virtiofs)
                  ├── Redpanda Data (D:\docker_data\cpc_lakehouse\redpanda)
                  └── MinIO Data    (D:\docker_data\cpc_lakehouse\minio)
```

**Các cơ chế kỷ luật tài nguyên:**
1. **Giới hạn RAM (`mem_limit`):** Ngăn một container (như Spark hay Redpanda) ngốn sạch RAM hệ thống.
2. **Log Rotation (`json-file max-size/max-file`):** Tránh log Docker ghi hàng GB làm chết đĩa.
3. **Profiles:** Chia nhóm container theo chức năng (`core`, `apps`, `tools`) để chỉ bật dịch vụ khi cần.
4. **`.wslconfig`:** Giới hạn tài nguyên cấp VM (RAM, vCPU, `sparseVhd=true`).

## 4. Ví dụ số
Máy có 16GB RAM:
- Windows OS và các ứng dụng văn phòng cần tối thiểu: 4.0GB.
- Ngân sách cấp tối đa cho Docker (WSL2): 8.0GB (`.wslconfig`).
- Ngân sách phân bổ cho các container ở trạng thái ổn định:
  - Redpanda: 704MB
  - Redpanda Console: 192MB
  - MinIO: 448MB
  - Spark (1 JVM driver local[2]): 2304MB
  - Dashboard (Streamlit + DuckDB): 384MB
  - Alert Worker: 96MB
  - **Tổng container core + apps:** ≈ 4.128GB (nằm gọn trong ngân sách 8GB của WSL2).

## 5. Điểm mạnh / Điểm yếu / Khi nào không dùng
- **Điểm mạnh:** Đảm bảo tính tái lập (reproducibility) trên mọi máy tính; cô lập phụ thuộc hoàn toàn; kiểm soát chính xác tài nguyên.
- **Điểm yếu:** File `ext4.vhdx` trên Windows mặc định không tự giải phóng dung lượng sau khi xóa file; I/O bind mount qua phân vùng NTFS có độ trễ cao hơn đĩa Linux gốc.
- **Khi nào không dùng:** Khi triển khai môi trường Production quy mô phân tán nhiều node (sử dụng Kubernetes hoặc Managed Services của Cloud Provider).

## 6. Hiểu lầm phổ biến
1. *Hiểu lầm:* Chạy `docker rm` hoặc xóa dữ liệu bên trong container sẽ tự động giải phóng dung lượng ổ C trên Windows.  
   *Sự thật:* File `.vhdx` của WSL2 chỉ tăng kích thước khi ghi và không tự co lại trừ khi bật cờ `sparseVhd` hoặc chạy lệnh compact đĩa ảo thủ công.
2. *Hiểu lầm:* `localhost` bên trong container trỏ tới máy tính cá nhân của bạn.  
   *Sự thật:* `localhost` trong container chỉ trỏ tới chính container đó; muốn gọi service khác trong cùng mạng Docker phải dùng tên service (ví dụ `http://minio:9000`).

## 7. Trong repo này

| File:dòng | Symbol | Đoạn code | Giải thích dòng nào quan trọng |
| :--- | :--- | :--- | :--- |
| `Makefile:6-16` | `help` | Trích đoạn từ `Makefile`: | Định nghĩa các lệnh vận hành chuẩn hóa cho hạ tầng Docker (`up`, `down`, `clean`, `nuke-data`, `disk-report`). |
| `config/pipeline.yaml:18-26` | `spark` | `driver_memory: "1536m"`, `warehouse_dir: "/tmp/spark-warehouse"` | Cấp heap Spark `1536m` vừa khít trong `mem_limit: 2304m` (chừa overhead JVM/Python) và đặt warehouse ở `/tmp` để tránh lỗi `chmod` trên NTFS bind mount `/app`. |
| `src/streaming/spark_session.py:48-106` | `build_spark_conf` | Cấu hình tập trung từ `AppConfig` | Đọc `spark.*` và `s3a.*` từ config, đảm bảo không hard-code endpoint hay để lộ credentials ra log container. |
| `infra/dashboard/Dockerfile:3-48` | `DUCKDB_EXTENSION_DIRECTORY` | Pre-install `httpfs` & `delta` lúc build image | Cài sẵn extension DuckDB vào `/opt/duckdb_extensions` và chạy bằng `USER appuser` để container khởi động offline không cần tải lại từ Internet. |
| `docker-compose.yml:156-234` | `services.dashboard`, `services.alert-worker` | `mem_limit: 768m` & `384m` trong profile `apps` | Dùng chung một image gọn (`python:3.11-slim`), nối mạng nội bộ `http://minio:9000` (`S3_ENDPOINT_HOST_PORT=minio:9000`), không copy `.env`. |

```makefile
help: ## Show available commands and their descriptions
	@echo "Available commands in CPC AdTech Lakehouse:"
	@echo "  make help        - Show this help message"
	@echo "  make lint        - Run ruff linter check"
	@echo "  make format      - Run ruff code formatter"
	@echo "  make test        - Run unit tests with pytest"
	@echo "  make up          - Start core infrastructure services (Task 1.3)"
	@echo "  make down        - Stop running infrastructure services (Task 1.3)"
	@echo "  make clean       - Clean dangling containers and temporary artifacts (Task 1.5)"
	@echo "  make nuke-data   - Reset all data volumes with confirmation (Task 1.5)"
	@echo "  make disk-report - Report disk usage on Drive C & D (Task 1.5)"
	@echo "  make s3-check    - Run MinIO S3 conformance tests (Task 1.2)"
```

## 8. Tự kiểm tra
1. *Câu hỏi:* Tại sao trong dự án này chúng ta cấu hình `DATA_ROOT=D:/docker_data/...` thay vì để Docker tự tạo named volume mặc định?  
   *Đáp án:* Để đưa toàn bộ dữ liệu ghi đĩa lớn của Kafka và MinIO sang ổ D, bảo vệ ổ C khỏi nguy cơ bị phình đĩa do file `ext4.vhdx`.
2. *Câu hỏi:* Nếu container Spark bị giới hạn `mem_limit: 2304m`, điều gì xảy ra nếu driver Spark cấp phát vượt quá mức này?  
   *Đáp án:* Linux OOM Killer của kernel WSL2 sẽ gửi tín hiệu `SIGKILL` (`exit code 137`) để hủy tiến trình của container ngay lập tức.
3. *Câu hỏi:* Cần thêm cấu hình gì vào Docker Compose để log của các container không ghi tràn đĩa cứng sau vài ngày chạy?  
   *Đáp án:* Cấu hình logging driver `json-file` kèm `options: { max-size: "10m", max-file: "3" }`.

## 9. Đọc thêm
- Docker Compose resource constraints: https://docs.docker.com/compose/compose-file/deploy/#resources
- WSL2 Advanced settings (.wslconfig): https://learn.microsoft.com/en-us/windows/wsl/wsl-config
