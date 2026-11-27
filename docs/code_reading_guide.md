# Prompt: Tạo Lộ Trình Đọc Code Song Song Với `docs/lessons/`

Yêu cầu của tôi:
"giờ muốn đọc file code đồng thời với các docs lessions thì nên đọc theo trình tự nào. tại tôi muốn hiểu cái flow của từng file, từng module chẳng hạn, kiểu trong mỗi cái epic là input của nó là gì, đi qua những file/module xử lý nào (file đó trong Dependency Graph hay sequence diagram có cấp bậc/vai trò gì), cho ra output gì"

## 1. Nguyên tắc phân tầng (Dependency Layers)

Luôn phân loại các file code theo **3 tầng phụ thuộc từ dưới lên trên** để tôi dễ nắm cấu trúc (tương tự tư duy SOLID/GRASP):

- **Tầng 0 (Nền & Cấu hình):** Hạ tầng Docker, cấu hình dùng chung, factory khởi tạo (các module khác import từ đây).
- **Tầng 1 (Luật thuần — Pure Logic):** Các hàm/class xử lý nghiệp vụ thuần túy, không phụ thuộc I/O nặng, có thể hiểu và test độc lập.
- **Tầng 2 (Điều phối — Orchestrator/Runner):** File entrypoint ghép Tầng 0 + Tầng 1 lại để đọc/ghi dữ liệu thật.

## 2. Cấu trúc bắt buộc cho mỗi Epic

Với mỗi Epic trong phạm vi yêu cầu, trình bày chính xác theo 4 phần sau:

1. **Input & Output của cả Epic:**
   - **Input:** Dữ liệu/sự kiện/bảng đầu vào là gì, nằm ở đâu.
   - **Output:** Sản phẩm đầu ra là gì (bảng Delta, Parquet, topic, checkpoint, file manifest, UI panel...).

2. **Sơ đồ Luồng & Phụ thuộc (ASCII Dependency/Flow Diagram):**
   - Vẽ một khối `text` nhỏ gọn thể hiện rõ luồng dữ liệu đi qua file nào (ghi rõ Tầng 0 / Tầng 1 / Tầng 2), file nào `import` file nào, và kết nối tới test/hạ tầng nào.

3. **Trình tự đọc (Lý thuyết ➔ Code):**
   - Đánh số thứ tự 1, 2, 3... theo chiều **từ dưới lên trên (Tầng 0 ➔ Tầng 1 ➔ Tầng 2)**.
   - Mỗi bước phải ghép cặp trực tiếp: **Đọc bài học `docs/lessons/Fxx` / `Txx` (và `docs/adr/` nếu có) ➔ Mở file code thật** (dùng link Markdown có thể click được trong IDE).
   - Mở ngoặc ghi chú ngắn gọn **cần tập trung nhìn vào hàm/class/đoạn nào** trong file đó (tránh đọc lan man).

4. **Mẹo đọc nhanh không bị rối:**
   - Kết thúc bằng 2–3 gạch đầu dòng chỉ cách đọc lướt file Tầng 2 (ví dụ: đọc từ `main()` trước rồi mới nhảy ngược lên hàm con) và cách tra cứu nhanh qua `docs/code_map.md`.

## 3. Ràng buộc

- **Chỉ đưa vào các file code và bài học đã thực sự tồn tại trong repo** (không bịa đường dẫn của các task chưa làm).
- Văn phong súc tích, thực chiến, giữ nguyên thuật ngữ kỹ thuật tiếng Anh, giải thích bằng tiếng Việt.

## Ví dụ Kết quả

````
Cách dễ hiểu nhất để đọc dự án này là **đọc từ dưới lên trên theo 3 tầng phụ thuộc (Dependency Layers)** của từng Epic:
- **Tầng 0 (Nền & Cấu hình):** Hạ tầng Docker + file config dùng chung (mọi file khác đều import từ đây).
- **Tầng 1 (Luật thuần — Pure Logic):** Các hàm xử lý thuần túy (không cần bật Kafka/Spark vẫn hiểu và test được).
- **Tầng 2 (Điều phối — Orchestrator/Runner):** File chính ghép Tầng 0 + Tầng 1 lại để đọc/ghi dữ liệu thật.

Bạn có thể mở [code_map.md](file:///d:/realtime-recruitment-lakehouse/docs/code_map.md) bên cạnh như **từ điển tra cứu nhanh**, rồi đọc theo đúng trình tự 3 Epic dưới đây:

---

## 🚀 EPIC 1 — Hạ tầng & Cấu hình dùng chung (Nền móng cho toàn bộ dự án)

- **Input:** Biến môi trường `.env` + cấu hình tĩnh `config/pipeline.yaml` + tài nguyên máy 16GB RAM.
- **Output:** Mạng Docker gồm **MinIO** (bucket `lakehouse` lưu ổ D), **Redpanda** (Kafka broker), và hàm `load()` cung cấp cấu hình cho mọi module Python.

```text
[.env] + [config/pipeline.yaml] ──► src/common/config.py (Tầng 0: mọi file khác đều import)
                                           │
docker-compose.yml + Makefile ─────────────┴──► Hạ tầng MinIO + Redpanda (ổ D)
                                                    └── Kiểm chứng bởi: scripts/s3_conformance.py
```

**Trình tự đọc (Lý thuyết ➔ Code):**

1. Đọc [T08_docker_wsl2.md](file:///d:/realtime-recruitment-lakehouse/docs/lessons/T08_docker_wsl2.md) + [F08_object_storage_s3.md](file:///d:/realtime-recruitment-lakehouse/docs/lessons/F08_object_storage_s3.md) ➔ Mở [docker-compose.yml](file:///d:/realtime-recruitment-lakehouse/docker-compose.yml) và [Makefile](file:///d:/realtime-recruitment-lakehouse/Makefile) (xem cách chia profile `core`/`apps`, giới hạn `mem_limit`, mount ổ D).
2. Đọc [T09_python_tooling.md](file:///d:/realtime-recruitment-lakehouse/docs/lessons/T09_python_tooling.md) ➔ Mở [pipeline.yaml](file:///d:/realtime-recruitment-lakehouse/config/pipeline.yaml) và [config.py](file:///d:/realtime-recruitment-lakehouse/src/common/config.py) (chú ý `s3a_uri` cho Spark vs `s3_uri` + `s3_endpoint_host_port` cho DuckDB).
3. _(Tùy chọn)_ Đọc [T04_minio_s3_boto3.md](file:///d:/realtime-recruitment-lakehouse/docs/lessons/T04_minio_s3_boto3.md) ➔ Xem lướt [s3_conformance.py](file:///d:/realtime-recruitment-lakehouse/scripts/s3_conformance.py) để hiểu cách kiểm tra S3 API.

---

## 📨 EPIC 2 — Hợp đồng dữ liệu & Giả lập luồng sự kiện (Producer)

- **Input:** 17 file CSV gốc trong `data/raw/` (chủ yếu `tracking.csv` + `application.csv`).
- **Output:**
  1. Dòng tin nhắn JSON (`cpc_event.v1`) đẩy vào topic Redpanda **`cpc_events_raw`** (có trộn 2% trùng, 2% trễ, 1% rác).
  2. Tờ đáp án **`data/manifests/replay_<run_id>.json`** + `.late_ids.jsonl` để đối soát.

```text
data/raw/*.csv
   │
   ├─► (Khảo sát 1 lần) scripts/profile_tracking.py ──► docs/data_dictionary.md
   │
   └─► src/producer/replay_producer.py  (Tầng 2: Orchestrator chạy luồng phát)
          ├── import src/common/config.py              (Tầng 0: lấy broker/topic)
          ├── import src/common/schemas/cpc_event.py   (Tầng 1: chuẩn hóa schema v1)
          ├── import src/producer/time_mapper.py       (Tầng 1: tua nhanh thời gian)
          └── import src/producer/fault_injector.py    (Tầng 1: bơm lỗi + đếm manifest)
                 │
                 ▼
       Redpanda topic: cpc_events_raw (tạo bởi infra/redpanda/create_topics.sh)
```

**Trình tự đọc (Lý thuyết ➔ Code):**

1. **Hiểu dữ liệu & luật:** Đọc [F12_data_modeling_medallion.md](file:///d:/realtime-recruitment-lakehouse/docs/lessons/F12_data_modeling_medallion.md) + [F13_data_quality_contracts.md](file:///d:/realtime-recruitment-lakehouse/docs/lessons/F13_data_quality_contracts.md) ➔ Mở [validation_rules.md](file:///d:/realtime-recruitment-lakehouse/src/common/schemas/validation_rules.md) và [cpc_event.py](file:///d:/realtime-recruitment-lakehouse/src/common/schemas/cpc_event.py) _(Tầng 1 — định nghĩa 7 mã lỗi & `CpcEventV1`)_.
2. **Hiểu Kafka/Redpanda:** Đọc [F07_kafka_streaming_internals.md](file:///d:/realtime-recruitment-lakehouse/docs/lessons/F07_kafka_streaming_internals.md) + [T01_redpanda_rpk.md](file:///d:/realtime-recruitment-lakehouse/docs/lessons/T01_redpanda_rpk.md) ➔ Mở [create_topics.sh](file:///d:/realtime-recruitment-lakehouse/infra/redpanda/create_topics.sh) _(cách chia 3 partition và đặt retention 1h)_.
3. **Hiểu 2 module thuần của Producer:**
   - Đọc [F01_batch_and_streaming.md](file:///d:/realtime-recruitment-lakehouse/docs/lessons/F01_batch_and_streaming.md) + [F02_three_notions_of_time.md](file:///d:/realtime-recruitment-lakehouse/docs/lessons/F02_three_notions_of_time.md) ➔ Mở [time_mapper.py](file:///d:/realtime-recruitment-lakehouse/src/producer/time_mapper.py) _(Tầng 1 — ánh xạ giờ gốc sang giờ mô phỏng)_.
   - Đọc [F04_watermark_and_late_data.md](file:///d:/realtime-recruitment-lakehouse/docs/lessons/F04_watermark_and_late_data.md) + [F06_delivery_semantics_idempotency.md](file:///d:/realtime-recruitment-lakehouse/docs/lessons/F06_delivery_semantics_idempotency.md) ➔ Mở [fault_injector.py](file:///d:/realtime-recruitment-lakehouse/src/producer/fault_injector.py) _(Tầng 1 — cách bơm 2% trùng / 2% trễ / 1% rác)_.
4. **Đọc file ghép tất cả lại:** Mở [replay_producer.py](file:///d:/realtime-recruitment-lakehouse/src/producer/replay_producer.py) _(Tầng 2 — chú ý hàm `run_replay` ở cuối file, nó gọi tuần tự: DuckDB sort CSV → đọc chunk → `TimeMapper` → `FaultInjector` → gửi Redpanda → ghi Manifest)_.

---

## ⚡ EPIC 3 (Task 3.1 → 3.3) — Spark Streaming & Q1 Ingest (Chỗ mình đang đứng)

- **Input:** Tin nhắn thô từ Redpanda topic **`cpc_events_raw`**.
- **Output:**
  - Dữ liệu hợp lệ ➔ Bảng Delta **Bronze** (`s3a://lakehouse/bronze/cpc_events`, chia theo `event_date`).
  - Dữ liệu rác ➔ Bảng Delta **DLQ** (`s3a://lakehouse/dlq/cpc_events_dlq`) + topic **`cpc_events_dlq`**.
  - Checkpoint lưu vị trí đã đọc ➔ `s3a://lakehouse/checkpoints/ingest_bronze`.

```text
Redpanda (cpc_events_raw)
   │
   ▼
src/streaming/ingest.py   (Tầng 2: Orchestrator của Q1 Ingest)
   ├── import src/common/config.py             (Tầng 0: lấy topic, trigger, đường dẫn)
   ├── import src/streaming/spark_session.py   (Tầng 0/1: dựng SparkSession 16GB + S3A + Delta)
   └── import src/streaming/validation.py      (Tầng 1: soi JSON & tách valid_df / invalid_df)
          │                                       ▲ (đối chiếu 100% với cpc_event.py qua
          │                                       │  tests/unit/test_validation_parity.py)
          ├── valid_df   ──► Delta Bronze (MinIO, idempotent txnVersion)
          └── invalid_df ──► Delta DLQ (MinIO) + Kafka topic cpc_events_dlq
```

**Trình tự đọc (Lý thuyết ➔ Code):**

1. **Hiểu Spark + Delta + DuckDB liên thông:** Đọc [F10_lakehouse_table_formats.md](file:///d:/realtime-recruitment-lakehouse/docs/lessons/F10_lakehouse_table_formats.md) + [T03_delta_lake.md](file:///d:/realtime-recruitment-lakehouse/docs/lessons/T03_delta_lake.md) + [T05_duckdb.md](file:///d:/realtime-recruitment-lakehouse/docs/lessons/T05_duckdb.md) ➔ Mở [Dockerfile](file:///d:/realtime-recruitment-lakehouse/infra/spark/Dockerfile) và [smoke_delta_interop.py](file:///d:/realtime-recruitment-lakehouse/tests/integration/smoke_delta_interop.py).
2. **Hiểu cách cấu hình SparkSession:** Đọc [T02_spark_structured_streaming.md](file:///d:/realtime-recruitment-lakehouse/docs/lessons/T02_spark_structured_streaming.md) ➔ Mở [spark_session.py](file:///d:/realtime-recruitment-lakehouse/src/streaming/spark_session.py) _(Tầng 0/1 — chỉ cần chú ý `build_spark_conf` và `build_spark`)_.
3. **Hiểu logic soi dữ liệu rác bằng Spark:** Mở [validation.py](file:///d:/realtime-recruitment-lakehouse/src/streaming/validation.py) _(Tầng 1 — chỉ có 1 hàm chính là `validate_df(df)`)_ cùng với [test_validation_parity.py](file:///d:/realtime-recruitment-lakehouse/tests/unit/test_validation_parity.py) _(xem 33 ca test để thấy rõ từng loại lỗi bị bắt ra sao)_.
4. **Hiểu luồng Q1 hoàn chỉnh:** Mở [ingest.py](file:///d:/realtime-recruitment-lakehouse/src/streaming/ingest.py) _(Tầng 2 — đọc đúng 3 hàm theo thứ tự chạy: `read_kafka_raw_stream` → `make_process_batch` → `start_ingest_query`)_.

---

### 💡 Mẹo đọc nhanh không bị rối

- **Đừng đọc từ dòng 1 xuống dòng cuối một mạch:** Trong mỗi file Tầng 2 (`replay_producer.py`, `ingest.py`), hãy cuộn xuống **`main()`** trước để thấy khung xương 4–5 bước chính, rồi mới nhảy ngược lên xem từng hàm con làm gì.
- Khi thấy một khái niệm lạ trong code (ví dụ `txnVersion`, `path.style.access`, `PERMISSIVE`), gõ `Ctrl+F` trong [code_map.md](file:///d:/realtime-recruitment-lakehouse/docs/code_map.md) — nó sẽ chỉ thẳng sang đúng bài học `Fxx`/`Txx` giải thích khái niệm đó.

```

````
