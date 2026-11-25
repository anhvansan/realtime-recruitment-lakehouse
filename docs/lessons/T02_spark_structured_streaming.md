# T02 — Apache Spark & Structured Streaming (PySpark)

## 1. Câu hỏi gốc
Làm sao xây dựng một runtime Spark trong Docker gọn nhẹ (< 2.3GB RAM), khởi động lặp lại được (không tải JAR lúc chạy), và kết nối trơn tru với cả Redpanda (Kafka API) lẫn MinIO (S3A + Delta Lake)?

## 2. Trực giác
Spark giống như một dây chuyền nhà máy chạy trên JVM. Nếu mỗi lần bấm nút khởi động bạn mới chạy đi mua phụ tùng (tải JAR từ Maven Central bằng `--packages`), dây chuyền vừa chậm vừa dễ hỏng khi mạng chập chờn hoặc phụ tùng đổi phiên bản. Cách làm chuẩn kỹ sư là **nướng sẵn toàn bộ phụ tùng đúng đời (pinned JARs)** ngay trong lúc đúc container image (`Dockerfile`), đồng thời giới hạn số băng chuyền (`shuffle.partitions=4`, `master=local[2]`) cho vừa khít diện tích xưởng 16GB RAM.

## 3. Cơ chế & Kiến trúc Runtime trong dự án

```
infra/spark/Dockerfile (apache/spark:3.5.3-scala2.12-java17-python3-ubuntu)
├── /opt/spark/jars/  (Baked at build time — zero runtime network download)
│   ├── delta-spark_2.12-3.2.1.jar + delta-storage-3.2.1.jar
│   ├── spark-sql-kafka-0-10_2.12-3.5.3.jar + kafka-clients
│   ├── hadoop-aws-3.3.4.jar
│   └── aws-java-sdk-bundle-1.12.262.jar
├── Python env: delta-spark==3.2.1, duckdb==2.0.0, pydantic, boto3
└── Volumes:
    ├── .:/app (mã nguồn dự án)
    └── ${DATA_ROOT}/ivy_cache:/home/spark/.ivy2 (cache ổ D, bảo vệ ổ C)
```

**4 cấu hình bắt buộc khi khởi tạo `SparkSession` làm việc với Delta + MinIO:**
1. **Delta Extensions & Catalog:** `spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension` và `spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog`.
2. **S3A Path-Style Access:** `spark.hadoop.fs.s3a.path.style.access=true` (nếu thiếu, Hadoop sẽ gọi `lakehouse.minio:9000` gây lỗi DNS).
3. **S3A Endpoint & SSL:** Truyền đầy đủ scheme `http://minio:9000` (`cfg.s3_endpoint`) và `fs.s3a.connection.ssl.enabled=false`.
4. **Local Tuning:** `spark.sql.shuffle.partitions=4` (tránh mặc định 200 tạo ra hàng trăm file Parquet rỗng/nhỏ) và `spark.sql.session.timeZone=UTC`.

## 4. Ví dụ số
- Ngân sách RAM của container `cpc-spark`: `mem_limit: 2304m` (2.25GB).
- Khi idle (`tail -f /dev/null` chờ chạy job/shell): RAM tiêu thụ < 50MB (thấp hơn rất nhiều ngưỡng AC `< 1GB`).
- Khi chạy smoke test (`driver.memory=1g`, `local[2]`): xử lý 23 commit Delta lên MinIO trong vài chục giây mà không vượt quá trần 2.3GB.

## 5. Điểm mạnh / Điểm yếu / Khi nào không dùng
- **Điểm mạnh:** Một API thống nhất cho cả batch và streaming; chịu lỗi bằng checkpoint; tích hợp bản địa với Delta Lake và Kafka.
- **Điểm yếu và bẫy:** Nặng RAM (JVM); cực kỳ nhạy cảm với **lệch phiên bản Scala (`_2.12` vs `_2.13`)** và **lệch phiên bản Hadoop (`hadoop-aws` phải khớp chính xác Hadoop `3.3.4` trong image Spark `3.5.3`)**.
- **Khi nào không dùng:** Chỉ cần truy vấn phân tích file nhỏ hoặc batch vài giây ➔ dùng DuckDB trực tiếp nhanh và nhẹ hơn nhiều.

## 6. Hiểu lầm phổ biến
1. *Hiểu lầm:* Cài `pip install delta-spark` là đủ để Spark chạy Delta Lake.  
   *Sự thật:* `delta-spark` trên pip chỉ cung cấp Python wrapper; tầng thực thi nằm ở các file JAR Java/Scala (`delta-spark_2.12` và `delta-storage`) phải nằm trong classpath của JVM (`/opt/spark/jars/`).
2. *Hiểu lầm:* `hadoop-aws` bản mới nhất luôn tốt hơn.  
   *Sự thật:* Dùng `hadoop-aws:3.4.x` trên Spark biên dịch với Hadoop `3.3.4` sẽ gây lỗi `NoSuchMethodError` hoặc `ClassNotFoundException` lúc chạy.

## 7. Trong repo này

| File:dòng | Symbol | Giải thích |
| :--- | :--- | :--- |
| `infra/spark/Dockerfile:5-38` | `FROM` & `download_jars.py` | Pin `apache/spark:3.5.3`, nướng sẵn 8 JAR thiếu vào `/opt/spark/jars/` không qua Ivy. |
| `infra/spark/download_jars.py:37-105` | `jar_paths_by_group`, `download_one` | Ép IPv4, kiểm tra `Content-Length`, retry 5 lần, tách layer `aws` (~270MB) và `core` (~13MB). |
| `docker-compose.yml:119-151` | `services.spark` | Cấu hình profile `apps`, `mem_limit: 2304m`, bind mount Ivy cache sang ổ D. |
| `tests/integration/smoke_delta_interop.py:63-86` | `build_smoke_spark` | Khởi tạo `SparkSession` chuẩn Delta + MinIO S3A từ `src/common/config.py`. |

## 8. Tự kiểm tra
1. *Câu hỏi:* Vì sao trong `Dockerfile` ta thay `spark-submit --packages` (Ivy) bằng `download_jars.py` tải trực tiếp 8 file JAR từ Maven Central?  
   *Đáp án:* Ivy trong container WSL2/Docker phải phân giải hàng loạt parent POM qua IPv6 dễ timeout (~130s/lần), tải lại nhiều JAR đã có sẵn trong Spark (`hadoop-client-runtime`, `hadoop-client-api`) và không tự retry khi file JAR bị đứt kết nối giữa chừng; `download_jars.py` ép IPv4, bỏ qua JAR đã có, kiểm tra `Content-Length` và retry tự động.
2. *Câu hỏi:* Tại sao `spark.sql.shuffle.partitions` phải đổi từ mặc định 200 xuống 4?  
   *Đáp án:* Trên máy local 2 core với tập dữ liệu nhỏ, 200 partition sinh ra 200 task và hàng trăm file Parquet nhỏ li ti trên mỗi micro-batch, làm chậm cả Spark lẫn DuckDB.
