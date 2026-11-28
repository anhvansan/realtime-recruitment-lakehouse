# Benchmark Small Files Problem & Delta OPTIMIZE Compaction

- **Ngày đo:** 2026-11-28 (+07)
- **Công cụ:** [scripts/bench/bench_small_files.py](file:///d:/realtime-recruitment-lakehouse/scripts/bench/bench_small_files.py) & [src/streaming/maintenance.py](file:///d:/realtime-recruitment-lakehouse/src/streaming/maintenance.py) (`make compact`)
- **Môi trường:** `cpc-spark` (`local[2]`, `1536m` heap) + DuckDB `1.5.6` (`httpfs`, `delta`) ➔ MinIO (`s3://lakehouse/`)

---

## 1. Benchmark có kiểm soát (`_bench_small_files`, 960 dòng, 5 truy vấn/pha sau warmup)

| Pha đo | Số file (`numFiles`) | Tổng dung lượng (Bytes) | Trung bình/file (Bytes) | DuckDB Median (ms) | DuckDB p95 (ms) | DuckDB Min (ms) |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: |
| **Trước `OPTIMIZE`** | 156 | 170,318 | 1,092 | 572.41 | 645.97 | 525.95 |
| **Sau `OPTIMIZE`** (`10.02s`) | **1** | **5,621** | **5,621** | **54.45** | **67.31** | **52.94** |

- **Mức giảm số file:** **`99.36%`** (`156` ➔ `1`).
- **Tăng tốc truy vấn DuckDB (Median):** **`10.51x`** (`572.41 ms` ➔ `54.45 ms`).
- **Tăng tốc p95:** **`9.60x`** (`645.97 ms` ➔ `67.31 ms`).
- **Chi tiết 5 lần chạy (ms):**
  - Trước `OPTIMIZE`: `[591.67, 569.85, 645.97, 525.95, 572.41]`
  - Sau `OPTIMIZE`: `[54.99, 52.94, 53.04, 54.45, 67.31]`

---

## 2. Kết quả `make compact` trên dữ liệu thật `ac_3_9` (30 ngày `event_date`)

| Bảng Delta | Số file trước | Số file sau | Dung lượng trước (Bytes) | Dung lượng sau (Bytes) | TB/file trước ➔ sau | Thời gian `OPTIMIZE` |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: |
| `bronze/cpc_events` | 39 | **30** | 541,042 | 479,919 | `13,873 ➔ 15,997 B` | `7.04s` |
| `silver/cpc_events` | 79 | **30** | 430,961 | 239,912 | `5,455 ➔ 7,997 B` | `7.55s` |

- Silver giảm từ `79` xuống còn đúng **`30` file** (mỗi partition `event_date` còn đúng 1 file Parquet sạch) và dung lượng metadata/dữ liệu giảm **~44.3%** (`430,961 ➔ 239,912 bytes`).
- Kiểm chứng sau nén: `make reconcile MANIFEST=/app/data/manifests/replay_ac_3_9.json` ➔ **`PASS`**; chạy lại Q2 Refine (`--available-now`) bỏ qua commit nén (`dataChange=false`), giữ nguyên **`1,958`** dòng Silver.
