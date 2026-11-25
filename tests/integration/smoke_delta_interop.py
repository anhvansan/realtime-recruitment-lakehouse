#!/usr/bin/env python3
"""Integration Smoke Test: Spark 3.5.3 <-> Delta Lake 3.2.1 <-> MinIO <-> DuckDB 2.0.0.

Verifies Task 3.1 Acceptance Criteria:
  (a) Spark writes a Delta table to s3a://lakehouse/_smoke/t1 across 3 commits
      (append, append, merge).
  (b) DuckDB delta_scan('s3://lakehouse/_smoke/t1') reads exact row count and values.
  (c) Spark runs 20 sequential commits while DuckDB reads continuously in a background
      thread: zero errors, zero torn/partial reads, monotonic row counts.
  (d) Table Features compatibility check (Deletion Vectors / Protocol v3/v7) between
      pinned Spark/Delta and DuckDB.

Follows Rule R6: Never logs S3 credentials or CREATE SECRET statements.
"""

from __future__ import annotations

import os
import sys
import threading
import time
from decimal import Decimal
from pathlib import Path
from typing import Any

import boto3
import duckdb
from botocore.client import Config as BotoConfig
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.types import DecimalType, IntegerType, StringType, StructField, StructType

# Ensure project root is importable when executed directly
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.common.config import AppConfig, load  # noqa: E402

SMOKE_PREFIX = "_smoke"
TABLE_T1_PREFIX = f"{SMOKE_PREFIX}/t1"
TABLE_FEATURES_PREFIX = f"{SMOKE_PREFIX}/t1_features"


def cleanup_s3_prefix(cfg: AppConfig, prefix: str) -> int:
    """Delete all objects under a given S3 prefix so the smoke test is idempotent."""
    s3 = boto3.client(
        "s3",
        endpoint_url=cfg.s3_endpoint,
        aws_access_key_id=cfg.s3_access_key,
        aws_secret_access_key=cfg.s3_secret_key,
        region_name=cfg.s3_region,
        config=BotoConfig(s3={"addressing_style": "path"}, signature_version="s3v4"),
    )
    paginator = s3.get_paginator("list_objects_v2")
    deleted = 0
    for page in paginator.paginate(Bucket=cfg.s3_bucket, Prefix=prefix):
        contents = page.get("Contents", [])
        if not contents:
            continue
        keys = [{"Key": obj["Key"]} for obj in contents]
        s3.delete_objects(Bucket=cfg.s3_bucket, Delete={"Objects": keys})
        deleted += len(keys)
    return deleted


def build_smoke_spark(cfg: AppConfig) -> SparkSession:
    """Build a local SparkSession configured with Delta Lake and MinIO S3A."""
    return (
        SparkSession.builder.appName("smoke_delta_interop")
        .master("local[2]")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", cfg.s3_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", cfg.s3_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", cfg.s3_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(cfg.s3_use_ssl).lower())
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def build_duckdb_connection(cfg: AppConfig) -> duckdb.DuckDBPyConnection:
    """Create an in-memory DuckDB connection configured with S3 secret (no http://)."""
    ext_dir = os.environ.get("DUCKDB_EXTENSION_DIRECTORY", "/opt/duckdb_extensions")
    conn_cfg: dict[str, Any] = {}
    if Path(ext_dir).exists():
        conn_cfg["extension_directory"] = ext_dir

    con = duckdb.connect(":memory:", config=conn_cfg)
    con.execute("SET memory_limit = '256MB'")
    con.execute("SET threads = 2")
    con.execute("LOAD httpfs")
    con.execute("LOAD delta")

    use_ssl_sql = "true" if cfg.s3_use_ssl else "false"
    # Rule R6: Never log this SQL string containing secrets
    con.execute(
        f"""
        CREATE OR REPLACE SECRET lakehouse_s3 (
            TYPE S3,
            KEY_ID '{cfg.s3_access_key}',
            SECRET '{cfg.s3_secret_key}',
            ENDPOINT '{cfg.s3_endpoint_host_port}',
            URL_STYLE 'path',
            USE_SSL {use_ssl_sql},
            REGION '{cfg.s3_region}'
        )
        """
    )
    return con


def run_test_a(spark: SparkSession, s3a_path: str) -> int:
    """(a) Spark writes Delta table across 3 commits: append, append, merge."""
    print("\n[Test A] Spark ghi bảng Delta qua 3 commit (append, append, merge)...")
    schema = StructType(
        [
            StructField("id", IntegerType(), nullable=False),
            StructField("campaign_id", IntegerType(), nullable=False),
            StructField("spend", DecimalType(18, 4), nullable=False),
            StructField("tag", StringType(), nullable=False),
            StructField("batch_num", IntegerType(), nullable=False),
        ]
    )

    # Commit 0: Initial append (3 rows)
    rows_c0 = [
        (1, 101, Decimal("1.5000"), "c0_init", 0),
        (2, 101, Decimal("2.0000"), "c0_init", 0),
        (3, 102, Decimal("3.2500"), "c0_init", 0),
    ]
    spark.createDataFrame(rows_c0, schema=schema).write.format("delta").mode("append").save(
        s3a_path
    )

    # Commit 1: Second append (2 rows)
    rows_c1 = [
        (4, 102, Decimal("4.0000"), "c1_append", 0),
        (5, 103, Decimal("5.7500"), "c1_append", 0),
    ]
    spark.createDataFrame(rows_c1, schema=schema).write.format("delta").mode("append").save(
        s3a_path
    )

    # Commit 2: Merge (update id=2, insert id=6)
    rows_c2 = [
        (2, 101, Decimal("2.5000"), "c2_merged_update", 0),
        (6, 103, Decimal("6.0000"), "c2_merged_insert", 0),
    ]
    src_df = spark.createDataFrame(rows_c2, schema=schema)
    dt = DeltaTable.forPath(spark, s3a_path)
    (
        dt.alias("t")
        .merge(src_df.alias("s"), "t.id = s.id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    history_versions = [row["version"] for row in dt.history().select("version").collect()]
    assert len(history_versions) == 3, f"Expected 3 Delta commits, got {history_versions}"
    print(f"  ✅ [Test A] PASS — 3 commits verified in _delta_log: versions={sorted(history_versions)}")
    return 6


def run_test_b(con: duckdb.DuckDBPyConnection, s3_path: str, expected_rows: int) -> None:
    """(b) DuckDB delta_scan reads exact row count, DECIMAL sum, and merged values."""
    print("\n[Test B] DuckDB delta_scan đọc kiểm chứng số dòng và giá trị...")
    rows = con.execute(
        f"SELECT id, campaign_id, spend, tag FROM delta_scan('{s3_path}') ORDER BY id"
    ).fetchall()

    assert len(rows) == expected_rows, f"Expected {expected_rows} rows, got {len(rows)}"
    by_id = {r[0]: r for r in rows}

    assert by_id[2][2] == Decimal("2.5000"), f"Row id=2 spend mismatch: {by_id[2]}"
    assert by_id[2][3] == "c2_merged_update", f"Row id=2 tag mismatch: {by_id[2]}"
    assert by_id[6][2] == Decimal("6.0000"), f"Row id=6 spend mismatch: {by_id[6]}"
    assert by_id[6][3] == "c2_merged_insert", f"Row id=6 tag mismatch: {by_id[6]}"

    total_spend = con.execute(f"SELECT SUM(spend) FROM delta_scan('{s3_path}')").fetchone()[0]
    expected_spend = Decimal("23.0000")  # 1.5 + 2.5 + 3.25 + 4.0 + 5.75 + 6.0
    assert total_spend == expected_spend, f"Expected SUM(spend)={expected_spend}, got {total_spend}"

    print(
        f"  ✅ [Test B] PASS — DuckDB read {len(rows)} rows, "
        f"SUM(spend)={total_spend} (exact DECIMAL match)"
    )


def run_test_c(
    spark: SparkSession,
    cfg: AppConfig,
    s3a_path: str,
    s3_path: str,
    initial_rows: int,
) -> None:
    """(c) 20 concurrent Spark commits while DuckDB reads continuously (snapshot isolation)."""
    print("\n[Test C] Kiểm thử đồng thời: Spark ghi 20 commit liên tiếp + DuckDB đọc liên tục...")
    schema = StructType(
        [
            StructField("id", IntegerType(), nullable=False),
            StructField("campaign_id", IntegerType(), nullable=False),
            StructField("spend", DecimalType(18, 4), nullable=False),
            StructField("tag", StringType(), nullable=False),
            StructField("batch_num", IntegerType(), nullable=False),
        ]
    )

    num_commits = 20
    rows_per_commit = 5
    stop_event = threading.Event()
    reader_errors: list[str] = []
    observed_counts: list[int] = []

    def duckdb_reader_loop() -> None:
        reader_con = build_duckdb_connection(cfg)
        prev_count = initial_rows
        try:
            while not stop_event.is_set():
                res = reader_con.execute(
                    f"SELECT COUNT(*), MAX(batch_num) FROM delta_scan('{s3_path}')"
                ).fetchone()
                cnt, max_batch = int(res[0]), int(res[1])
                observed_counts.append(cnt)

                # Snapshot isolation invariants:
                # 1. Row count must be monotonically non-decreasing
                if cnt < prev_count:
                    reader_errors.append(f"Non-monotonic read: {cnt} < {prev_count}")
                # 2. Row count must match exact full-batch boundary (no torn/partial writes)
                expected_for_batch = initial_rows + max_batch * rows_per_commit
                if cnt != expected_for_batch:
                    reader_errors.append(
                        f"Torn/partial state detected: count={cnt} vs expected={expected_for_batch} "
                        f"at max_batch={max_batch}"
                    )
                prev_count = cnt
                time.sleep(0.05)
        except Exception as exc:  # noqa: BLE001
            reader_errors.append(f"DuckDB read exception: {type(exc).__name__}: {exc}")
        finally:
            reader_con.close()

    reader_thread = threading.Thread(target=duckdb_reader_loop, daemon=True)
    reader_thread.start()

    try:
        for batch_idx in range(1, num_commits + 1):
            base_id = 1000 + batch_idx * rows_per_commit
            batch_rows = [
                (base_id + j, 200 + j, Decimal("1.0000"), f"stream_b{batch_idx}", batch_idx)
                for j in range(rows_per_commit)
            ]
            spark.createDataFrame(batch_rows, schema=schema).write.format("delta").mode(
                "append"
            ).save(s3a_path)
    finally:
        stop_event.set()
        reader_thread.join(timeout=15)

    assert not reader_errors, f"Concurrent read errors detected: {reader_errors[:5]}"
    assert len(observed_counts) > 0, "DuckDB reader executed 0 reads during concurrent writes"

    # Final consistency verification
    final_con = build_duckdb_connection(cfg)
    try:
        final_count = final_con.execute(f"SELECT COUNT(*) FROM delta_scan('{s3_path}')").fetchone()[
            0
        ]
    finally:
        final_con.close()

    expected_final = initial_rows + num_commits * rows_per_commit
    assert (
        final_count == expected_final
    ), f"Expected final count={expected_final}, got {final_count}"

    print(
        f"  ✅ [Test C] PASS — 20 commits completed; DuckDB performed {len(observed_counts)} "
        f"concurrent reads with 0 errors and 0 torn states (final rows={final_count})"
    )


def run_test_d(
    spark: SparkSession,
    con: duckdb.DuckDBPyConnection,
    s3a_feat_path: str,
    s3_feat_path: str,
) -> None:
    """(d) Check Delta Table Features (Deletion Vectors / Protocol v3/v7) compatibility."""
    print("\n[Test D] Kiểm tra tương thích Delta Table Features (Deletion Vectors)...")
    df = spark.createDataFrame([(i, f"row_{i}") for i in range(1, 6)], ["id", "val"])
    df.write.format("delta").mode("overwrite").save(s3a_feat_path)

    spark.sql(
        f"ALTER TABLE delta.`{s3a_feat_path}` "
        "SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')"
    )

    # Trigger Deletion Vector bitmap via DELETE
    dt = DeltaTable.forPath(spark, s3a_feat_path)
    dt.delete("id = 3")

    detail = dt.detail().collect()[0]
    min_reader = detail["minReaderVersion"]
    min_writer = detail["minWriterVersion"]
    table_features = detail["tableFeatures"] or []
    assert (
        "deletionVectors" in table_features
    ), f"Expected 'deletionVectors' in tableFeatures, got {table_features}"

    rows = con.execute(f"SELECT id, val FROM delta_scan('{s3_feat_path}') ORDER BY id").fetchall()
    ids = [r[0] for r in rows]
    assert ids == [1, 2, 4, 5], f"Expected [1, 2, 4, 5] after DV delete, got {ids}"

    print(
        f"  ✅ [Test D] PASS — DuckDB natively read Delta table with "
        f"minReaderVersion={min_reader}, minWriterVersion={min_writer}, "
        f"tableFeatures={table_features} (rows={ids})"
    )


def main() -> int:
    """Execute full Spark <-> Delta <-> MinIO <-> DuckDB interop smoke test."""
    t0 = time.perf_counter()
    cfg = load()
    s3a_t1 = f"s3a://{cfg.s3_bucket}/{TABLE_T1_PREFIX}"
    s3_t1 = f"s3://{cfg.s3_bucket}/{TABLE_T1_PREFIX}"
    s3a_feat = f"s3a://{cfg.s3_bucket}/{TABLE_FEATURES_PREFIX}"
    s3_feat = f"s3://{cfg.s3_bucket}/{TABLE_FEATURES_PREFIX}"

    print("=" * 80)
    print("🚀 SMOKE TEST LIÊN THÔNG: Spark <-> Delta Lake <-> MinIO <-> DuckDB (Task 3.1)")
    print(f"   Spark S3A Endpoint : {cfg.s3_endpoint} (path-style=true)")
    print(f"   DuckDB Endpoint    : {cfg.s3_endpoint_host_port} (use_ssl={cfg.s3_use_ssl})")
    print(f"   Target Bucket/Path : {s3a_t1}")
    print("=" * 80)

    # Clean up any leftover smoke artifacts from prior runs
    cleanup_s3_prefix(cfg, f"{SMOKE_PREFIX}/")

    spark = build_smoke_spark(cfg)
    spark.sparkContext.setLogLevel("WARN")
    con = build_duckdb_connection(cfg)

    try:
        initial_rows = run_test_a(spark, s3a_t1)
        run_test_b(con, s3_t1, expected_rows=initial_rows)
        run_test_c(spark, cfg, s3a_t1, s3_t1, initial_rows=initial_rows)
        run_test_d(spark, con, s3a_feat, s3_feat)
    finally:
        con.close()
        spark.stop()
        removed = cleanup_s3_prefix(cfg, f"{SMOKE_PREFIX}/")
        print(f"\n🧹 Đã dọn sạch {removed} object tạm tại s3://{cfg.s3_bucket}/{SMOKE_PREFIX}/")

    elapsed = time.perf_counter() - t0
    print("\n" + "=" * 80)
    print(f"📊 KẾT QUẢ SMOKE TEST TASK 3.1: 4/4 MỤC (a, b, c, d) PASS trong {elapsed:.2f}s")
    print("=" * 80)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
