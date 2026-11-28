#!/usr/bin/env python3
"""Benchmark Small Files Problem & Delta OPTIMIZE Compaction (Task 3.8 / Lab L4).

Creates a temporary fragmented Delta table on MinIO containing ~240+ small Parquet
files, measures:
  - File count (numFiles) and average file size via DeltaTable.detail()
  - DuckDB delta_scan() query latency (median & p95 across multiple runs)

Then runs ``compact_table()`` (Delta OPTIMIZE) and repeats the measurements, printing
a Markdown comparison table and JSON summary. Cleans up temporary MinIO objects on exit.
"""

from __future__ import annotations

import argparse
import json
import math
import statistics
import sys
import time
from decimal import Decimal
from pathlib import Path
from typing import Any

# Ensure project root is importable when executed directly inside container
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.common.config import AppConfig, load  # noqa: E402
from src.streaming.maintenance import compact_table, get_table_stats  # noqa: E402
from src.streaming.spark_session import build_spark  # noqa: E402
from tests.integration.smoke_delta_interop import (  # noqa: E402
    build_duckdb_connection,
    cleanup_s3_prefix,
)

BENCH_PREFIX = "_bench_small_files"


def _create_fragmented_delta_table(
    spark: Any,
    s3a_path: str,
    *,
    target_files: int,
    appends: int = 3,
) -> int:
    """Write ``target_files`` non-empty small Parquet files across ``appends`` commits."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import DecimalType

    files_per_append = max(1, math.ceil(target_files / appends))
    rows_per_append = files_per_append * 4  # guarantee >= 4 rows per Parquet file
    total_rows = 0

    for batch_idx in range(appends):
        start_id = batch_idx * rows_per_append
        end_id = start_id + rows_per_append
        df = (
            spark.range(start_id, end_id)
            .withColumn("campaign_id", (F.col("id") % F.lit(15)).cast("int"))
            .withColumn("spend", F.lit(Decimal("1.2500")).cast(DecimalType(18, 4)))
            .withColumn("event_date", F.lit("2026-01-01").cast("date"))
            .withColumn("_bucket", (F.col("id") % F.lit(files_per_append)).cast("int"))
            .repartition(files_per_append, F.col("_bucket"))
            .drop("_bucket")
        )
        df.write.format("delta").mode("append").partitionBy("event_date").save(s3a_path)
        total_rows += rows_per_append

    return total_rows


def _measure_duckdb_query_ms(cfg: AppConfig, s3_path: str, *, num_queries: int) -> dict[str, Any]:
    """Measure DuckDB delta_scan() latency (1 warmup + N timed queries)."""
    sql = (
        f"SELECT campaign_id, COUNT(*), SUM(spend) "
        f"FROM delta_scan('{s3_path}') GROUP BY campaign_id ORDER BY campaign_id"
    )
    con = build_duckdb_connection(cfg)
    timings_ms: list[float] = []
    row_sum = 0
    try:
        # Warmup once to initialize S3 connection/metadata cache fairly
        warmup_rows = con.execute(sql).fetchall()
        row_sum = sum(int(r[1]) for r in warmup_rows)

        for _ in range(num_queries):
            t0 = time.perf_counter()
            res = con.execute(sql).fetchall()
            elapsed_ms = (time.perf_counter() - t0) * 1000.0
            timings_ms.append(round(elapsed_ms, 2))
            row_sum = sum(int(r[1]) for r in res)
    finally:
        con.close()

    sorted_t = sorted(timings_ms)
    p95_idx = min(len(sorted_t) - 1, math.ceil(0.95 * len(sorted_t)) - 1)
    return {
        "row_count": row_sum,
        "timings_ms": timings_ms,
        "median_ms": round(statistics.median(sorted_t), 2),
        "p95_ms": sorted_t[p95_idx],
        "min_ms": sorted_t[0],
    }


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Benchmark DuckDB delta_scan() before & after Delta OPTIMIZE compaction."
    )
    parser.add_argument(
        "--target-files",
        type=int,
        default=240,
        help="Approximate number of small Parquet files to generate (default: 240).",
    )
    parser.add_argument(
        "--queries",
        type=int,
        default=5,
        help="Number of timed DuckDB queries before and after compaction (default: 5).",
    )
    parser.add_argument(
        "--keep-tmp",
        action="store_true",
        help="Keep temporary _bench_small_files Delta table on MinIO.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Create fragmented Delta table, measure before/after OPTIMIZE, and print report."""
    args = _parse_args(argv)
    cfg = load()
    s3a_path = cfg.s3a_uri(BENCH_PREFIX)
    s3_path = cfg.s3_uri(BENCH_PREFIX)

    cleanup_s3_prefix(cfg, f"{BENCH_PREFIX}/")
    spark = build_spark("bench_small_files", cfg=cfg, log_level="WARN")

    try:
        written_rows = _create_fragmented_delta_table(
            spark,
            s3a_path,
            target_files=args.target_files,
        )
        before_stats = get_table_stats(spark, s3a_path)
        before_query = _measure_duckdb_query_ms(cfg, s3_path, num_queries=args.queries)

        compact_res = compact_table(spark, s3a_path)
        after_stats = compact_res["after"]
        after_query = _measure_duckdb_query_ms(cfg, s3_path, num_queries=args.queries)
    finally:
        spark.stop()
        if not args.keep_tmp:
            cleanup_s3_prefix(cfg, f"{BENCH_PREFIX}/")

    assert before_query["row_count"] == written_rows
    assert after_query["row_count"] == written_rows

    file_reduction_pct = round(
        (1.0 - (after_stats["num_files"] / before_stats["num_files"])) * 100.0, 2
    )
    speedup = round(before_query["median_ms"] / max(0.01, after_query["median_ms"]), 2)

    summary = {
        "rows": written_rows,
        "before": {**before_stats, **before_query},
        "after": {**after_stats, **after_query},
        "optimize_elapsed_sec": compact_res["elapsed_sec"],
        "file_reduction_pct": file_reduction_pct,
        "median_speedup_x": speedup,
    }

    print("\n" + "=" * 88)
    print(f"SMALL FILES BENCHMARK RESULTS (Task 3.8 / Lab L4) | rows={written_rows}")
    print("=" * 88)
    print("| Phase | Num Files | Total Bytes | Avg File Bytes | DuckDB Median (ms) | DuckDB p95 (ms) |")
    print("| :--- | ---: | ---: | ---: | ---: | ---: |")
    print(
        f"| Before OPTIMIZE | {before_stats['num_files']} | {before_stats['size_in_bytes']} | "
        f"{before_stats['avg_file_size_bytes']} | {before_query['median_ms']} | {before_query['p95_ms']} |"
    )
    print(
        f"| After OPTIMIZE | {after_stats['num_files']} | {after_stats['size_in_bytes']} | "
        f"{after_stats['avg_file_size_bytes']} | {after_query['median_ms']} | {after_query['p95_ms']} |"
    )
    print(
        f"\nFile reduction: {file_reduction_pct}% | Median speedup: {speedup}x | "
        f"OPTIMIZE time: {compact_res['elapsed_sec']}s"
    )
    print("JSON_BENCH=" + json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
