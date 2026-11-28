#!/usr/bin/env python3
"""Empirical Watermark & Late-Data Policy Experiment for ADR-006 (Task 3.7 / Lab L3).

Stages the existing Bronze Delta table into N sequential 1-file Delta commits in a
temporary directory so Spark Structured Streaming advances the watermark across multiple
micro-batches, then runs 4 Q2 Refine configurations on identical input:
  1. P1_wm_5m      : watermark_delay="5 minutes", stateful dropDuplicatesWithinWatermark + MERGE
  2. P1_wm_10m     : watermark_delay="10 minutes", stateful dropDuplicatesWithinWatermark + MERGE
  3. P1_wm_20m     : watermark_delay="20 minutes", stateful dropDuplicatesWithinWatermark + MERGE
  4. P3_merge_only : no stateful watermark dedup, in-batch dropDuplicates + global Delta MERGE

Collects MetricsListener telemetry (state rows, state memory bytes, batch durations) and
compares Silver output against the replay manifest + late_ids.jsonl.
"""

from __future__ import annotations

import argparse
import json
import math
import shutil
import sys
import time
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any

# Ensure project root is importable when executed directly inside container
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.common.config import AppConfig, load  # noqa: E402
from src.streaming.listener import MetricsListener  # noqa: E402
from src.streaming.refine import start_refine_query  # noqa: E402
from src.streaming.spark_session import build_spark  # noqa: E402
from tests.integration.reconcile import (  # noqa: E402
    find_latest_manifest,
    load_late_events,
    resolve_late_ids_path,
)


@dataclass(frozen=True)
class ExperimentVariant:
    """Single watermark/dedup configuration evaluated in ADR-006."""

    name: str
    watermark_delay: str | None
    disable_stateful_dedup: bool


VARIANTS: tuple[ExperimentVariant, ...] = (
    ExperimentVariant(name="P1_wm_5m", watermark_delay="5 minutes", disable_stateful_dedup=False),
    ExperimentVariant(name="P1_wm_10m", watermark_delay="10 minutes", disable_stateful_dedup=False),
    ExperimentVariant(name="P1_wm_20m", watermark_delay="20 minutes", disable_stateful_dedup=False),
    ExperimentVariant(name="P3_merge_only", watermark_delay=None, disable_stateful_dedup=True),
)


def stage_chunked_bronze(
    spark: Any,
    source_bronze_uri: str,
    target_bronze_dir: Path,
    num_batches: int,
) -> tuple[int, set[str]]:
    """Copy Bronze rows sorted by arrival order into ``num_batches`` sequential Delta commits."""
    from pyspark.sql import functions as F

    bronze_df = spark.read.format("delta").load(source_bronze_uri)
    ordered_df = bronze_df.orderBy(
        F.col("kafka_timestamp").asc_nulls_last(),
        F.col("kafka_partition").asc_nulls_last(),
        F.col("kafka_offset").asc_nulls_last(),
        F.col("source_row_id").asc_nulls_last(),
    )
    all_rows = ordered_df.collect()
    schema = ordered_df.schema
    total_rows = len(all_rows)
    if total_rows == 0:
        raise RuntimeError(f"Bronze table is empty at {source_bronze_uri}")

    bronze_unique_ids = {str(r["event_id"]) for r in all_rows}
    actual_batches = max(1, min(num_batches, total_rows))
    chunk_size = math.ceil(total_rows / actual_batches)

    target_uri = str(target_bronze_dir)
    for idx in range(0, total_rows, chunk_size):
        chunk = all_rows[idx : idx + chunk_size]
        chunk_df = spark.createDataFrame(chunk, schema=schema).coalesce(1)
        chunk_df.write.format("delta").mode("append").save(target_uri)

    return total_rows, bronze_unique_ids


def run_variant(
    spark: Any,
    cfg: AppConfig,
    variant: ExperimentVariant,
    *,
    chunked_bronze_dir: Path,
    work_dir: Path,
    bronze_unique_ids: set[str],
    late_events: dict[str, Any],
    expected_spend_total: Decimal,
) -> dict[str, Any]:
    """Execute one Q2 Refine variant over the staged multi-batch Bronze table."""
    from pyspark.sql import functions as F

    run_root = work_dir / variant.name
    silver_dir = run_root / "silver"
    dlq_dir = run_root / "dlq"
    ckpt_dir = run_root / "checkpoint"

    collected_metrics: list[dict[str, Any]] = []
    listener = MetricsListener(
        cfg=cfg,
        flush_interval_sec=0.0,
        s3_writer=lambda _payload: None,
        delta_writer=collected_metrics.extend,
    )
    spark.streams.addListener(listener)

    t0 = time.perf_counter()
    try:
        query = start_refine_query(
            spark,
            cfg,
            available_now=True,
            bronze_path=str(chunked_bronze_dir),
            silver_path=str(silver_dir),
            dlq_path=str(dlq_dir),
            checkpoint_path=str(ckpt_dir),
            watermark_delay=variant.watermark_delay,
            max_files_per_trigger=1,
            disable_stateful_dedup=variant.disable_stateful_dedup,
            query_name=f"exp_{variant.name}",
        )
        query.awaitTermination()
        time.sleep(0.25)
        listener.flush()
    finally:
        spark.streams.removeListener(listener)

    wall_sec = round(time.perf_counter() - t0, 2)

    silver_df = spark.read.format("delta").load(str(silver_dir))
    silver_agg = silver_df.agg(
        F.count("*").alias("cnt"),
        F.countDistinct("event_id").alias("distinct_cnt"),
        F.sum("spend").alias("sum_spend"),
    ).collect()[0]

    silver_rows = int(silver_agg["cnt"])
    silver_distinct = int(silver_agg["distinct_cnt"])
    silver_spend = (
        Decimal(str(silver_agg["sum_spend"]))
        if silver_agg["sum_spend"] is not None
        else Decimal("0.0000")
    )
    silver_ids = {str(r["event_id"]) for r in silver_df.select("event_id").collect()}

    dropped_ids = sorted(bronze_unique_ids - silver_ids)
    dropped_in_late = [eid for eid in dropped_ids if eid in late_events]
    dropped_unexpected = [eid for eid in dropped_ids if eid not in late_events]

    max_state_rows = max((int(m["state_num_rows_total"]) for m in collected_metrics), default=0)
    max_state_mem = max((int(m["state_memory_used_bytes"]) for m in collected_metrics), default=0)
    non_empty_batches = [m for m in collected_metrics if int(m["input_rows"]) > 0]
    total_batch_ms = sum(int(m["batch_duration_ms"]) for m in non_empty_batches)
    avg_batch_ms = (
        round(total_batch_ms / len(non_empty_batches), 1) if non_empty_batches else 0.0
    )

    spend_loss = expected_spend_total - silver_spend

    return {
        "config": variant.name,
        "watermark": variant.watermark_delay or "none (P3 MERGE-only)",
        "bronze_unique": len(bronze_unique_ids),
        "silver_rows": silver_rows,
        "silver_distinct": silver_distinct,
        "dropped_total": len(dropped_ids),
        "dropped_in_late_ids": len(dropped_in_late),
        "dropped_unexpected": len(dropped_unexpected),
        "silver_spend": f"{silver_spend:.4f}",
        "spend_loss": f"{spend_loss:.4f}",
        "max_state_rows": max_state_rows,
        "max_state_mem_bytes": max_state_mem,
        "batches_processed": len(non_empty_batches),
        "avg_batch_ms": avg_batch_ms,
        "wall_sec": wall_sec,
    }


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run Task 3.7 ADR-006 watermark vs MERGE-only experiment."
    )
    parser.add_argument(
        "--manifest",
        type=Path,
        default=None,
        help="Path to replay_<run_id>.json manifest (default: latest in data/manifests).",
    )
    parser.add_argument(
        "--num-batches",
        type=int,
        default=40,
        help="Number of sequential Bronze micro-batch commits to stage (default: 40).",
    )
    parser.add_argument(
        "--work-dir",
        type=Path,
        default=Path("/tmp/_exp_adr006"),
        help="Temporary directory inside container for staged Bronze/Silver runs.",
    )
    parser.add_argument(
        "--keep-tmp",
        action="store_true",
        help="Preserve staged experiment Delta tables after printing results.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Run all 4 ADR-006 variants and print Markdown + JSON comparison results."""
    args = _parse_args(argv)
    cfg = load()

    manifest_dir = _PROJECT_ROOT / "data" / "manifests"
    manifest_path = (
        args.manifest if args.manifest is not None else find_latest_manifest(manifest_dir)
    )
    with manifest_path.open("r", encoding="utf-8") as fh:
        manifest: dict[str, Any] = json.load(fh)

    late_ids_path = resolve_late_ids_path(manifest_path, manifest)
    late_events = load_late_events(late_ids_path)
    expected_spend_total = Decimal(str(manifest["expected_spend_total"]))

    work_dir: Path = args.work_dir
    if work_dir.exists():
        shutil.rmtree(work_dir)
    chunked_bronze_dir = work_dir / "bronze_chunked"
    chunked_bronze_dir.mkdir(parents=True, exist_ok=True)

    spark = build_spark(
        "exp_adr006_watermark",
        cfg=cfg,
        warehouse_dir=str(work_dir / "warehouse"),
        log_level="WARN",
    )
    try:
        total_bronze, bronze_unique_ids = stage_chunked_bronze(
            spark,
            cfg.s3a_uri(cfg.pipeline.storage.bronze_path),
            chunked_bronze_dir,
            num_batches=args.num_batches,
        )

        results: list[dict[str, Any]] = []
        for variant in VARIANTS:
            res = run_variant(
                spark,
                cfg,
                variant,
                chunked_bronze_dir=chunked_bronze_dir,
                work_dir=work_dir,
                bronze_unique_ids=bronze_unique_ids,
                late_events=late_events,
                expected_spend_total=expected_spend_total,
            )
            results.append(res)
    finally:
        spark.stop()
        if not args.keep_tmp and work_dir.exists():
            shutil.rmtree(work_dir, ignore_errors=True)

    print("\n" + "=" * 96)
    print(
        f"ADR-006 EXPERIMENT RESULTS | manifest={manifest_path.name} "
        f"| bronze_rows={total_bronze} | bronze_unique={len(bronze_unique_ids)} "
        f"| late_injected={len(late_events)} | staged_batches={args.num_batches}"
    )
    print("=" * 96)
    header = (
        "| Config | Watermark | Silver | Dropped (Late / Unexp) | Spend Loss | "
        "Max State Rows | Max State Bytes | Avg Batch ms | Wall (s) |"
    )
    sep = "| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"
    print(header)
    print(sep)
    for r in results:
        print(
            f"| {r['config']} | {r['watermark']} | {r['silver_rows']} | "
            f"{r['dropped_total']} ({r['dropped_in_late_ids']}/{r['dropped_unexpected']}) | "
            f"{r['spend_loss']} | {r['max_state_rows']} | {r['max_state_mem_bytes']} | "
            f"{r['avg_batch_ms']} | {r['wall_sec']} |"
        )
    print("\nJSON_RESULTS=" + json.dumps(results, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
