"""Unit and Spark integration tests for Task 3.4 — Q2 Refine (Silver MERGE & dedup)."""

from __future__ import annotations

import tempfile
from collections.abc import Callable
from decimal import Decimal
from pathlib import Path
from typing import Any, TypeVar

from src.common.config import load
from src.streaming.refine import make_upsert_silver
from src.streaming.spark_session import build_spark

try:
    import pytest
except ImportError:  # pragma: no cover - allows standalone execution inside spark container
    pytest = None  # type: ignore[assignment]

_F = TypeVar("_F", bound=Callable[..., Any])


def _mark_spark(func: _F) -> _F:
    if pytest is not None:
        return pytest.mark.spark(func)  # type: ignore[return-value]
    return func


def test_refine_config_defaults() -> None:
    """Host unit check: Q2 refine parameters load with expected defaults."""
    cfg = load()
    assert cfg.pipeline.streaming.max_files_per_trigger == 100
    assert cfg.pipeline.streaming.watermark_delay == "10 minutes"
    assert cfg.pipeline.storage.silver_path == "silver/cpc_events"
    assert cfg.pipeline.storage.checkpoints.refine_silver == "checkpoints/refine_silver"


def _run_upsert_silver_idempotency_check(base_dir: Path) -> None:
    """Verify upsert_silver deduplicates within batch and produces zero duplicates on replay."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        DateType,
        DecimalType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    silver_path = str(base_dir / "silver_cpc_events")
    dlq_path = str(base_dir / "dlq_cpc_events")

    spark = build_spark(
        "test_refine_upsert_silver",
        local_only=True,
        warehouse_dir=str(base_dir / "warehouse"),
    )
    try:
        schema = StructType(
            [
                StructField("schema_version", StringType(), False),
                StructField("event_id", StringType(), False),
                StructField("event_time", StringType(), False),
                StructField("produced_at", StringType(), False),
                StructField("event_type", StringType(), False),
                StructField("job_id", StringType(), False),
                StructField("campaign_id", StringType(), False),
                StructField("group_id", StringType(), True),
                StructField("publisher_id", StringType(), False),
                StructField("bid", StringType(), True),
                StructField("spend", StringType(), False),
                StructField("source_row_id", LongType(), False),
                StructField("payload_raw", StringType(), False),
                StructField("kafka_topic", StringType(), False),
                StructField("kafka_partition", StringType(), True),
                StructField("kafka_offset", LongType(), False),
            ]
        )

        # 3 unique valid events + 1 duplicate of evt-1 + 1 Silver-stage invalid negative spend
        rows = [
            (
                "v1",
                "evt-1",
                "2026-01-01T00:01:00Z",
                "2026-01-01T00:01:01Z",
                "click",
                "10",
                "100",
                "1",
                "5",
                "2.5000",
                "2.5000",
                1,
                '{"event_id":"evt-1"}',
                "cpc_events_raw",
                "0",
                1,
            ),
            (
                "v1",
                "evt-1",
                "2026-01-01T00:01:20Z",
                "2026-01-01T00:01:21Z",
                "click",
                "10",
                "100",
                "1",
                "5",
                "2.5000",
                "2.5000",
                1,
                '{"event_id":"evt-1"}',
                "cpc_events_raw",
                "0",
                2,
            ),
            (
                "v1",
                "evt-2",
                "2026-01-01T00:02:00.123456+00:00",
                "2026-01-01T00:02:01Z",
                "click",
                "11",
                "100",
                None,
                "6",
                "1.2500",
                "1.2500",
                2,
                '{"event_id":"evt-2"}',
                "cpc_events_raw",
                "0",
                3,
            ),
            (
                "v1",
                "evt-3",
                "2026-01-01T00:03:00Z",
                "2026-01-01T00:03:01Z",
                "apply",
                "12",
                "101",
                None,
                "7",
                None,
                "0.0000",
                3,
                '{"event_id":"evt-3"}',
                "cpc_events_raw",
                "0",
                4,
            ),
            (
                "v1",
                "evt-bad-neg",
                "2026-01-01T00:04:00Z",
                "2026-01-01T00:04:01Z",
                "click",
                "13",
                "101",
                None,
                "7",
                "1.0000",
                "-1.0000",
                4,
                '{"event_id":"evt-bad-neg"}',
                "cpc_events_raw",
                "0",
                5,
            ),
        ]

        batch_df = spark.createDataFrame(rows, schema=schema).withColumn(
            "kafka_partition", F.col("kafka_partition").cast("int")
        )

        cfg = load()
        upsert_silver = make_upsert_silver(
            spark,
            cfg,
            silver_path=silver_path,
            dlq_path=dlq_path,
        )

        # Call 1: initializes Silver Delta table (3 unique valid rows) + 1 DLQ row (batch_id=0)
        upsert_silver(batch_df, 0)
        # Call 2: exact retry of batch_id=0 (Silver MERGE + DLQ txnVersion must both be idempotent)
        upsert_silver(batch_df, 0)
        # Call 3: new batch_id=1 carrying duplicate event_ids (Silver MERGE must insert 0 rows)
        upsert_silver(batch_df, 1)

        silver_df = spark.read.format("delta").load(silver_path)
        assert silver_df.schema["event_time"].dataType == TimestampType()
        assert silver_df.schema["processed_at"].dataType == TimestampType()
        assert silver_df.schema["spend"].dataType == DecimalType(18, 4)
        assert silver_df.schema["event_date"].dataType == DateType()

        stats = silver_df.agg(
            F.count("*").alias("total_rows"),
            F.countDistinct("event_id").alias("distinct_events"),
            F.sum("spend").alias("total_spend"),
        ).collect()[0]

        assert stats["total_rows"] == 3, f"Expected 3 rows, got {stats['total_rows']}"
        assert stats["distinct_events"] == 3
        assert stats["total_spend"] == Decimal("3.7500"), (
            f"Expected total_spend=3.7500, got {stats['total_spend']}"
        )

        dlq_rows = spark.read.format("delta").load(dlq_path).collect()
        assert len(dlq_rows) == 2, (
            f"Expected 2 Silver DLQ rows (batch 0 retry skipped + batch 1), got {len(dlq_rows)}"
        )
        assert dlq_rows[0]["stage"] == "silver"
        assert dlq_rows[0]["error_type"] == "negative_amount"
    finally:
        spark.stop()


@_mark_spark
def test_upsert_silver_idempotent_and_deduplicates(tmp_path: Path) -> None:
    """Verify upsert_silver runs idempotently across repeated calls on same batch."""
    assert pytest is not None
    pytest.importorskip("pyspark")
    _run_upsert_silver_idempotency_check(tmp_path)


if __name__ == "__main__":
    test_refine_config_defaults()
    with tempfile.TemporaryDirectory() as tmp:
        _run_upsert_silver_idempotency_check(Path(tmp))
    print("PASS: test_refine upsert_silver idempotent MERGE, DECIMAL(18,4), and Silver DLQ")
