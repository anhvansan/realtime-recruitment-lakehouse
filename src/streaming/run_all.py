"""Single-JVM Structured Streaming orchestrator running Q1 Ingest + Q2 Refine (Task 3.6).

Runs both Q1 (Kafka -> Bronze/DLQ) and Q2 (Bronze -> Silver MERGE) inside one
``SparkSession`` (local[2], 1536m driver heap within mem_limit: 2304m) to honor the
16GB RAM budget defined in Section 4.1 & 4.4 of PROJECT_BACKLOG.md.
"""

from __future__ import annotations

import argparse
import logging
import signal
import sys
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any

# Ensure project root is importable when invoked directly as a script inside container
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.common.config import AppConfig, load  # noqa: E402
from src.streaming.ingest import start_ingest_query  # noqa: E402
from src.streaming.listener import MetricsListener  # noqa: E402
from src.streaming.refine import start_refine_query  # noqa: E402
from src.streaming.spark_session import build_spark  # noqa: E402

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def ensure_bronze_table_exists(spark: SparkSession, cfg: AppConfig) -> None:
    """Create an empty Bronze Delta table if missing so Q2 can subscribe immediately."""
    from delta.tables import DeltaTable  # type: ignore[import-not-found]
    from pyspark.sql.types import (
        DateType,
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    bronze_uri = cfg.s3a_uri(cfg.pipeline.storage.bronze_path)
    if DeltaTable.isDeltaTable(spark, bronze_uri):
        return

    bronze_schema = StructType(
        [
            StructField("schema_version", StringType(), True),
            StructField("event_id", StringType(), True),
            StructField("event_time", StringType(), True),
            StructField("produced_at", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("job_id", StringType(), True),
            StructField("campaign_id", StringType(), True),
            StructField("group_id", StringType(), True),
            StructField("publisher_id", StringType(), True),
            StructField("bid", StringType(), True),
            StructField("spend", StringType(), True),
            StructField("source_row_id", LongType(), True),
            StructField("payload_raw", StringType(), True),
            StructField("kafka_topic", StringType(), True),
            StructField("kafka_partition", IntegerType(), True),
            StructField("kafka_offset", LongType(), True),
            StructField("kafka_timestamp", TimestampType(), True),
            StructField("ingested_at", TimestampType(), True),
            StructField("event_date", DateType(), True),
        ]
    )
    empty_df = spark.createDataFrame([], schema=bronze_schema)
    empty_df.write.format("delta").mode("ignore").partitionBy("event_date").save(bronze_uri)
    logger.info("Initialized empty Bronze Delta table at %s", bronze_uri)


def run_streaming_pipeline(
    spark: SparkSession,
    cfg: AppConfig,
    *,
    available_now: bool = False,
    starting_offsets: str = "earliest",
    publish_kafka_dlq: bool = True,
) -> None:
    """Start Q1 Ingest and Q2 Refine inside one SparkSession and wait cleanly."""
    ensure_bronze_table_exists(spark, cfg)
    listener = MetricsListener(cfg=cfg, spark=spark)
    spark.streams.addListener(listener)

    try:
        if available_now:
            logger.info("Starting Q1 Ingest (availableNow=True)...")
            q1 = start_ingest_query(
                spark,
                cfg,
                available_now=True,
                starting_offsets=starting_offsets,
                publish_kafka_dlq=publish_kafka_dlq,
            )
            q1.awaitTermination()

            logger.info("Starting Q2 Refine (availableNow=True)...")
            q2 = start_refine_query(spark, cfg, available_now=True)
            q2.awaitTermination()
            return

        q1 = start_ingest_query(
            spark,
            cfg,
            available_now=False,
            starting_offsets=starting_offsets,
            publish_kafka_dlq=publish_kafka_dlq,
        )
        q2 = start_refine_query(spark, cfg, available_now=False)
        queries = (q1, q2)
        logger.info("Started Q1 (%s) and Q2 (%s) in single SparkSession.", q1.name, q2.name)

        def _stop_all(_signum: int, _frame: Any) -> None:
            logger.info("Received stop signal; stopping Q1 and Q2 queries...")
            for query in queries:
                if query.isActive:
                    query.stop()

        prev_sigint = signal.getsignal(signal.SIGINT)
        prev_sigterm = signal.getsignal(signal.SIGTERM)
        signal.signal(signal.SIGINT, _stop_all)
        signal.signal(signal.SIGTERM, _stop_all)

        try:
            spark.streams.awaitAnyTermination()
        finally:
            for query in queries:
                if query.isActive:
                    query.stop()
            signal.signal(signal.SIGINT, prev_sigint)
            signal.signal(signal.SIGTERM, prev_sigterm)
    finally:
        # Allow async QueryProgressEvents from final batch to arrive before flushing
        time.sleep(0.25)
        flushed = listener.flush()
        spark.streams.removeListener(listener)
        logger.info("Flushed %d buffered streaming metric rows to Delta.", flushed)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run Q1 Ingest + Q2 Refine streaming pipeline in 1 Spark JVM."
    )
    parser.add_argument(
        "--available-now",
        action="store_true",
        help="Drain all available offsets in Q1 then Q2 sequentially and exit.",
    )
    parser.add_argument(
        "--starting-offsets",
        type=str,
        default="earliest",
        help="Kafka startingOffsets on initial run without checkpoint (default: earliest).",
    )
    parser.add_argument(
        "--no-kafka-dlq",
        action="store_true",
        help="Skip publishing invalid records to cpc_events_dlq Kafka topic.",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="WARN",
        help="Spark driver log level (default: WARN).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint for unified Q1 + Q2 Spark Structured Streaming runner."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    args = _parse_args(argv)
    cfg = load()
    spark = build_spark("cpc_lakehouse_streaming", cfg=cfg, log_level=args.log_level)

    try:
        run_streaming_pipeline(
            spark,
            cfg,
            available_now=args.available_now,
            starting_offsets=args.starting_offsets,
            publish_kafka_dlq=not args.no_kafka_dlq,
        )
    finally:
        spark.stop()

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
