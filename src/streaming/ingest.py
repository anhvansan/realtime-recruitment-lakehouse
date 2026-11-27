"""Q1 Ingest Structured Streaming job: Redpanda -> Bronze & DLQ (Task 3.3).

Reads raw JSON events from topic ``cpc_events_raw``, validates every record against
the ``cpc_event.v1`` contract via ``validate_df()``, and routes rows in ``foreachBatch``:
  - Valid events -> Bronze Delta table (append, partitioned by ``event_date``,
    idempotent via ``txnAppId`` + ``txnVersion=batch_id``).
  - Invalid events -> DLQ Delta table (idempotent via ``txnAppId`` + ``txnVersion``)
    and Kafka topic ``cpc_events_dlq`` (at-least-once).
"""

from __future__ import annotations

import argparse
import logging
import signal
import sys
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from src.common.config import AppConfig, load
from src.streaming.spark_session import build_spark
from src.streaming.validation import validate_df

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.streaming.query import StreamingQuery

logger = logging.getLogger(__name__)


def read_kafka_raw_stream(
    spark: SparkSession,
    cfg: AppConfig,
    *,
    starting_offsets: str = "earliest",
) -> DataFrame:
    """Create Kafka Structured Streaming DataFrame from ``cpc_events_raw``."""
    from pyspark.sql import functions as F

    streaming_cfg = cfg.pipeline.streaming
    raw_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", cfg.kafka_bootstrap_internal)
        .option("subscribe", cfg.pipeline.kafka.raw_topic)
        .option("startingOffsets", starting_offsets)
        .option("maxOffsetsPerTrigger", str(streaming_cfg.max_offsets_per_trigger))
        .option("failOnDataLoss", str(streaming_cfg.fail_on_data_loss).lower())
        .load()
    )

    return raw_df.select(
        F.col("value").cast("string").alias("payload_raw"),
        F.col("topic").cast("string").alias("kafka_topic"),
        F.col("partition").cast("int").alias("kafka_partition"),
        F.col("offset").cast("long").alias("kafka_offset"),
        F.col("timestamp").cast("timestamp").alias("kafka_timestamp"),
    )


def publish_to_kafka_dlq(
    invalid_df: DataFrame,
    bootstrap_servers: str,
    dlq_topic: str,
) -> None:
    """Publish invalid DLQ records to Kafka DLQ topic (at-least-once delivery)."""
    from pyspark.sql import functions as F

    dlq_kafka_df = invalid_df.select(
        F.concat_ws(
            ":",
            F.coalesce(F.col("source_topic"), F.lit("")),
            F.coalesce(F.col("source_partition").cast("string"), F.lit("-1")),
            F.coalesce(F.col("source_offset").cast("string"), F.lit("-1")),
        ).alias("key"),
        F.to_json(F.struct(*[F.col(col_name) for col_name in invalid_df.columns])).alias("value"),
    )

    (
        dlq_kafka_df.write.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("topic", dlq_topic)
        .save()
    )


def make_process_batch(
    cfg: AppConfig,
    *,
    bronze_path: str | None = None,
    dlq_path: str | None = None,
    publish_kafka_dlq: bool = True,
) -> Callable[[DataFrame, int], None]:
    """Return a ``foreachBatch`` handler that routes valid->Bronze and invalid->DLQ."""
    target_bronze = bronze_path or cfg.s3a_uri(cfg.pipeline.storage.bronze_path)
    target_dlq = dlq_path or cfg.s3a_uri(cfg.pipeline.storage.dlq_path)
    dlq_topic = cfg.pipeline.kafka.dlq_topic
    bootstrap = cfg.kafka_bootstrap_internal

    def process_batch(batch_df: DataFrame, batch_id: int) -> None:
        batch_df.persist()
        try:
            if batch_df.isEmpty():
                return

            valid_df, invalid_df = validate_df(batch_df)
            valid_count = valid_df.count()
            invalid_count = invalid_df.count()

            if valid_count > 0:
                (
                    valid_df.write.format("delta")
                    .mode("append")
                    .option("txnAppId", "ingest_bronze")
                    .option("txnVersion", str(batch_id))
                    .partitionBy("event_date")
                    .save(target_bronze)
                )

            if invalid_count > 0:
                (
                    invalid_df.write.format("delta")
                    .mode("append")
                    .option("txnAppId", "ingest_dlq")
                    .option("txnVersion", str(batch_id))
                    .save(target_dlq)
                )
                if publish_kafka_dlq:
                    publish_to_kafka_dlq(invalid_df, bootstrap, dlq_topic)

            logger.info(
                "Q1 ingest batch_id=%s valid=%s invalid=%s",
                batch_id,
                valid_count,
                invalid_count,
            )
        finally:
            batch_df.unpersist()

    return process_batch


def start_ingest_query(
    spark: SparkSession,
    cfg: AppConfig,
    *,
    available_now: bool = False,
    starting_offsets: str = "earliest",
    publish_kafka_dlq: bool = True,
) -> StreamingQuery:
    """Start the Q1 Structured Streaming query and return the StreamingQuery handle."""
    raw_stream = read_kafka_raw_stream(spark, cfg, starting_offsets=starting_offsets)
    checkpoint_uri = cfg.s3a_uri(cfg.pipeline.storage.checkpoints.ingest_bronze)
    process_batch = make_process_batch(cfg, publish_kafka_dlq=publish_kafka_dlq)

    writer = (
        raw_stream.writeStream.queryName("q1_ingest_bronze_dlq")
        .foreachBatch(process_batch)
        .option("checkpointLocation", checkpoint_uri)
    )

    if available_now:
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime=cfg.pipeline.streaming.trigger_interval)

    return writer.start()


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run Q1 Ingest Structured Streaming job (Kafka -> Bronze & DLQ)."
    )
    parser.add_argument(
        "--available-now",
        action="store_true",
        help="Process all currently available Kafka offsets and stop cleanly.",
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
    """CLI entrypoint for Q1 Ingest streaming job."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    args = _parse_args(argv)
    cfg = load()
    spark = build_spark("q1_ingest_bronze_dlq", cfg=cfg, log_level=args.log_level)

    query = start_ingest_query(
        spark,
        cfg,
        available_now=args.available_now,
        starting_offsets=args.starting_offsets,
        publish_kafka_dlq=not args.no_kafka_dlq,
    )

    def _stop_handler(_signum: int, _frame: Any) -> None:
        logger.info("Received stop signal; stopping Q1 ingest query...")
        if query.isActive:
            query.stop()

    prev_sigint = signal.getsignal(signal.SIGINT)
    prev_sigterm = signal.getsignal(signal.SIGTERM)
    signal.signal(signal.SIGINT, _stop_handler)
    signal.signal(signal.SIGTERM, _stop_handler)

    try:
        query.awaitTermination()
    finally:
        signal.signal(signal.SIGINT, prev_sigint)
        signal.signal(signal.SIGTERM, prev_sigterm)
        spark.stop()

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
