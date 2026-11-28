"""Q2 Refine Structured Streaming job: Bronze Delta -> Silver Delta (Task 3.4).

Reads append-only Bronze Delta events, normalizes types (UTC TIMESTAMP, DECIMAL(18,4),
BIGINT, DATE), applies two-layer deduplication:
  1. Stateful stream deduplication bounded by a 10-minute watermark
     (``withWatermark`` + ``dropDuplicatesWithinWatermark(["event_id"])``).
  2. Global idempotent Delta ``MERGE`` insert-only on ``t.event_id = s.event_id
     AND t.event_date = s.event_date`` (partition pruning).

Any record failing Silver type casting/contract checks is routed to DLQ Delta with
``stage = 'silver'`` using ``txnAppId="refine_dlq"`` and ``txnVersion=batch_id``.
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

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.streaming.query import StreamingQuery

logger = logging.getLogger(__name__)

SILVER_COLUMNS: tuple[str, ...] = (
    "schema_version",
    "event_id",
    "event_time",
    "produced_at",
    "event_type",
    "job_id",
    "campaign_id",
    "group_id",
    "publisher_id",
    "bid",
    "spend",
    "source_row_id",
    "event_date",
    "processed_at",
)


def read_bronze_delta_stream(
    spark: SparkSession,
    cfg: AppConfig,
    *,
    bronze_path: str | None = None,
    max_files_per_trigger: int | None = None,
) -> DataFrame:
    """Read Bronze Delta table as a stream with TIMESTAMP ``event_time`` for watermarking."""
    from pyspark.sql import functions as F

    bronze_uri = bronze_path or cfg.s3a_uri(cfg.pipeline.storage.bronze_path)
    max_files = str(
        max_files_per_trigger
        if max_files_per_trigger is not None
        else cfg.pipeline.streaming.max_files_per_trigger
    )

    bronze_df = (
        spark.readStream.format("delta")
        .option("maxFilesPerTrigger", max_files)
        .load(bronze_uri)
    )
    return bronze_df.withColumn("event_time", F.expr("try_cast(event_time as timestamp)"))


def transform_bronze_to_silver(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Normalize Bronze rows into Silver schema and split invalid rows to DLQ (stage='silver')."""
    from pyspark.sql import functions as F

    cols = set(df.columns)
    event_time_ts = F.expr("try_cast(event_time as timestamp)")
    produced_at_ts = F.expr("try_cast(produced_at as timestamp)")
    bid_dec = F.when(F.col("bid").isNull(), F.lit(None).cast("decimal(18,4)")).otherwise(
        F.expr("try_cast(bid as decimal(18,4))")
    )
    spend_dec = F.expr("try_cast(spend as decimal(18,4))")
    source_row_id_num = F.expr("try_cast(source_row_id as bigint)")
    event_date_col = (
        F.coalesce(F.expr("try_cast(event_date as date)"), F.to_date(event_time_ts))
        if "event_date" in cols
        else F.to_date(event_time_ts)
    )

    payload_raw_col = (
        F.col("payload_raw").cast("string")
        if "payload_raw" in cols
        else F.to_json(F.struct(*[F.col(c) for c in df.columns]))
    )
    kafka_topic_col = (
        F.col("kafka_topic").cast("string")
        if "kafka_topic" in cols
        else F.lit(None).cast("string")
    )
    kafka_partition_col = (
        F.col("kafka_partition").cast("int")
        if "kafka_partition" in cols
        else F.lit(None).cast("int")
    )
    kafka_offset_col = (
        F.col("kafka_offset").cast("long")
        if "kafka_offset" in cols
        else F.lit(None).cast("long")
    )

    missing_req = (
        F.col("event_id").isNull()
        | (F.trim(F.col("event_id").cast("string")) == F.lit(""))
        | F.col("event_type").isNull()
        | (F.trim(F.col("event_type").cast("string")) == F.lit(""))
        | F.col("job_id").isNull()
        | (F.trim(F.col("job_id").cast("string")) == F.lit(""))
        | F.col("campaign_id").isNull()
        | (F.trim(F.col("campaign_id").cast("string")) == F.lit(""))
        | F.col("publisher_id").isNull()
        | (F.trim(F.col("publisher_id").cast("string")) == F.lit(""))
        | source_row_id_num.isNull()
        | ((F.col("event_type") == F.lit("click")) & F.col("bid").isNull())
    )
    bad_ts = event_time_ts.isNull() | produced_at_ts.isNull() | event_date_col.isNull()
    bad_dec = spend_dec.isNull() | (F.col("bid").isNotNull() & bid_dec.isNull())
    neg_amt = (
        (spend_dec < F.lit(0))
        | (bid_dec.isNotNull() & (bid_dec < F.lit(0)))
        | (source_row_id_num < F.lit(0))
    )

    error_type_col = (
        F.when(missing_req, F.lit("missing_required"))
        .when(bad_ts, F.lit("bad_timestamp"))
        .when(bad_dec, F.lit("malformed_decimal"))
        .when(neg_amt, F.lit("negative_amount"))
        .otherwise(F.lit(None).cast("string"))
    )
    error_detail_col = (
        F.when(missing_req, F.lit("Missing required Silver field"))
        .when(bad_ts, F.lit("Failed TIMESTAMP/DATE normalization in Silver"))
        .when(bad_dec, F.lit("Failed DECIMAL(18,4) cast in Silver"))
        .when(neg_amt, F.lit("Negative monetary or source_row_id value in Silver"))
        .otherwise(F.lit(None).cast("string"))
    )

    enriched = (
        df.withColumn("_event_time_ts", event_time_ts)
        .withColumn("_produced_at_ts", produced_at_ts)
        .withColumn("_bid_dec", bid_dec)
        .withColumn("_spend_dec", spend_dec)
        .withColumn("_source_row_id_num", source_row_id_num)
        .withColumn("_event_date", event_date_col)
        .withColumn("_payload_raw_dlq", payload_raw_col)
        .withColumn("_source_topic", kafka_topic_col)
        .withColumn("_source_partition", kafka_partition_col)
        .withColumn("_source_offset", kafka_offset_col)
        .withColumn("_error_type", error_type_col)
        .withColumn("_error_detail", error_detail_col)
    )

    schema_version_col = (
        F.col("schema_version").cast("string") if "schema_version" in cols else F.lit("v1")
    )
    group_id_col = F.col("group_id").cast("string") if "group_id" in cols else F.lit(None).cast("string")

    valid_df = enriched.filter(F.col("_error_type").isNull()).select(
        schema_version_col.alias("schema_version"),
        F.col("event_id").cast("string").alias("event_id"),
        F.col("_event_time_ts").alias("event_time"),
        F.col("_produced_at_ts").alias("produced_at"),
        F.col("event_type").cast("string").alias("event_type"),
        F.col("job_id").cast("string").alias("job_id"),
        F.col("campaign_id").cast("string").alias("campaign_id"),
        group_id_col.alias("group_id"),
        F.col("publisher_id").cast("string").alias("publisher_id"),
        F.col("_bid_dec").alias("bid"),
        F.col("_spend_dec").alias("spend"),
        F.col("_source_row_id_num").alias("source_row_id"),
        F.col("_event_date").alias("event_date"),
        F.current_timestamp().alias("processed_at"),
    )

    invalid_df = enriched.filter(F.col("_error_type").isNotNull()).select(
        F.col("_payload_raw_dlq").alias("payload_raw"),
        F.col("_error_type").alias("error_type"),
        F.col("_error_detail").alias("error_detail"),
        F.col("_source_topic").alias("source_topic"),
        F.col("_source_partition").alias("source_partition"),
        F.col("_source_offset").alias("source_offset"),
        F.current_timestamp().alias("failed_at"),
        F.lit("silver").alias("stage"),
    )

    return valid_df, invalid_df


def make_upsert_silver(
    spark: SparkSession,
    cfg: AppConfig,
    *,
    silver_path: str | None = None,
    dlq_path: str | None = None,
) -> Callable[[DataFrame, int], None]:
    """Return a ``foreachBatch`` handler performing in-batch dedup + partition-pruned Delta MERGE."""
    target_silver = silver_path or cfg.s3a_uri(cfg.pipeline.storage.silver_path)
    target_dlq = dlq_path or cfg.s3a_uri(cfg.pipeline.storage.dlq_path)

    def upsert_silver(batch_df: DataFrame, batch_id: int) -> None:
        from delta.tables import DeltaTable  # type: ignore[import-not-found]

        batch_df.persist()
        try:
            if batch_df.isEmpty():
                return

            valid_df, invalid_df = transform_bronze_to_silver(batch_df)
            src_df = valid_df.dropDuplicates(["event_id"])

            valid_count = src_df.count()
            invalid_count = invalid_df.count()

            if valid_count > 0:
                if DeltaTable.isDeltaTable(spark, target_silver):
                    dt = DeltaTable.forPath(spark, target_silver)
                    (
                        dt.alias("t")
                        .merge(
                            src_df.alias("s"),
                            "t.event_id = s.event_id AND t.event_date = s.event_date",
                        )
                        .whenNotMatchedInsertAll()
                        .execute()
                    )
                else:
                    (
                        src_df.write.format("delta")
                        .mode("append")
                        .option("txnAppId", "refine_silver_init")
                        .option("txnVersion", str(batch_id))
                        .partitionBy("event_date")
                        .save(target_silver)
                    )

            if invalid_count > 0:
                (
                    invalid_df.write.format("delta")
                    .mode("append")
                    .option("txnAppId", "refine_dlq")
                    .option("txnVersion", str(batch_id))
                    .save(target_dlq)
                )

            logger.info(
                "Q2 refine batch_id=%s valid_deduped=%s invalid=%s",
                batch_id,
                valid_count,
                invalid_count,
            )
        finally:
            batch_df.unpersist()

    return upsert_silver


def start_refine_query(
    spark: SparkSession,
    cfg: AppConfig,
    *,
    available_now: bool = False,
    bronze_path: str | None = None,
    silver_path: str | None = None,
    dlq_path: str | None = None,
    checkpoint_path: str | None = None,
    watermark_delay: str | None = None,
    max_files_per_trigger: int | None = None,
    disable_stateful_dedup: bool = False,
    query_name: str = "refine_silver",
) -> StreamingQuery:
    """Start the Q2 Refine Structured Streaming query (Bronze -> Silver)."""
    bronze_stream = read_bronze_delta_stream(
        spark,
        cfg,
        bronze_path=bronze_path,
        max_files_per_trigger=max_files_per_trigger,
    )
    if disable_stateful_dedup:
        refined_stream = bronze_stream
    else:
        resolved_watermark = watermark_delay or cfg.pipeline.streaming.watermark_delay
        refined_stream = bronze_stream.withWatermark(
            "event_time", resolved_watermark
        ).dropDuplicatesWithinWatermark(["event_id"])

    checkpoint_uri = checkpoint_path or cfg.s3a_uri(cfg.pipeline.storage.checkpoints.refine_silver)
    upsert_fn = make_upsert_silver(
        spark,
        cfg,
        silver_path=silver_path,
        dlq_path=dlq_path,
    )

    writer = (
        refined_stream.writeStream.queryName(query_name)
        .foreachBatch(upsert_fn)
        .option("checkpointLocation", checkpoint_uri)
    )

    if available_now:
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime=cfg.pipeline.streaming.trigger_interval)

    return writer.start()


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run Q2 Refine Structured Streaming job (Bronze Delta -> Silver Delta)."
    )
    parser.add_argument(
        "--available-now",
        action="store_true",
        help="Process all available Bronze Delta commits and stop cleanly.",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="WARN",
        help="Spark driver log level (default: WARN).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint for Q2 Refine streaming job."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    args = _parse_args(argv)
    cfg = load()
    spark = build_spark("q2_refine_silver", cfg=cfg, log_level=args.log_level)

    query = start_refine_query(spark, cfg, available_now=args.available_now)

    def _stop_handler(_signum: int, _frame: Any) -> None:
        logger.info("Received stop signal; stopping Q2 refine query...")
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
