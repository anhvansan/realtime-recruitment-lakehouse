"""Spark Structured Streaming package for CPC AdTech Lakehouse."""

from src.streaming.ingest import (
    make_process_batch,
    publish_to_kafka_dlq,
    read_kafka_raw_stream,
    start_ingest_query,
)
from src.streaming.refine import (
    make_upsert_silver,
    read_bronze_delta_stream,
    start_refine_query,
    transform_bronze_to_silver,
)
from src.streaming.run_all import (
    ensure_bronze_table_exists,
    run_streaming_pipeline,
)
from src.streaming.spark_session import (
    build_spark,
    build_spark_conf,
    list_s3a_paths,
    redact_spark_conf,
)
from src.streaming.validation import validate_df

__all__ = [
    "build_spark",
    "build_spark_conf",
    "ensure_bronze_table_exists",
    "list_s3a_paths",
    "make_process_batch",
    "make_upsert_silver",
    "publish_to_kafka_dlq",
    "read_bronze_delta_stream",
    "read_kafka_raw_stream",
    "redact_spark_conf",
    "run_streaming_pipeline",
    "start_ingest_query",
    "start_refine_query",
    "transform_bronze_to_silver",
    "validate_df",
]
