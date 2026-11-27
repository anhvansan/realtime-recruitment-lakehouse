"""Spark Structured Streaming package for CPC AdTech Lakehouse."""

from src.streaming.ingest import (
    make_process_batch,
    publish_to_kafka_dlq,
    read_kafka_raw_stream,
    start_ingest_query,
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
    "list_s3a_paths",
    "make_process_batch",
    "publish_to_kafka_dlq",
    "read_kafka_raw_stream",
    "redact_spark_conf",
    "start_ingest_query",
    "validate_df",
]
