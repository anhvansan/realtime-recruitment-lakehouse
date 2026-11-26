"""Spark Structured Streaming package for CPC AdTech Lakehouse."""

from src.streaming.spark_session import (
    build_spark,
    build_spark_conf,
    list_s3a_paths,
    redact_spark_conf,
)

__all__ = [
    "build_spark",
    "build_spark_conf",
    "list_s3a_paths",
    "redact_spark_conf",
]
