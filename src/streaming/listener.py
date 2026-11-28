"""Structured Streaming observability listener (Task 3.9).

Captures ``StreamingQueryProgress`` events from both Q1 (``ingest_bronze``) and
Q2 (``refine_silver``), writes:
  1. Live snapshot JSON to ``s3://<bucket>/ops/latest.json`` via boto3 (for Dashboard Panel 4).
  2. Buffered historical metrics to Delta table ``s3a://<bucket>/ops/stream_metrics``
     periodically (~every 60s) and on ``flush()`` to avoid generating small files every batch.

Rule R6: Never logs or persists S3 credentials anywhere.
"""

from __future__ import annotations

import json
import logging
import math
import threading
import time
from collections.abc import Callable
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from src.common.config import AppConfig

if TYPE_CHECKING:
    from pyspark.sql import SparkSession
    from pyspark.sql.streaming.listener import StreamingQueryListener as _BaseListener
else:
    try:
        from pyspark.sql.streaming.listener import StreamingQueryListener as _BaseListener
    except ImportError:  # pragma: no cover - allows CI/host runs without pyspark
        _BaseListener = object

logger = logging.getLogger(__name__)
UTC = timezone.utc


def _safe_float(value: Any) -> float:
    """Convert numeric rate to finite float, defaulting None/NaN/Inf to 0.0."""
    if value is None:
        return 0.0
    try:
        num = float(value)
    except (TypeError, ValueError):
        return 0.0
    return num if math.isfinite(num) else 0.0


def parse_query_progress(progress: Any) -> dict[str, Any]:
    """Extract canonical metrics dictionary from a Spark StreamingQueryProgress payload."""
    if isinstance(progress, str):
        data: dict[str, Any] = json.loads(progress)
    elif isinstance(progress, dict):
        data = progress
    else:
        raw_json: Any = getattr(progress, "json", None)
        if isinstance(raw_json, str):
            data = json.loads(raw_json)
        elif callable(raw_json):
            data = json.loads(str(raw_json()))
        else:
            raise TypeError(f"Unsupported progress object type: {type(progress).__name__}")

    duration_ms = data.get("durationMs") or {}
    if isinstance(duration_ms, dict):
        if "triggerExecution" in duration_ms:
            batch_duration_ms = int(duration_ms.get("triggerExecution") or 0)
        else:
            batch_duration_ms = sum(int(v or 0) for v in duration_ms.values())
    else:
        batch_duration_ms = 0

    event_time = data.get("eventTime") or {}
    watermark_val = event_time.get("watermark") if isinstance(event_time, dict) else None
    event_time_watermark = str(watermark_val) if watermark_val else None

    state_ops = data.get("stateOperators") or []
    state_num_rows_total = sum(int(op.get("numRowsTotal") or 0) for op in state_ops)
    state_memory_used_bytes = sum(int(op.get("memoryUsedBytes") or 0) for op in state_ops)

    query_id = str(data.get("id") or "")
    query_name = str(data.get("name") or query_id or "unknown")

    return {
        "query_name": query_name,
        "query_id": query_id,
        "batch_id": int(data.get("batchId") or 0),
        "timestamp": str(data.get("timestamp") or ""),
        "input_rows": int(data.get("numInputRows") or 0),
        "input_rows_per_second": _safe_float(data.get("inputRowsPerSecond")),
        "processed_rows_per_second": _safe_float(data.get("processedRowsPerSecond")),
        "batch_duration_ms": batch_duration_ms,
        "event_time_watermark": event_time_watermark,
        "state_num_rows_total": state_num_rows_total,
        "state_memory_used_bytes": state_memory_used_bytes,
    }


def _default_s3_latest_writer(cfg: AppConfig, key: str) -> Callable[[dict[str, Any]], None]:
    """Return a boto3 writer updating ``s3://<bucket>/<key>`` with JSON payload."""
    import boto3
    from botocore.client import Config as BotoConfig

    def _write(payload: dict[str, Any]) -> None:
        s3 = boto3.client(
            "s3",
            endpoint_url=cfg.s3_endpoint,
            aws_access_key_id=cfg.s3_access_key,
            aws_secret_access_key=cfg.s3_secret_key,
            region_name=cfg.s3_region,
            config=BotoConfig(s3={"addressing_style": "path"}, signature_version="s3v4"),
        )
        body = json.dumps(payload, indent=2, sort_keys=False).encode("utf-8")
        s3.put_object(
            Bucket=cfg.s3_bucket,
            Key=key,
            Body=body,
            ContentType="application/json",
        )

    return _write


def _default_delta_metrics_writer(
    spark: SparkSession, metrics_uri: str
) -> Callable[[list[dict[str, Any]]], None]:
    """Return a Spark Delta append writer for ``ops/stream_metrics`` with explicit schema."""
    from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

    schema = StructType(
        [
            StructField("query_name", StringType(), False),
            StructField("query_id", StringType(), False),
            StructField("batch_id", LongType(), False),
            StructField("timestamp", StringType(), False),
            StructField("input_rows", LongType(), False),
            StructField("input_rows_per_second", DoubleType(), False),
            StructField("processed_rows_per_second", DoubleType(), False),
            StructField("batch_duration_ms", LongType(), False),
            StructField("event_time_watermark", StringType(), True),
            StructField("state_num_rows_total", LongType(), False),
            StructField("state_memory_used_bytes", LongType(), False),
        ]
    )

    def _write(records: list[dict[str, Any]]) -> None:
        if not records:
            return
        rows = [
            (
                str(r["query_name"]),
                str(r["query_id"]),
                int(r["batch_id"]),
                str(r["timestamp"]),
                int(r["input_rows"]),
                float(r["input_rows_per_second"]),
                float(r["processed_rows_per_second"]),
                int(r["batch_duration_ms"]),
                r["event_time_watermark"],
                int(r["state_num_rows_total"]),
                int(r["state_memory_used_bytes"]),
            )
            for r in records
        ]
        df = spark.createDataFrame(rows, schema=schema)
        df.write.format("delta").mode("append").save(metrics_uri)

    return _write


class MetricsListener(_BaseListener):
    """StreamingQueryListener writing live ``ops/latest.json`` and buffered Delta metrics."""

    def __init__(
        self,
        cfg: AppConfig,
        spark: SparkSession | None = None,
        *,
        flush_interval_sec: float = 60.0,
        latest_key: str = "ops/latest.json",
        metrics_path: str | None = None,
        s3_writer: Callable[[dict[str, Any]], None] | None = None,
        delta_writer: Callable[[list[dict[str, Any]]], None] | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.cfg = cfg
        self.flush_interval_sec = flush_interval_sec
        self.latest_key = latest_key
        self.metrics_uri = metrics_path or cfg.s3a_uri(cfg.pipeline.storage.ops_path)
        self._clock = clock
        self._lock = threading.Lock()
        self._latest_by_query: dict[str, dict[str, Any]] = {}
        self._buffer: list[dict[str, Any]] = []
        self._last_flush_ts: float = clock()

        self._s3_writer = s3_writer or _default_s3_latest_writer(cfg, latest_key)
        if delta_writer is not None:
            self._delta_writer = delta_writer
        elif spark is not None:
            self._delta_writer = _default_delta_metrics_writer(spark, self.metrics_uri)
        else:
            self._delta_writer = None

    def onQueryStarted(self, event: Any) -> None:  # noqa: N802
        """Log query startup non-blockingly."""
        try:
            name = getattr(event, "name", None) or getattr(event, "id", "unknown")
            logger.info("Streaming query started: %s", name)
        except Exception as exc:  # noqa: BLE001
            logger.warning("MetricsListener.onQueryStarted ignored error: %s", exc)

    def onQueryIdle(self, event: Any) -> None:  # noqa: N802
        """No-op handler required by PySpark 3.4+ StreamingQueryListener ABC."""

    def onQueryTerminated(self, event: Any) -> None:  # noqa: N802
        """Log query termination without raising into Spark driver."""
        try:
            qid = getattr(event, "id", "unknown")
            exc = getattr(event, "exception", None)
            if exc:
                logger.warning("Streaming query terminated with error id=%s: %s", qid, exc)
            else:
                logger.info("Streaming query terminated cleanly id=%s", qid)
        except Exception as err:  # noqa: BLE001
            logger.warning("MetricsListener.onQueryTerminated ignored error: %s", err)

    def onQueryProgress(self, event: Any) -> None:  # noqa: N802
        """Record progress, update ``ops/latest.json``, and flush Delta every ~60s."""
        try:
            record = parse_query_progress(event.progress)
        except Exception as exc:  # noqa: BLE001
            logger.warning("MetricsListener failed to parse QueryProgressEvent: %s", exc)
            return

        now = self._clock()
        records_to_flush: list[dict[str, Any]] = []

        with self._lock:
            self._latest_by_query[record["query_name"]] = record
            self._buffer.append(record)
            latest_snapshot = {
                "updated_at": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "queries": dict(self._latest_by_query),
            }
            if now - self._last_flush_ts >= self.flush_interval_sec and self._buffer:
                records_to_flush = list(self._buffer)
                self._buffer.clear()
                self._last_flush_ts = now

        try:
            self._s3_writer(latest_snapshot)
        except Exception as exc:  # noqa: BLE001
            logger.warning("MetricsListener failed to write %s: %s", self.latest_key, exc)

        if records_to_flush:
            self._write_delta_batch(records_to_flush)

    def flush(self) -> int:
        """Flush any buffered progress records into ``ops/stream_metrics`` Delta table."""
        with self._lock:
            if not self._buffer:
                return 0
            records_to_flush = list(self._buffer)
            self._buffer.clear()
            self._last_flush_ts = self._clock()

        self._write_delta_batch(records_to_flush)
        return len(records_to_flush)

    def _write_delta_batch(self, records: list[dict[str, Any]]) -> None:
        """Safely write buffered metric rows to Delta without crashing streaming queries."""
        if self._delta_writer is None or not records:
            return
        try:
            self._delta_writer(records)
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "MetricsListener failed to append %d metric rows to %s: %s",
                len(records),
                self.metrics_uri,
                exc,
            )
