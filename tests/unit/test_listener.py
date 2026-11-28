"""Host unit tests for Task 3.9 — StreamingQueryListener MetricsListener."""

from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any

from src.common.config import load
from src.streaming.listener import MetricsListener, parse_query_progress


def _sample_progress_payload(
    *,
    name: str = "refine_silver",
    batch_id: int = 3,
    input_rows: int = 250,
    with_state: bool = True,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "id": "11111111-2222-3333-4444-555555555555",
        "name": name,
        "batchId": batch_id,
        "timestamp": "2026-01-01T00:10:00.000Z",
        "numInputRows": input_rows,
        "inputRowsPerSecond": 25.5,
        "processedRowsPerSecond": 120.75,
        "durationMs": {
            "addBatch": 450,
            "getBatch": 12,
            "triggerExecution": 510,
        },
        "eventTime": {"watermark": "2026-01-01T00:05:00.000Z"} if with_state else {},
        "stateOperators": (
            [
                {"numRowsTotal": 1200, "memoryUsedBytes": 65536},
                {"numRowsTotal": 50, "memoryUsedBytes": 4096},
            ]
            if with_state
            else []
        ),
    }
    return payload


def test_parse_query_progress_with_state_and_watermark() -> None:
    """Extract all 11 metrics accurately including summed stateOperators and watermark."""
    raw = json.dumps(_sample_progress_payload(with_state=True))
    fake_progress = SimpleNamespace(json=raw)
    parsed = parse_query_progress(fake_progress)

    assert parsed == {
        "query_name": "refine_silver",
        "query_id": "11111111-2222-3333-4444-555555555555",
        "batch_id": 3,
        "timestamp": "2026-01-01T00:10:00.000Z",
        "input_rows": 250,
        "input_rows_per_second": 25.5,
        "processed_rows_per_second": 120.75,
        "batch_duration_ms": 510,
        "event_time_watermark": "2026-01-01T00:05:00.000Z",
        "state_num_rows_total": 1250,
        "state_memory_used_bytes": 69632,
    }


def test_parse_query_progress_without_state_and_nan_rates() -> None:
    """Handle stateless query progress with empty stateOperators and NaN rates."""
    payload = _sample_progress_payload(name="ingest_bronze", batch_id=0, with_state=False)
    payload["inputRowsPerSecond"] = float("nan")
    payload["processedRowsPerSecond"] = None

    parsed = parse_query_progress(payload)
    assert parsed["query_name"] == "ingest_bronze"
    assert parsed["event_time_watermark"] is None
    assert parsed["state_num_rows_total"] == 0
    assert parsed["state_memory_used_bytes"] == 0
    assert parsed["input_rows_per_second"] == 0.0
    assert parsed["processed_rows_per_second"] == 0.0


def test_metrics_listener_buffers_until_interval_and_flushes_on_demand() -> None:
    """Verify latest.json writes every batch while Delta flushes only at >= 60s or flush()."""
    now = [100.0]
    s3_writes: list[dict[str, Any]] = []
    delta_flushes: list[list[dict[str, Any]]] = []

    cfg = load()
    listener = MetricsListener(
        cfg,
        flush_interval_sec=60.0,
        s3_writer=s3_writes.append,
        delta_writer=delta_flushes.append,
        clock=lambda: now[0],
    )

    # Batch at t=110s (< 60s elapsed): updates latest.json, buffers Delta
    now[0] = 110.0
    listener.onQueryProgress(
        SimpleNamespace(
            progress=_sample_progress_payload(name="ingest_bronze", batch_id=0, with_state=False)
        )
    )
    assert len(s3_writes) == 1
    assert "ingest_bronze" in s3_writes[-1]["queries"]
    assert len(delta_flushes) == 0

    # Batch at t=140s (< 60s elapsed): updates latest.json with both queries, still buffered
    now[0] = 140.0
    listener.onQueryProgress(
        SimpleNamespace(
            progress=_sample_progress_payload(name="refine_silver", batch_id=0, with_state=True)
        )
    )
    assert len(s3_writes) == 2
    assert set(s3_writes[-1]["queries"].keys()) == {"ingest_bronze", "refine_silver"}
    assert len(delta_flushes) == 0

    # Batch at t=165s (>= 60s since 100.0): triggers periodic Delta flush of all 3 rows
    now[0] = 165.0
    listener.onQueryProgress(
        SimpleNamespace(
            progress=_sample_progress_payload(name="refine_silver", batch_id=1, with_state=True)
        )
    )
    assert len(delta_flushes) == 1
    assert len(delta_flushes[0]) == 3

    # One more batch at t=175s, then explicit flush() on shutdown
    now[0] = 175.0
    listener.onQueryProgress(
        SimpleNamespace(
            progress=_sample_progress_payload(name="ingest_bronze", batch_id=1, with_state=False)
        )
    )
    assert len(delta_flushes) == 1
    flushed_count = listener.flush()
    assert flushed_count == 1
    assert len(delta_flushes) == 2
    assert delta_flushes[1][0]["batch_id"] == 1
    assert listener.flush() == 0


def test_metrics_listener_swallows_writer_errors() -> None:
    """Transient S3 or Delta writer failures must never crash the streaming query."""

    def _fail_s3(_payload: dict[str, Any]) -> None:
        raise RuntimeError("MinIO connection reset")

    def _fail_delta(_records: list[dict[str, Any]]) -> None:
        raise RuntimeError("Delta write failed")

    cfg = load()
    listener = MetricsListener(
        cfg,
        flush_interval_sec=0.0,
        s3_writer=_fail_s3,
        delta_writer=_fail_delta,
    )
    listener.onQueryStarted(SimpleNamespace(name="ingest_bronze", id="q1"))
    listener.onQueryProgress(
        SimpleNamespace(progress=_sample_progress_payload(name="ingest_bronze", batch_id=0))
    )
    listener.onQueryIdle(SimpleNamespace(id="q1"))
    listener.onQueryTerminated(SimpleNamespace(id="q1", exception=None))
    assert listener.flush() == 0
