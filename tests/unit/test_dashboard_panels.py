"""Unit tests for Streamlit dashboard panel helpers and S3 client caching (Tasks 5.3-5.7)."""

from __future__ import annotations

import io
import json
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pandas as pd

from src.common.config import AppConfig
from src.dashboard.panels.common import (
    classify_pacing_status,
    compute_funnel_summary,
    format_metric_or_na,
    format_percent_or_na,
    format_utc_timestamp,
    is_freshness_stale,
    pacing_status_style,
    sanitize_dlq_payload,
)
from src.dashboard.panels.funnel import format_gold_last_run_caption
from src.dashboard.panels.health import parse_ops_latest_rows
from src.dashboard.repository import DashboardRepository, DataSourcePaths


def test_format_metric_and_percent_return_na_for_missing_or_infinite() -> None:
    """Zero-denominator NULL/NaN/inf values must always render as 'n/a', never '0'."""
    for bad in (None, float("nan"), pd.NA, float("inf"), float("-inf")):
        assert format_metric_or_na(bad) == "n/a"
        assert format_metric_or_na(bad, prefix="$") == "n/a"
        assert format_percent_or_na(bad) == "n/a"

    assert format_metric_or_na(1234.5, ",.2f", prefix="$") == "$1,234.50"
    assert format_metric_or_na(42, ",d") == "42"
    assert format_metric_or_na(0, ",d") == "0"
    assert format_percent_or_na(0.25, decimals=1) == "25.0%"
    assert format_percent_or_na(0.0, decimals=2) == "0.00%"


def test_classify_pacing_status_and_styles_section_2_3() -> None:
    """Section 2.3 C1 pacing_ratio=1.6 -> fast (red), <0.6 -> slow (yellow), NULL -> na."""
    assert classify_pacing_status(1.6) == "fast"
    assert "#fde8e8" in pacing_status_style("fast")

    assert classify_pacing_status(0.5) == "slow"
    assert "#fef3c7" in pacing_status_style("slow")

    assert classify_pacing_status(1.0) == "normal"
    assert pacing_status_style("normal") == ""

    assert classify_pacing_status(None) == "na"
    assert classify_pacing_status(float("nan")) == "na"
    assert pacing_status_style("na") == ""


def test_is_freshness_stale_threshold_boundary() -> None:
    """Freshness > 120s or missing triggers stale alert; <= 120s stays healthy."""
    assert is_freshness_stale(120.0, threshold_s=120.0) is False
    assert is_freshness_stale(45.2, threshold_s=120.0) is False
    assert is_freshness_stale(120.1, threshold_s=120.0) is True
    assert is_freshness_stale(121, threshold_s=120.0) is True
    assert is_freshness_stale(None, threshold_s=120.0) is True
    assert is_freshness_stale(float("nan"), threshold_s=120.0) is True


def test_sanitize_dlq_payload_truncates_and_redacts_secrets() -> None:
    """DLQ payload sanitizer flattens whitespace, redacts secrets, and enforces max_len."""
    raw = (
        '{"event_id": "e1", "secret": "minioadmin123", '
        '"token": "tok_abc_999", "password": "pw1", "api_key": "sk-test"\n\t}'
    )
    cleaned = sanitize_dlq_payload(raw, max_len=200)
    assert "minioadmin123" not in cleaned
    assert "tok_abc_999" not in cleaned
    assert "pw1" not in cleaned
    assert "sk-test" not in cleaned
    assert "<redacted>" in cleaned
    assert "\n" not in cleaned
    assert "\t" not in cleaned

    long_text = "x" * 200
    truncated = sanitize_dlq_payload(long_text, max_len=140)
    assert len(truncated) == 140
    assert truncated.endswith("...")
    assert sanitize_dlq_payload(None) == ""
    assert sanitize_dlq_payload(pd.NA) == ""


def test_compute_funnel_summary_handles_zero_denominators_and_valid_totals() -> None:
    """Funnel summary returns None (displayed as 'n/a') when denominator is zero."""
    empty_summary = compute_funnel_summary(pd.DataFrame())
    assert empty_summary["clicks"] == 0
    assert empty_summary["cpqa"] is None
    assert format_metric_or_na(empty_summary["cpqa"]) == "n/a"

    zero_qual_df = pd.DataFrame(
        [{"clicks": 100, "applies": 0, "qualified": 0, "unqualified": 0, "spend": 50.0}]
    )
    zero_summary = compute_funnel_summary(zero_qual_df)
    assert zero_summary["click_to_apply_rate"] == 0.0
    assert zero_summary["qualified_rate"] is None
    assert zero_summary["cpa"] is None
    assert zero_summary["cpqa"] is None
    assert format_metric_or_na(zero_summary["cpqa"], prefix="$") == "n/a"
    assert format_percent_or_na(zero_summary["qualified_rate"]) == "n/a"

    valid_df = pd.DataFrame(
        [
            {"clicks": 120, "applies": 25, "qualified": 6, "unqualified": 19, "spend": 300.0},
            {"clicks": 80, "applies": 15, "qualified": 4, "unqualified": 11, "spend": 200.0},
        ]
    )
    valid_summary = compute_funnel_summary(valid_df)
    assert valid_summary["clicks"] == 200
    assert valid_summary["applies"] == 40
    assert valid_summary["qualified"] == 10
    assert valid_summary["spend"] == 500.0
    assert valid_summary["click_to_apply_rate"] == 0.2
    assert valid_summary["qualified_rate"] == 0.25
    assert valid_summary["cpc"] == 2.5
    assert valid_summary["cpa"] == 12.5
    assert valid_summary["cpqa"] == 50.0


def test_format_utc_timestamp() -> None:
    dt = datetime(2026, 4, 6, 17, 0, 42, tzinfo=timezone.utc)
    assert format_utc_timestamp(dt) == "2026-04-06 17:00:42 UTC"
    assert format_utc_timestamp(None) == "n/a"


def test_dashboard_repository_caches_s3_client(monkeypatch) -> None:
    """Repository initializes boto3 S3 client once and reuses it across metadata reads."""
    import boto3

    client_calls: list[dict] = []
    mock_s3 = MagicMock()

    def _fake_get_object(Bucket: str, Key: str) -> dict:
        payload = {"status": "ok", "key": Key, "bucket": Bucket}
        return {"Body": io.BytesIO(json.dumps(payload).encode("utf-8"))}

    mock_s3.get_object.side_effect = _fake_get_object

    def _fake_client(service_name: str, **kwargs):
        client_calls.append({"service_name": service_name, **kwargs})
        return mock_s3

    monkeypatch.setattr(boto3, "client", _fake_client)

    cfg = AppConfig()
    paths = DataSourcePaths(
        silver_path="unused",
        bronze_path="unused",
        dlq_path="unused",
        gold_efficiency_path="unused",
        budget_csv="unused",
        publisher_csv="unused",
    )
    repo = DashboardRepository(cfg=cfg, paths=paths)

    ops = repo.get_ops_latest()
    gold = repo.get_gold_last_run()

    assert len(client_calls) == 1
    assert ops is not None and ops["key"] == "ops/latest.json"
    assert gold is not None and gold["key"] == "gold/_last_run.json"
    assert mock_s3.get_object.call_count == 2


def test_parse_ops_latest_rows_unwraps_nested_queries_schema() -> None:
    """Panel 4 parses nested Task 3.9 ops/latest.json with ingest_bronze and refine_silver."""
    payload = {
        "updated_at": "2026-11-28T09:58:44Z",
        "queries": {
            "ingest_bronze": {
                "query_name": "ingest_bronze",
                "batch_id": 12,
                "timestamp": "2026-11-28T09:58:43Z",
                "input_rows": 0,
                "input_rows_per_second": 0.0,
                "processed_rows_per_second": 15.25,
                "batch_duration_ms": 420,
                "event_time_watermark": None,
                "state_num_rows_total": 0,
                "state_memory_used_bytes": 0,
            },
            "refine_silver": {
                "query_name": "refine_silver",
                "batch_id": 9,
                "timestamp": "2026-11-28T09:58:44Z",
                "input_rows": 120,
                "input_rows_per_second": 48.5,
                "processed_rows_per_second": 64.1,
                "batch_duration_ms": 1850,
                "event_time_watermark": "2026-04-06T16:45:42.647Z",
                "state_num_rows_total": 1958,
                "state_memory_used_bytes": 2 * 1024 * 1024,
            },
        },
    }
    rows = parse_ops_latest_rows(payload)
    assert len(rows) == 2

    by_name = {r["query"]: r for r in rows}
    ingest = by_name["ingest_bronze"]
    assert ingest["batch_id"] == 12
    assert ingest["input_rows"] == 0
    assert ingest["input_rows_per_sec"] == "0.00"
    assert ingest["processed_rows_per_sec"] == "15.25"
    assert ingest["batch_duration_ms"] == "420"
    assert ingest["state_memory_mb"] == "0.00"
    assert ingest["state_rows"] == "0"
    assert ingest["watermark"] == "n/a"
    assert ingest["updated_at"] == "2026-11-28 09:58:43 UTC"

    refine = by_name["refine_silver"]
    assert refine["batch_id"] == 9
    assert refine["input_rows"] == 120
    assert refine["input_rows_per_sec"] == "48.50"
    assert refine["processed_rows_per_sec"] == "64.10"
    assert refine["batch_duration_ms"] == "1,850"
    assert refine["state_memory_mb"] == "2.00"
    assert refine["state_rows"] == "1,958"
    assert refine["watermark"] == "2026-04-06T16:45:42.647Z"
    assert refine["updated_at"] == "2026-11-28 09:58:44 UTC"
    assert parse_ops_latest_rows(None) == []


def test_format_gold_last_run_caption_reads_last_run_utc() -> None:
    """Panel 3 reads Task 4.6 last_run_utc field and falls back cleanly when missing."""
    payload = {
        "last_run_utc": "2026-11-28T10:15:30Z",
        "duration_sec": 3.25,
        "status": "success",
        "models_built": 3,
    }
    caption = format_gold_last_run_caption(payload)
    assert "2026-11-28 10:15:30 UTC" in caption
    assert "success" in caption
    assert "3.25s" in caption

    assert "Gold cập nhật lúc n/a" in format_gold_last_run_caption(None)
