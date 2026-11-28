"""Panel 4: Pipeline Health & DLQ Inspector (Task 5.6)."""

from __future__ import annotations

from typing import Any

import pandas as pd
import streamlit as st

from src.dashboard.db import DatabaseConnectionError
from src.dashboard.panels.common import (
    fetch_dlq_breakdown,
    fetch_dlq_samples,
    fetch_ops_latest,
    fetch_pipeline_health,
    fetch_sim_now,
    format_metric_or_na,
    format_percent_or_na,
    format_utc_timestamp,
    is_freshness_stale,
    render_connection_error,
    render_panel_footer,
    sanitize_dlq_payload,
)
from src.dashboard.repository import DashboardRepository

_DLQ_SAMPLE_LIMIT = 20


def _first_present(mapping: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if mapping.get(key) is not None:
            return mapping[key]
    return None


def parse_ops_latest_rows(ops_latest: dict[str, Any] | None) -> list[dict[str, Any]]:
    """Parse ``ops/latest.json`` (Task 3.9 nested ``queries`` schema) into display rows."""
    if not ops_latest or not isinstance(ops_latest, dict):
        return []

    raw_queries = ops_latest.get("queries")
    queries_map = raw_queries if isinstance(raw_queries, dict) else ops_latest
    snapshot_updated_at = ops_latest.get("updated_at")

    rows: list[dict[str, Any]] = []
    for query_name, metrics in queries_map.items():
        if not isinstance(metrics, dict):
            continue
        mem_bytes = metrics.get("state_memory_used_bytes")
        mem_mb = (
            float(mem_bytes) / (1024.0 * 1024.0)
            if mem_bytes is not None
            else metrics.get("state_memory_mb")
        )
        rows.append(
            {
                "query": str(metrics.get("query_name") or query_name),
                "batch_id": metrics.get("batch_id"),
                "input_rows": metrics.get("input_rows"),
                "input_rows_per_sec": format_metric_or_na(
                    _first_present(metrics, "input_rows_per_second", "input_rows_per_sec"),
                    ",.2f",
                ),
                "processed_rows_per_sec": format_metric_or_na(
                    _first_present(metrics, "processed_rows_per_second", "processed_rows_per_sec"),
                    ",.2f",
                ),
                "batch_duration_ms": format_metric_or_na(
                    _first_present(metrics, "batch_duration_ms", "trigger_execution_ms"),
                    ",.0f",
                ),
                "state_memory_mb": format_metric_or_na(mem_mb, ",.2f"),
                "state_rows": format_metric_or_na(metrics.get("state_num_rows_total"), ",d"),
                "watermark": _first_present(metrics, "event_time_watermark", "watermark") or "n/a",
                "updated_at": format_utc_timestamp(
                    _first_present(metrics, "timestamp", "updated_at") or snapshot_updated_at
                ),
            }
        )
    return rows


def render_health_panel(repo: DashboardRepository) -> None:
    """Render Panel 4: pipeline freshness, e2e latency p50/p95, DLQ breakdown, and ops/latest.json."""
    st.subheader("Pipeline Health & DLQ Inspector")

    try:
        health_df, health_ms = fetch_pipeline_health()
        dlq_breakdown_df, dlq_bd_ms = fetch_dlq_breakdown()
        dlq_samples_df, dlq_sm_ms = fetch_dlq_samples(limit=_DLQ_SAMPLE_LIMIT)
        sim_info, sim_ms = fetch_sim_now()
    except DatabaseConnectionError as exc:
        render_connection_error(exc)
        return

    ops_latest, ops_ms = fetch_ops_latest()
    total_ms = health_ms + dlq_bd_ms + dlq_sm_ms + sim_ms + ops_ms

    stale_th = repo.thresholds.data_stale_seconds
    if not health_df.empty:
        row = health_df.iloc[0]
        freshness_s = row.get("data_freshness_s")
        freshness_label = format_metric_or_na(freshness_s, ",.1f", suffix=" s")
        if is_freshness_stale(freshness_s, threshold_s=float(stale_th)):
            st.error(
                f"Trạng thái Freshness: **ĐỎ (Trễ > {stale_th}s)** — "
                f"`data_freshness_s = {freshness_label}` | `max_processed_at = {format_utc_timestamp(row.get('max_processed_at'))}`"
            )
        else:
            st.success(
                f"Trạng thái Freshness: **XANH (Tươi <= {stale_th}s)** — "
                f"`data_freshness_s = {freshness_label}` | `max_processed_at = {format_utc_timestamp(row.get('max_processed_at'))}`"
            )

        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Freshness", freshness_label)
        c2.metric("E2E Latency p50", format_metric_or_na(row.get("e2e_latency_p50_s"), ",.2f", suffix=" s"))
        c3.metric("E2E Latency p95", format_metric_or_na(row.get("e2e_latency_p95_s"), ",.2f", suffix=" s"))
        c4.metric("Dedup Rate", format_percent_or_na(row.get("dedup_rate"), 2))
        c5.metric("DLQ Rate", format_percent_or_na(row.get("dlq_rate"), 2))

        r1, r2, r3 = st.columns(3)
        r1.metric("Silver Rows", format_metric_or_na(row.get("silver_rows"), ",d"))
        r2.metric("Bronze Rows", format_metric_or_na(row.get("bronze_rows"), ",d"))
        r3.metric("DLQ Rows", format_metric_or_na(row.get("dlq_rows"), ",d"))
    else:
        st.info("Chưa có số liệu sức khỏe pipeline.")

    st.markdown("**Spark Streaming Metrics (`ops/latest.json`)**")
    if ops_latest:
        ops_rows = parse_ops_latest_rows(ops_latest)
        if ops_rows:
            st.dataframe(pd.DataFrame(ops_rows), width="stretch", hide_index=True)
        else:
            st.info("`ops/latest.json` không chứa bản ghi query nào.")
    else:
        st.info("Chưa tìm thấy `ops/latest.json` trên MinIO.")

    col_bd, col_sm = st.columns([1, 2])
    with col_bd:
        st.markdown("**Phân rã DLQ theo `error_type`**")
        if dlq_breakdown_df.empty:
            st.info("DLQ đang trống.")
        else:
            bd_display = dlq_breakdown_df.copy()
            bd_display["last_failed_at"] = bd_display["last_failed_at"].apply(format_utc_timestamp)
            st.dataframe(bd_display, width="stretch", hide_index=True)

    with col_sm:
        st.markdown(f"**Mẫu DLQ gần nhất (tối đa {_DLQ_SAMPLE_LIMIT} dòng, đã rút gọn & ẩn bí mật)**")
        if dlq_samples_df.empty:
            st.info("Không có bản ghi DLQ mẫu.")
        else:
            sm_display = pd.DataFrame(
                {
                    "error_type": dlq_samples_df["error_type"].astype(str),
                    "stage": dlq_samples_df["stage"].astype(str),
                    "error_detail": dlq_samples_df["error_detail"].astype(str).apply(
                        lambda s: sanitize_dlq_payload(s, max_len=100)
                    ),
                    "failed_at": dlq_samples_df["failed_at"].apply(format_utc_timestamp),
                    "payload_preview": dlq_samples_df["payload_preview"].apply(
                        lambda s: sanitize_dlq_payload(s, max_len=140)
                    ),
                }
            )
            st.dataframe(sm_display, width="stretch", hide_index=True)

    render_panel_footer(sim_info.get("sim_now"), total_ms)
