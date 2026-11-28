"""Shared formatting, status classification, and cached data helpers for dashboard panels."""

from __future__ import annotations

import math
import re
import time
from datetime import date, datetime
from typing import Any

import pandas as pd
import streamlit as st

from src.dashboard.db import get_root_connection
from src.dashboard.repository import DashboardRepository

_SECRET_PATTERN = re.compile(
    r'(?i)(["\']?\b(?:secret|password|passwd|token|api[_-]?key|access[_-]?key|secret[_-]?key)\b["\']?\s*[:=]\s*)(["\']?)([^\s,"\'}]+)\2'
)


def format_metric_or_na(
    value: Any,
    fmt: str = ",.2f",
    prefix: str = "",
    suffix: str = "",
) -> str:
    """Format a numeric metric or return ``'n/a'`` for NULL/NaN/infinite values (Rule R9)."""
    if value is None or pd.isna(value):
        return "n/a"
    if isinstance(value, bool):
        return str(value)
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return str(value)
    if not math.isfinite(numeric):
        return "n/a"
    if "d" in fmt:
        return f"{prefix}{format(int(numeric), fmt)}{suffix}"
    target = value if isinstance(value, int) else numeric
    return f"{prefix}{format(target, fmt)}{suffix}"


def format_percent_or_na(value: Any, decimals: int = 1) -> str:
    """Format a 0..1 ratio as a percentage string or return ``'n/a'`` when missing."""
    if value is None or pd.isna(value) or isinstance(value, bool):
        return "n/a"
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return "n/a"
    if not math.isfinite(numeric):
        return "n/a"
    return f"{numeric * 100:.{decimals}f}%"


def classify_pacing_status(
    pacing_ratio: Any,
    *,
    fast: float = 1.2,
    slow: float = 0.6,
) -> str:
    """Classify campaign pacing ratio into ``fast``, ``slow``, ``normal``, or ``na``."""
    if pacing_ratio is None or pd.isna(pacing_ratio) or isinstance(pacing_ratio, bool):
        return "na"
    try:
        ratio = float(pacing_ratio)
    except (TypeError, ValueError):
        return "na"
    if not math.isfinite(ratio):
        return "na"
    if ratio > fast:
        return "fast"
    if ratio < slow:
        return "slow"
    return "normal"


def pacing_status_style(status: str) -> str:
    """Return CSS inline style for pacing alert status (red > 1.2, yellow < 0.6)."""
    if status == "fast":
        return "background-color: #fde8e8; color: #9b1c1c; font-weight: 600;"
    if status == "slow":
        return "background-color: #fef3c7; color: #92400e; font-weight: 600;"
    return ""


def is_freshness_stale(freshness_s: Any, threshold_s: float = 120.0) -> bool:
    """Return True when data freshness exceeds ``threshold_s`` or is unavailable."""
    if freshness_s is None or pd.isna(freshness_s) or isinstance(freshness_s, bool):
        return True
    try:
        numeric = float(freshness_s)
    except (TypeError, ValueError):
        return True
    if not math.isfinite(numeric):
        return True
    return numeric > threshold_s


def sanitize_dlq_payload(payload: Any, max_len: int = 140) -> str:
    """Normalize whitespace, redact secret-like fields, and truncate DLQ payload text."""
    if payload is None:
        return ""
    if not isinstance(payload, str) and pd.isna(payload):
        return ""
    compact = re.sub(r"[\r\n\t]+", " ", str(payload)).strip()
    redacted = _SECRET_PATTERN.sub(r"\1<redacted>", compact)
    if max_len <= 0:
        return ""
    if len(redacted) <= max_len:
        return redacted
    if max_len <= 3:
        return redacted[:max_len]
    return f"{redacted[: max_len - 3]}..."


def format_utc_timestamp(ts: Any) -> str:
    """Format a datetime/timestamp/ISO string into ``YYYY-MM-DD HH:MM:SS UTC``."""
    if ts is None:
        return "n/a"
    if not isinstance(ts, str) and pd.isna(ts):
        return "n/a"
    try:
        parsed = pd.Timestamp(ts)
    except (TypeError, ValueError):
        return str(ts)
    if pd.isna(parsed):
        return "n/a"
    if parsed.tzinfo is not None:
        parsed = parsed.tz_convert("UTC").tz_localize(None)
    return parsed.strftime("%Y-%m-%d %H:%M:%S UTC")


def compute_funnel_summary(df: pd.DataFrame) -> dict[str, Any]:
    """Aggregate Click -> Apply -> Qualified metrics using ``None`` for zero denominators."""
    if df is None or df.empty:
        return {
            "clicks": 0,
            "applies": 0,
            "qualified": 0,
            "unqualified": 0,
            "spend": 0.0,
            "click_to_apply_rate": None,
            "qualified_rate": None,
            "cpc": None,
            "cpa": None,
            "cpqa": None,
        }

    def _sum_col(col: str) -> float:
        if col not in df.columns:
            return 0.0
        return float(pd.to_numeric(df[col], errors="coerce").fillna(0).sum())

    clicks = int(_sum_col("clicks"))
    applies = int(_sum_col("applies"))
    qualified = int(_sum_col("qualified"))
    unqualified = int(_sum_col("unqualified"))
    spend = _sum_col("spend")

    return {
        "clicks": clicks,
        "applies": applies,
        "qualified": qualified,
        "unqualified": unqualified,
        "spend": spend,
        "click_to_apply_rate": (applies / clicks) if clicks > 0 else None,
        "qualified_rate": (qualified / applies) if applies > 0 else None,
        "cpc": (spend / clicks) if clicks > 0 else None,
        "cpa": (spend / applies) if applies > 0 else None,
        "cpqa": (spend / qualified) if qualified > 0 else None,
    }


@st.cache_resource(show_spinner=False)
def get_repository() -> DashboardRepository:
    """Return process-wide cached DashboardRepository bound to the cached DuckDB root."""
    return DashboardRepository(root_conn=get_root_connection())


@st.cache_data(ttl=5, show_spinner=False)
def fetch_sim_now() -> tuple[dict[str, Any], float]:
    t0 = time.perf_counter()
    data = get_repository().get_sim_now()
    return data, (time.perf_counter() - t0) * 1000.0


@st.cache_data(ttl=5, show_spinner=False)
def fetch_pacing(sim_now_iso: str, event_date_iso: str) -> tuple[pd.DataFrame, float]:
    t0 = time.perf_counter()
    sim_now = datetime.fromisoformat(sim_now_iso)
    event_date = date.fromisoformat(event_date_iso)
    df = get_repository().get_pacing(sim_now=sim_now, event_date=event_date)
    return df, (time.perf_counter() - t0) * 1000.0


@st.cache_data(ttl=5, show_spinner=False)
def fetch_publisher_quality(sim_now_iso: str) -> tuple[pd.DataFrame, float]:
    t0 = time.perf_counter()
    sim_now = datetime.fromisoformat(sim_now_iso)
    df = get_repository().get_publisher_quality(sim_now=sim_now)
    return df, (time.perf_counter() - t0) * 1000.0


@st.cache_data(ttl=5, show_spinner=False)
def fetch_funnel_efficiency() -> tuple[pd.DataFrame, float]:
    t0 = time.perf_counter()
    df = get_repository().get_funnel_efficiency()
    return df, (time.perf_counter() - t0) * 1000.0


@st.cache_data(ttl=5, show_spinner=False)
def fetch_pipeline_health() -> tuple[pd.DataFrame, float]:
    t0 = time.perf_counter()
    df = get_repository().get_pipeline_health()
    return df, (time.perf_counter() - t0) * 1000.0


@st.cache_data(ttl=5, show_spinner=False)
def fetch_dlq_breakdown() -> tuple[pd.DataFrame, float]:
    t0 = time.perf_counter()
    df = get_repository().get_dlq_breakdown()
    return df, (time.perf_counter() - t0) * 1000.0


@st.cache_data(ttl=5, show_spinner=False)
def fetch_dlq_samples(limit: int = 20) -> tuple[pd.DataFrame, float]:
    t0 = time.perf_counter()
    df = get_repository().get_dlq_samples(limit=limit)
    return df, (time.perf_counter() - t0) * 1000.0


@st.cache_data(ttl=5, show_spinner=False)
def fetch_ops_latest() -> tuple[dict[str, Any] | None, float]:
    t0 = time.perf_counter()
    data = get_repository().get_ops_latest()
    return data, (time.perf_counter() - t0) * 1000.0


@st.cache_data(ttl=5, show_spinner=False)
def fetch_gold_last_run() -> tuple[dict[str, Any] | None, float]:
    t0 = time.perf_counter()
    data = get_repository().get_gold_last_run()
    return data, (time.perf_counter() - t0) * 1000.0


def render_connection_error(exc: Exception) -> None:
    """Display a non-crashing, credential-safe warning when MinIO/DuckDB is unreachable."""
    st.error(
        "Mất kết nối dữ liệu Lakehouse (MinIO / DuckDB). "
        "Hãy kiểm tra dịch vụ MinIO đang chạy và thử lại sau ít giây.\n\n"
        f"Chi tiết tóm tắt: {type(exc).__name__}"
    )


def render_panel_footer(sim_now: Any, query_ms: float, extra: str | None = None) -> None:
    """Render standardized panel footer with ``sim_now`` clock and query latency."""
    sim_label = format_utc_timestamp(sim_now)
    suffix = f" | {extra}" if extra else ""
    st.caption(f"Cập nhật lúc {sim_label} (sim_now) | Truy vấn mất {query_ms:.1f} ms{suffix}")
