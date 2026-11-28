"""Main Streamlit application for CPC AdTech Lakehouse Monitor (Task 5.7)."""

from __future__ import annotations

import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import streamlit as st  # noqa: E402

from src.dashboard.db import DatabaseConnectionError  # noqa: E402
from src.dashboard.panels.common import (  # noqa: E402
    fetch_sim_now,
    format_utc_timestamp,
    get_repository,
    render_connection_error,
)
from src.dashboard.panels.funnel import render_funnel_panel  # noqa: E402
from src.dashboard.panels.health import render_health_panel  # noqa: E402
from src.dashboard.panels.pacing import render_pacing_panel  # noqa: E402
from src.dashboard.panels.publisher_quality import render_publisher_quality_panel  # noqa: E402
from src.dashboard.repository import DashboardRepository  # noqa: E402

st.set_page_config(
    page_title="CPC AdTech Lakehouse Monitor",
    page_icon="📊",
    layout="wide",
)


@st.fragment(run_every="10s")
def _pacing_fragment(repo: DashboardRepository) -> None:
    render_pacing_panel(repo)


@st.fragment(run_every="10s")
def _publisher_fragment(repo: DashboardRepository) -> None:
    render_publisher_quality_panel(repo)


@st.fragment(run_every="10s")
def _funnel_fragment(repo: DashboardRepository) -> None:
    render_funnel_panel(repo)


@st.fragment(run_every="10s")
def _health_fragment(repo: DashboardRepository) -> None:
    render_health_panel(repo)


def main() -> None:
    """Render the 4-panel CPC AdTech Lakehouse Dashboard."""
    st.title("CPC AdTech Lakehouse Monitor")
    st.caption(
        "Near Real-Time Lakehouse Dashboard (DuckDB zero-copy trên MinIO Delta & Gold Parquet) | "
        "Nguồn ngân sách thật: `campaign.csv` (ADR-010)."
    )

    try:
        repo = get_repository()
    except DatabaseConnectionError as exc:
        render_connection_error(exc)
        return

    with st.sidebar:
        st.header("Cấu hình làm tươi")
        st.markdown("- **Tự động làm tươi:** mỗi `10s` (`@st.fragment`)")
        st.markdown("- **Bộ nhớ đệm truy vấn:** TTL `5s` (`@st.cache_data`)")
        st.markdown("- **Đường nhanh:** Silver/Bronze/DLQ Delta qua DuckDB")
        st.markdown("- **Đường sâu:** Gold Parquet (`dbt_cpc`)")
        if st.button("Làm mới ngay", width="stretch"):
            st.cache_data.clear()
            st.rerun()

    try:
        sim_info, _ = fetch_sim_now()
        sim_now_label = format_utc_timestamp(sim_info.get("sim_now"))
        total_rows = sim_info.get("total_silver_rows", 0)
        st.info(
            f"**Đồng hồ mô phỏng (`sim_now`, R13):** `{sim_now_label}` | "
            f"**Tổng dòng Silver:** `{total_rows:,}`"
        )
    except DatabaseConnectionError as exc:
        render_connection_error(exc)
        return

    tab_pacing, tab_publisher, tab_funnel, tab_health = st.tabs(
        [
            "📊 1. Budget Pacing",
            "🛡️ 2. Publisher Radar",
            "🎯 3. Hiring Funnel & CPA",
            "🩺 4. Pipeline Health & DLQ",
        ]
    )

    with tab_pacing:
        _pacing_fragment(repo)

    with tab_publisher:
        _publisher_fragment(repo)

    with tab_funnel:
        _funnel_fragment(repo)

    with tab_health:
        _health_fragment(repo)


if __name__ == "__main__":
    main()
