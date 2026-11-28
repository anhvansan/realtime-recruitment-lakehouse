"""Panel 2: Publisher Fraud & Quality Radar (Task 5.4)."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from src.dashboard.db import DatabaseConnectionError
from src.dashboard.panels.common import (
    fetch_publisher_quality,
    fetch_sim_now,
    format_metric_or_na,
    format_percent_or_na,
    render_connection_error,
    render_panel_footer,
)
from src.dashboard.repository import DashboardRepository

_PUBLISHER_ROW_LIMIT = 100


def render_publisher_quality_panel(repo: DashboardRepository) -> None:
    """Render Panel 2: trailing 60m publisher click-to-apply quality and R1/R2 flags."""
    st.subheader("Publisher Fraud & Quality Radar")
    th = repo.thresholds
    st.info(
        "**Heuristic rule-based, không phải ML.** "
        f"Ngưỡng đang áp dụng từ `config/alerts.yaml`: "
        f"**R1** (`clicks_60m >= {th.click_fraud_min_clicks}` & `applies_60m <= {th.click_fraud_max_applies}`); "
        f"**R2** (`clicks_60m >= {th.click_fraud_r2_min_clicks}` & "
        f"`click_to_apply_rate_60m < {th.click_fraud_r2_median_factor:.1f} × median`)."
    )

    try:
        sim_info, sim_ms = fetch_sim_now()
    except DatabaseConnectionError as exc:
        render_connection_error(exc)
        return

    sim_now = sim_info.get("sim_now")
    if sim_now is None:
        st.info("Chưa có dữ liệu Silver để đánh giá chất lượng Publisher.")
        render_panel_footer(None, sim_ms)
        return

    try:
        pub_df, query_ms = fetch_publisher_quality(sim_now.isoformat())
    except DatabaseConnectionError as exc:
        render_connection_error(exc)
        return

    total_ms = sim_ms + query_ms

    if pub_df.empty:
        st.info("Không có lượt click nào trong cửa sổ 60 phút gần nhất tính tới sim_now.")
        render_panel_footer(sim_now, total_ms)
        return

    work_df = pub_df.copy()
    work_df["fraud_flag_r1"] = work_df["fraud_flag_r1"].fillna(False).astype(bool)
    work_df["fraud_flag_r2"] = work_df["fraud_flag_r2"].fillna(False).astype(bool)

    r1_count = int(work_df["fraud_flag_r1"].sum())
    r2_count = int(work_df["fraud_flag_r2"].sum())
    any_flagged = int((work_df["fraud_flag_r1"] | work_df["fraud_flag_r2"]).sum())

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Publisher hoạt động (60m)", f"{len(work_df)}")
    c2.metric("Bị gắn cờ bất kỳ", f"{any_flagged}")
    c3.metric("Vi phạm R1 (0 apply)", f"{r1_count}")
    c4.metric("Vi phạm R2 (< 20% median)", f"{r2_count}")

    sorted_df = work_df.sort_values(
        by=["fraud_flag_r1", "fraud_flag_r2", "clicks_60m"],
        ascending=[False, False, False],
    ).head(_PUBLISHER_ROW_LIMIT)

    display_df = pd.DataFrame(
        {
            "publisher_id": sorted_df["publisher_id"].astype(str),
            "publisher_name": sorted_df["publisher_name"].astype(str),
            "clicks_60m": sorted_df["clicks_60m"].apply(lambda v: format_metric_or_na(v, ",d")),
            "applies_60m": sorted_df["applies_60m"].apply(lambda v: format_metric_or_na(v, ",d")),
            "spend_60m": sorted_df["spend_60m"].apply(lambda v: format_metric_or_na(v, ",.2f", prefix="$")),
            "click_to_apply_rate_60m": sorted_df["click_to_apply_rate_60m"].apply(
                lambda v: format_percent_or_na(v, 2)
            ),
            "median_rate_60m": sorted_df["median_click_to_apply_rate_60m"].apply(
                lambda v: format_percent_or_na(v, 2)
            ),
            "fraud_flag_r1": sorted_df["fraud_flag_r1"],
            "fraud_flag_r2": sorted_df["fraud_flag_r2"],
        }
    )

    def _highlight_flagged(row: pd.Series) -> list[str]:
        if bool(row["fraud_flag_r1"]) or bool(row["fraud_flag_r2"]):
            return ["background-color: #fde8e8; color: #9b1c1c; font-weight: 600;"] * len(row)
        return [""] * len(row)

    styled = display_df.style.apply(_highlight_flagged, axis=1)
    st.dataframe(styled, width="stretch", hide_index=True)

    render_panel_footer(
        sim_now,
        total_ms,
        extra=f"Hiển thị {len(display_df)}/{len(work_df)} publishers trong 60m gần nhất",
    )
