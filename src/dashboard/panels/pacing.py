"""Panel 1: Budget Pacing Monitor (Task 5.3)."""

from __future__ import annotations

import altair as alt
import pandas as pd
import streamlit as st

from src.dashboard.db import DatabaseConnectionError
from src.dashboard.panels.common import (
    classify_pacing_status,
    fetch_pacing,
    fetch_sim_now,
    format_metric_or_na,
    format_percent_or_na,
    pacing_status_style,
    render_connection_error,
    render_panel_footer,
)
from src.dashboard.repository import DashboardRepository

_RANKING_ROW_LIMIT = 50


def render_pacing_panel(repo: DashboardRepository) -> None:
    """Render Panel 1: hourly campaign pacing curve, KPIs, and alert-colored ranking."""
    st.subheader("Budget Pacing Monitor")
    try:
        sim_info, sim_ms = fetch_sim_now()
    except DatabaseConnectionError as exc:
        render_connection_error(exc)
        return

    sim_now = sim_info.get("sim_now")
    sim_date = sim_info.get("sim_date")
    if sim_now is None or sim_date is None:
        st.info("Chưa có dữ liệu Silver để tính Budget Pacing.")
        render_panel_footer(None, sim_ms)
        return

    try:
        pacing_df, query_ms = fetch_pacing(sim_now.isoformat(), sim_date.isoformat())
    except DatabaseConnectionError as exc:
        render_connection_error(exc)
        return

    total_ms = sim_ms + query_ms

    exclude_outliers = st.checkbox(
        "Ẩn campaign ngân sách ngoại lai (campaign_id = 1: $1,002,930,129/ngày)",
        value=True,
        key="pacing_exclude_outliers",
    )
    st.caption(
        "Nguồn ngân sách: dữ liệu thật từ `campaign.csv` (ADR-010, không giả lập). "
        "Đường pacing tuyến tính lý tưởng: `ideal_cum_spend = daily_budget * elapsed_fraction`."
    )

    if pacing_df.empty:
        st.info(f"Không có dữ liệu pacing cho ngày {sim_date}.")
        render_panel_footer(sim_now, total_ms)
        return

    work_df = pacing_df.copy()
    if exclude_outliers and "is_outlier_budget" in work_df.columns:
        work_df = work_df[~work_df["is_outlier_budget"].fillna(False)].copy()

    if work_df.empty:
        st.warning("Tất cả campaign hiện tại đều nằm trong nhóm ngoại lai đã lọc.")
        render_panel_footer(sim_now, total_ms)
        return

    work_df = work_df.sort_values(["campaign_id", "hour_ts"]).reset_index(drop=True)
    latest_df = work_df.groupby("campaign_id", as_index=False).tail(1).copy()

    total_spend_today = float(pd.to_numeric(latest_df["cum_spend"], errors="coerce").fillna(0).sum())
    util_series = pd.to_numeric(latest_df["budget_utilization"], errors="coerce")
    pacing_series = pd.to_numeric(latest_df["pacing_ratio"], errors="coerce")

    warn_util = repo.thresholds.budget_utilization_warning
    fast_th = repo.thresholds.pacing_fast
    slow_th = repo.thresholds.pacing_slow

    over_90_count = int((util_series >= warn_util).sum())
    fast_count = int((pacing_series > fast_th).sum())
    slow_count = int((pacing_series < slow_th).sum())

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Tổng chi hôm nay (sim_now)", format_metric_or_na(total_spend_today, ",.2f", prefix="$"))
    c2.metric(f"Campaign >= {int(warn_util * 100)}% ngân sách", f"{over_90_count}")
    c3.metric(f"Chạy nhanh (> {fast_th:.1f}x)", f"{fast_count}")
    c4.metric(f"Chạy chậm (< {slow_th:.1f}x)", f"{slow_count}")

    latest_sorted_for_select = latest_df.sort_values(
        by=["cum_spend", "campaign_id"],
        ascending=[False, True],
    )
    campaign_ids = latest_sorted_for_select["campaign_id"].astype(str).tolist()
    campaign_labels = {
        str(row["campaign_id"]): f"{row['campaign_name']} (ID: {row['campaign_id']})"
        for _, row in latest_sorted_for_select.iterrows()
    }

    selected_campaign = st.selectbox(
        "Chọn campaign để xem đường cong Actual vs Ideal theo giờ",
        options=campaign_ids,
        format_func=lambda cid: campaign_labels.get(cid, cid),
        key="pacing_campaign_selector",
    )

    camp_df = work_df[work_df["campaign_id"].astype(str) == str(selected_campaign)].copy()
    if not camp_df.empty:
        chart_source = camp_df[["hour_ts", "cum_spend", "ideal_cum_spend"]].melt(
            id_vars=["hour_ts"],
            value_vars=["cum_spend", "ideal_cum_spend"],
            var_name="series",
            value_name="usd",
        )
        chart_source["series"] = chart_source["series"].map(
            {
                "cum_spend": "Chi tiêu thực tế (cum_spend)",
                "ideal_cum_spend": "Tuyến tính lý tưởng (ideal_cum_spend)",
            }
        )
        chart_source["usd"] = pd.to_numeric(chart_source["usd"], errors="coerce")

        line_chart = (
            alt.Chart(chart_source)
            .mark_line(point=True)
            .encode(
                x=alt.X("hour_ts:T", title="Giờ (UTC)"),
                y=alt.Y("usd:Q", title="Chi tiêu lũy kế (USD)"),
                color=alt.Color(
                    "series:N",
                    title="Đường cong",
                    scale=alt.Scale(range=["#2563eb", "#9ca3af"]),
                ),
                tooltip=[
                    alt.Tooltip("hour_ts:T", title="Giờ (UTC)"),
                    alt.Tooltip("series:N", title="Chuỗi"),
                    alt.Tooltip("usd:Q", title="USD", format=",.2f"),
                ],
            )
            .properties(height=280)
        )
        st.altair_chart(line_chart, width="stretch")

    st.markdown(f"**Bảng xếp hạng Pacing (Top {_RANKING_ROW_LIMIT} theo `pacing_ratio`)**")
    ranked = latest_df.sort_values(
        by=["pacing_ratio", "cum_spend"],
        ascending=[False, False],
        na_position="last",
    ).head(_RANKING_ROW_LIMIT)

    ranked["status"] = ranked["pacing_ratio"].apply(
        lambda v: classify_pacing_status(v, fast=fast_th, slow=slow_th)
    )

    display_df = pd.DataFrame(
        {
            "campaign_id": ranked["campaign_id"].astype(str),
            "campaign_name": ranked["campaign_name"].astype(str),
            "status": ranked["status"],
            "pacing_ratio": ranked["pacing_ratio"].apply(lambda v: format_metric_or_na(v, ".2f", suffix="x")),
            "cum_spend": ranked["cum_spend"].apply(lambda v: format_metric_or_na(v, ",.2f", prefix="$")),
            "daily_budget": ranked["daily_budget"].apply(lambda v: format_metric_or_na(v, ",.2f", prefix="$")),
            "budget_utilization": ranked["budget_utilization"].apply(lambda v: format_percent_or_na(v, 1)),
            "projected_eod_spend": ranked["projected_eod_spend"].apply(
                lambda v: format_metric_or_na(v, ",.2f", prefix="$")
            ),
            "budget_source": ranked["budget_source"].astype(str),
        }
    )

    def _highlight_row(row: pd.Series) -> list[str]:
        style = pacing_status_style(str(row["status"]))
        return [style] * len(row)

    styled = display_df.style.apply(_highlight_row, axis=1)
    st.dataframe(styled, width="stretch", hide_index=True)

    render_panel_footer(
        sim_now,
        total_ms,
        extra=f"Hiển thị {len(display_df)}/{len(latest_df)} campaigns (ngày {sim_date})",
    )
