"""Panel 3: Hiring Funnel & CPA (Task 5.5)."""

from __future__ import annotations

from typing import Any

import altair as alt
import pandas as pd
import streamlit as st

from src.dashboard.db import DatabaseConnectionError
from src.dashboard.panels.common import (
    compute_funnel_summary,
    fetch_funnel_efficiency,
    fetch_gold_last_run,
    fetch_sim_now,
    format_metric_or_na,
    format_percent_or_na,
    format_utc_timestamp,
    render_connection_error,
    render_panel_footer,
)
from src.dashboard.repository import DashboardRepository

_TOP_BOTTOM_LIMIT = 15


def format_gold_last_run_caption(gold_meta: dict[str, Any] | None) -> str:
    """Format Gold refresh caption from ``gold/_last_run.json`` (Task 4.6 schema)."""
    if gold_meta:
        run_ts = gold_meta.get("last_run_utc") or gold_meta.get("finished_at_utc")
        if run_ts:
            gold_ts = format_utc_timestamp(run_ts)
            gold_status = gold_meta.get("status", "unknown")
            duration = gold_meta.get("duration_sec")
            dur_part = f", {duration}s" if duration is not None else ""
            return (
                f"Gold cập nhật lúc {gold_ts} (trạng thái dbt: {gold_status}{dur_part}) — "
                "Phễu: Click → Apply → Qualified"
            )
    return "Gold cập nhật lúc n/a (chưa có gold/_last_run.json) — Phễu: Click → Apply → Qualified"


def _format_cpqa_table(df: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "job_title": df["job_title"].astype(str),
            "campaign_name": df["campaign_name"].astype(str),
            "company_name": df["company_name"].astype(str),
            "event_date": df["event_date"].astype(str),
            "clicks": df["clicks"].apply(lambda v: format_metric_or_na(v, ",d")),
            "applies": df["applies"].apply(lambda v: format_metric_or_na(v, ",d")),
            "qualified": df["qualified"].apply(lambda v: format_metric_or_na(v, ",d")),
            "spend": df["spend"].apply(lambda v: format_metric_or_na(v, ",.2f", prefix="$")),
            "cpc": df["cpc"].apply(lambda v: format_metric_or_na(v, ",.2f", prefix="$")),
            "cpa": df["cpa"].apply(lambda v: format_metric_or_na(v, ",.2f", prefix="$")),
            "cpqa": df["cpqa"].apply(lambda v: format_metric_or_na(v, ",.2f", prefix="$")),
            "click_to_apply_rate": df["click_to_apply_rate"].apply(lambda v: format_percent_or_na(v, 1)),
            "qualified_rate": df["qualified_rate"].apply(lambda v: format_percent_or_na(v, 1)),
        }
    )


def render_funnel_panel(repo: DashboardRepository) -> None:
    """Render Panel 3: Click -> Apply -> Qualified funnel and CPQA rankings from Gold."""
    del repo
    st.subheader("Hiring Funnel & CPA (Gold Mart)")

    try:
        funnel_df, funnel_ms = fetch_funnel_efficiency()
        sim_info, sim_ms = fetch_sim_now()
    except DatabaseConnectionError as exc:
        render_connection_error(exc)
        return

    gold_meta, meta_ms = fetch_gold_last_run()
    total_ms = funnel_ms + sim_ms + meta_ms

    st.caption(format_gold_last_run_caption(gold_meta))

    if funnel_df.empty:
        st.info("Bảng Gold `fct_recruitment_efficiency.parquet` chưa có dữ liệu.")
        render_panel_footer(sim_info.get("sim_now"), total_ms)
        return

    summary = compute_funnel_summary(funnel_df)
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Clicks", format_metric_or_na(summary["clicks"], ",d"))
    c2.metric("Applies", format_metric_or_na(summary["applies"], ",d"))
    c3.metric("Qualified", format_metric_or_na(summary["qualified"], ",d"))
    c4.metric("Click → Apply", format_percent_or_na(summary["click_to_apply_rate"], 2))
    c5.metric("Qualified Rate", format_percent_or_na(summary["qualified_rate"], 2))
    c6.metric("CPQA Tổng", format_metric_or_na(summary["cpqa"], ",.2f", prefix="$"))

    stage_df = pd.DataFrame(
        [
            {"stage": "1. Click", "count": summary["clicks"]},
            {"stage": "2. Apply", "count": summary["applies"]},
            {"stage": "3. Qualified", "count": summary["qualified"]},
        ]
    )
    funnel_chart = (
        alt.Chart(stage_df)
        .mark_bar(cornerRadiusTopRight=4, cornerRadiusBottomRight=4)
        .encode(
            x=alt.X("count:Q", title="Số lượng sự kiện"),
            y=alt.Y("stage:N", sort=["1. Click", "2. Apply", "3. Qualified"], title="Tầng phễu"),
            color=alt.Color(
                "stage:N",
                legend=None,
                scale=alt.Scale(range=["#2563eb", "#0ea5e9", "#10b981"]),
            ),
            tooltip=[
                alt.Tooltip("stage:N", title="Tầng"),
                alt.Tooltip("count:Q", title="Số lượng", format=",d"),
            ],
        )
        .properties(height=180)
    )
    st.altair_chart(funnel_chart, width="stretch")

    work_df = funnel_df.copy()
    work_df["cpqa_num"] = pd.to_numeric(work_df["cpqa"], errors="coerce")
    with_cpqa = work_df[work_df["cpqa_num"].notna()].copy()
    na_cpqa_count = int(work_df["cpqa_num"].isna().sum())

    col_best, col_worst = st.columns(2)
    with col_best:
        st.markdown(f"**Top {_TOP_BOTTOM_LIMIT} CPQA tốt nhất (thấp nhất)**")
        if with_cpqa.empty:
            st.info("Chưa có job/campaign nào có `qualified > 0` để xếp hạng CPQA.")
        else:
            best_df = with_cpqa.sort_values(by=["cpqa_num", "qualified"], ascending=[True, False]).head(
                _TOP_BOTTOM_LIMIT
            )
            st.dataframe(_format_cpqa_table(best_df), width="stretch", hide_index=True)

    with col_worst:
        st.markdown(f"**Top {_TOP_BOTTOM_LIMIT} CPQA cao nhất (tốn kém nhất)**")
        if with_cpqa.empty:
            st.info("Chưa có job/campaign nào có `qualified > 0` để xếp hạng CPQA.")
        else:
            worst_df = with_cpqa.sort_values(by=["cpqa_num", "spend"], ascending=[False, False]).head(
                _TOP_BOTTOM_LIMIT
            )
            st.dataframe(_format_cpqa_table(worst_df), width="stretch", hide_index=True)

    if na_cpqa_count > 0:
        st.caption(
            f"Lưu ý: có **{na_cpqa_count}/{len(work_df)}** dòng có `qualified = 0` nên `CPQA = \"n/a\"` (quy tắc mẫu số bằng 0)."
        )

    render_panel_footer(sim_info.get("sim_now"), total_ms, extra=f"{len(work_df)} dòng Gold Parquet")
