#!/usr/bin/env python3
"""Profiling script for the 18 CSV datasets in data/raw/.

Implements Task 2.1 (Step 1):
- Phase 1: Inventory & Triage of all 18 CSV files (size, rows, schema, null rates, samples, category)
- Phase 2: Deep verification of Hypotheses and Unknowns (U0-U9, H1, H4) using DuckDB SQL.
"""

from __future__ import annotations

import glob
import os
import sys
from typing import Any

import duckdb


def format_bytes(size: int) -> str:
    """Format bytes to human readable string."""
    for unit in ["B", "KB", "MB", "GB"]:
        if size < 1024.0:
            return f"{size:3.1f} {unit}"
        size /= 1024.0
    return f"{size:.1f} TB"


def run_phase_1(con: duckdb.DuckDBPyConnection, raw_files: list[str]) -> list[dict[str, Any]]:
    """Phase 1: Inventory and triage of all 18 files."""
    print("=" * 80)
    print("GIAI ĐOẠN 1: INVENTORY & TRIAGE TOÀN BỘ 18 FILE CSV")
    print("=" * 80)

    summary_list = []
    triage_categories = {
        # A: Stream source
        "tracking.csv": ("A - Event Stream (Clicks/Events)", "Luồng sự kiện chính (click, search/view)"),
        "application.csv": ("A/B - Event Stream / Seed (Applies)", "Sự kiện ứng tuyển (apply/qualified/unqualified)"),
        # B: Dimension
        "campaign.csv": ("B - Dimension / Seed", "Thông tin chiến dịch, ngân sách & bid"),
        "job.csv": ("B - Dimension / Seed", "Thông tin tin tuyển dụng"),
        "master_publisher.csv": ("B - Dimension / Seed", "Danh mục đối tác hiển thị quảng cáo"),
        "company.csv": ("B - Dimension / Seed", "Thông tin nhà tuyển dụng"),
        "group.csv": ("B - Dimension / Seed", "Nhóm tài khoản / Advertiser group"),
        "events.csv": ("B - Ground Truth / Benchmark", "Bảng tổng hợp đối soát theo giờ (clicks, applies, spend_hour)"),
        "master_major_category.csv": ("B - Dimension", "Danh mục ngành nghề cấp 1"),
        "master_minor_category.csv": ("B - Dimension", "Danh mục ngành nghề cấp 2"),
        # C: Ignore
        "search.csv": ("C - Secondary / Ignore", "Snapshot tìm kiếm job (thông tin đã có trong job.csv)"),
        "master_city.csv": ("C - Ignore (Out-of-Scope)", "Dữ liệu địa lý chi tiết (~11MB), không cần cho P1-P4"),
        "job_location.csv": ("C - Ignore (Out-of-Scope)", "Bảng nối địa điểm job chi tiết"),
        "dev_user.csv": ("C - Ignore (Out-of-Scope)", "User nội bộ hệ thống"),
        "conversation.csv": ("C - Ignore (Out-of-Scope)", "Lịch sử chat/tin nhắn nội bộ"),
        "user_role.csv": ("C - Ignore (Out-of-Scope)", "Phân quyền user nội bộ"),
        "master_role.csv": ("C - Ignore (Out-of-Scope)", "Danh mục vai trò nội bộ"),
        "user_client_assignee.csv": ("C - Ignore (Out-of-Scope)", "Phân công phụ trách khách hàng"),
    }

    for fpath in raw_files:
        fname = os.path.basename(fpath)
        file_size = os.path.getsize(fpath)
        safe_path = fpath.replace("\\", "/")

        try:
            # Row count
            row_count = con.execute(f"SELECT count(*) FROM read_csv_auto('{safe_path}')").fetchone()[0]

            # Schema
            desc = con.execute(f"DESCRIBE SELECT * FROM read_csv_auto('{safe_path}')").fetchall()
            cols_info = [(r[0], r[1]) for r in desc]
            col_names = [c[0] for c in cols_info]

            # Null count for candidate key columns
            null_stats = {}
            candidate_keys = [
                c for c in col_names if any(k in c.lower() for k in ["id", "time", "date", "status", "type"])
            ]
            if candidate_keys and row_count > 0:
                null_queries = [f"SUM(CASE WHEN \"{c}\" IS NULL THEN 1 ELSE 0 END) AS \"{c}\"" for c in candidate_keys]
                null_res = con.execute(f"SELECT {', '.join(null_queries)} FROM read_csv_auto('{safe_path}')").fetchone()
                for c, null_cnt in zip(candidate_keys, null_res, strict=False):
                    null_pct = (null_cnt / row_count) * 100 if row_count > 0 else 0
                    null_stats[c] = (null_cnt, null_pct)

            # Sample 2 rows
            samples = con.execute(f"SELECT * FROM read_csv_auto('{safe_path}') LIMIT 2").fetchall()

            category, reason = triage_categories.get(fname, ("Chưa phân loại", "Cần kiểm tra"))

            summary = {
                "file": fname,
                "size_bytes": file_size,
                "size_str": format_bytes(file_size),
                "rows": row_count,
                "cols": cols_info,
                "null_stats": null_stats,
                "samples": samples,
                "category": category,
                "reason": reason,
            }
            summary_list.append(summary)

            print(f"\n📂 [{fname}] — {format_bytes(file_size)} — {row_count:,} dòng — Phân loại: {category}")
            print(f"   Lý do: {reason}")
            print(f"   Cột ({len(cols_info)}): {', '.join([f'{c[0]} ({c[1]})' for c in cols_info])}")
            if null_stats:
                null_str = ", ".join([f"{k}: {cnt} null ({pct:.1f}%)" for k, (cnt, pct) in list(null_stats.items())[:5]])
                print(f"   Null stats: {null_str}")
            if samples:
                sample_display = [str(x) if str(x) != "" else "NULL" for x in samples[0][:5]]
                print(f"   Mẫu dòng 1 (5 cột đầu): {sample_display}")

        except Exception as e:
            print(f"❌ Lỗi khi đọc file {fname}: {e}")

    return summary_list


def run_phase_2(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    """Phase 2: Deep verification of U0-U9, H1, H4."""
    print("\n" + "=" * 80)
    print("GIAI ĐOẠN 2: ĐÀO SÂU KIỂM CHỨNG CÁC GIẢ THUYẾT & UNKNOWNS (U0–U9, H1, H4)")
    print("=" * 80)

    verdicts: dict[str, Any] = {}

    # --------------------------------------------------------------------------
    # U1: Cột thời điểm sự kiện trong tracking.csv & application.csv
    # --------------------------------------------------------------------------
    print("\n--- [U1] CỘT THỜI ĐIỂM SỰ KIỆN TRONG TRACKING & APPLICATION ---")
    u1_tracking_sample = con.execute("""
        SELECT create_time, ts, id, job_id, campaign_id, publisher_id, bid
        FROM read_csv_auto('data/raw/tracking.csv')
        LIMIT 5
    """).fetchall()
    print("Mẫu dữ liệu tracking.csv (create_time vs ts vs id):")
    for r in u1_tracking_sample:
        print(f"   create_time={r[0]} | ts={r[1]} | id={r[2]} | job={r[3]} | camp={r[4]} | pub={r[5]} | bid={r[6]}")

    # Inspect what create_time and ts are
    u1_t_stats = con.execute("""
        SELECT
            min(ts) as min_ts,
            max(ts) as max_ts,
            count(*) as total_rows,
            count(ts) as valid_ts_count
        FROM read_csv_auto('data/raw/tracking.csv')
    """).fetchall()
    print(f"\nThống kê cột tracking.ts (TIMESTAMP): {u1_t_stats}")

    # Inspect application.csv timestamps
    u1_app_sample = con.execute("""
        SELECT id, created_date, last_modified_date, status, is_review, is_match, is_rejected, score, job_id, publisher_id
        FROM read_csv_auto('data/raw/application.csv')
        LIMIT 5
    """).fetchall()
    print("\nMẫu dữ liệu application.csv:")
    for r in u1_app_sample:
        print(f"   id={r[0]} | created_date={r[1]} | modified={r[2]} | status={r[3]} | job={r[8]} | pub={r[9]}")

    u1_app_stats = con.execute("""
        SELECT
            min(created_date) as min_created,
            max(created_date) as max_created,
            count(*) as total_rows
        FROM read_csv_auto('data/raw/application.csv')
    """).fetchall()
    print(f"Thống kê thời gian application.created_date: {u1_app_stats}")

    # --------------------------------------------------------------------------
    # U2 & H4: Phễu sự kiện (tracking.csv, application.csv, events.csv)
    # --------------------------------------------------------------------------
    print("\n--- [U2 & H4] PHỄU SỰ KIỆN & CÓ SỰ KIỆN IMPRESSION KHÔNG? ---")
    print("Nội dung events.csv (Mẫu bảng tổng hợp theo giờ):")
    events_sample = con.execute("SELECT * FROM read_csv_auto('data/raw/events.csv') LIMIT 5").fetchall()
    for row in events_sample:
        print(f"   {row}")

    events_summary = con.execute("""
        SELECT
            min(dates) as min_date,
            max(dates) as max_date,
            sum(clicks) as total_clicks,
            sum(cast(conversion as bigint)) as total_conversions,
            sum(qualified_application) as total_qualified,
            sum(disqualified_application) as total_disqualified,
            sum(spend_hour) as total_spend_hour
        FROM read_csv_auto('data/raw/events.csv')
    """).fetchall()
    print(f"Tổng quan events.csv: {events_summary}")

    # Check distinct values of tracking.custom_track, ev, etc.
    print("\nPhân tích các cột sự kiện trong tracking.csv:")
    custom_tracks = con.execute("""
        SELECT custom_track, count(*)
        FROM read_csv_auto('data/raw/tracking.csv')
        GROUP BY 1 ORDER BY 2 DESC
    """).fetchall()
    print(f"custom_track distribution: {custom_tracks}")

    ev_dist = con.execute("""
        SELECT ev, count(*)
        FROM read_csv_auto('data/raw/tracking.csv')
        GROUP BY 1 ORDER BY 2 DESC
    """).fetchall()
    print(f"ev column distribution: {ev_dist}")

    # Cross-tab between ev and custom_track
    ev_cross = con.execute("""
        SELECT
            coalesce(cast(ev as varchar), 'NULL') as ev,
            coalesce(custom_track, 'NULL') as custom_track,
            count(*) as cnt,
            count(job_id) as with_job,
            count(campaign_id) as with_campaign,
            count(bid) filter (where bid > 0) as paid_clicks
        FROM read_csv_auto('data/raw/tracking.csv')
        GROUP BY 1, 2
        ORDER BY 1, 3 DESC
    """).fetchall()
    print("\nCross-tab ev vs custom_track (with job/campaign/paid_clicks counts):")
    for r in ev_cross:
        print(f"   ev={r[0]:5} | custom_track={r[1]:20} | count={r[2]:5} | with_job={r[3]:5} | with_campaign={r[4]:5} | paid_clicks={r[5]:5}")

    # Check application status
    print("\nPhân tích trạng thái trong application.csv:")
    app_statuses = con.execute("""
        SELECT status, count(*)
        FROM read_csv_auto('data/raw/application.csv')
        GROUP BY 1 ORDER BY 2 DESC
    """).fetchall()
    print(f"application.status distribution: {app_statuses}")

    app_flags = con.execute("""
        SELECT
            sum(is_review) as reviews,
            sum(is_match) as matches,
            sum(is_rejected) as rejections,
            sum(is_contacted) as contacted,
            avg(score) as avg_score
        FROM read_csv_auto('data/raw/application.csv')
    """).fetchall()
    print(f"application flags (is_review, is_match, is_rejected, is_contacted, avg_score): {app_flags}")

    # --------------------------------------------------------------------------
    # H1: spend và bid trong tracking.csv & campaign.csv
    # --------------------------------------------------------------------------
    print("\n--- [H1] GIÁ THẦU (BID) VÀ QUY TẮC TÍNH CHI TIÊU (SPEND) ---")
    bid_stats = con.execute("""
        SELECT
            min(bid) as min_bid,
            max(bid) as max_bid,
            avg(bid) as avg_bid,
            count(*) filter (where bid is null) as null_bids,
            count(*) filter (where bid = 0) as zero_bids,
            count(*) filter (where bid > 0) as positive_bids
        FROM read_csv_auto('data/raw/tracking.csv')
    """).fetchall()
    print(f"Thống kê tracking.bid: {bid_stats}")

    bid_dist = con.execute("""
        SELECT bid, count(*)
        FROM read_csv_auto('data/raw/tracking.csv')
        GROUP BY 1 ORDER BY 2 DESC LIMIT 10
    """).fetchall()
    print(f"Top 10 giá bid phổ biến trong tracking.csv: {bid_dist}")

    # Total spend if spend = bid
    spend_calc = con.execute("""
        SELECT
            sum(bid) as total_simulated_spend,
            count(*) as total_clicks
        FROM read_csv_auto('data/raw/tracking.csv')
    """).fetchall()
    print(f"Tổng spend (nếu spend = bid) trên 6,662 clicks: {spend_calc[0][0]:,.2f}")

    # --------------------------------------------------------------------------
    # U5: Khoá tự nhiên và tính duy nhất của event
    # --------------------------------------------------------------------------
    print("\n--- [U5] KHOÁ TỰ NHIÊN DUY NHẤT CHO MỖI EVENT ---")
    u5_tracking = con.execute("""
        SELECT
            count(*) as total_rows,
            count(distinct id) as distinct_ids,
            count(distinct create_time) as distinct_create_times,
            count(*) - count(distinct id) as duplicate_ids
        FROM read_csv_auto('data/raw/tracking.csv')
    """).fetchall()
    print(f"Tracking ID uniqueness: total={u5_tracking[0][0]}, distinct_id={u5_tracking[0][1]}, distinct_create_time={u5_tracking[0][2]}, dup_id={u5_tracking[0][3]}")

    u5_app = con.execute("""
        SELECT
            count(*) as total_rows,
            count(distinct id) as distinct_ids
        FROM read_csv_auto('data/raw/application.csv')
    """).fetchall()
    print(f"Application ID uniqueness: total={u5_app[0][0]}, distinct_id={u5_app[0][1]}")

    # --------------------------------------------------------------------------
    # U6: Dữ liệu null, âm, ngoại lai
    # --------------------------------------------------------------------------
    print("\n--- [U6] MẪU DỮ LIỆU NULL VÀ NGOẠI LAI TRONG TRACKING ---")
    u6_summary = con.execute("""
        SELECT
            count(*) filter (where job_id is null) as null_job,
            count(*) filter (where campaign_id is null) as null_campaign,
            count(*) filter (where publisher_id is null) as null_publisher,
            count(*) filter (where group_id is null) as null_group,
            count(*) filter (where bid is null or bid <= 0) as zero_or_null_bid,
            count(*) filter (where ts is null) as null_ts
        FROM read_csv_auto('data/raw/tracking.csv')
    """).fetchall()
    print(f"Tracking keys null summary: null_job={u6_summary[0][0]}, null_camp={u6_summary[0][1]}, null_pub={u6_summary[0][2]}, null_group={u6_summary[0][3]}, zero_or_null_bid={u6_summary[0][4]}, null_ts={u6_summary[0][5]}")

    # --------------------------------------------------------------------------
    # U7: Khoảng thời gian, số event/ngày, phân phối theo giờ
    # --------------------------------------------------------------------------
    print("\n--- [U7] PHÂN PHỐI SỰ KIỆN THEO NGÀY VÀ THEO GIỜ (THEO TS) ---")
    daily_dist = con.execute("""
        SELECT
            CAST(ts AS DATE) as event_date,
            count(*) as cnt
        FROM read_csv_auto('data/raw/tracking.csv')
        GROUP BY 1
        ORDER BY 1
    """).fetchall()
    print(f"Phân phối ngày theo cột 'ts' (tổng {len(daily_dist)} ngày):")
    for d in daily_dist:
        print(f"   {d[0]}: {d[1]:,} clicks")

    hourly_dist = con.execute("""
        SELECT
            EXTRACT(hour FROM ts) as hr,
            count(*) as cnt
        FROM read_csv_auto('data/raw/tracking.csv')
        WHERE ts IS NOT NULL
        GROUP BY 1
        ORDER BY 1
    """).fetchall()
    print("Phân phối sự kiện theo 24 giờ trong ngày:")
    for h in hourly_dist:
        print(f"   Hour {int(h[0]):02d}: {h[1]:,} clicks")

    # --------------------------------------------------------------------------
    # U8: So sánh search.csv vs job.csv & Khóa join
    # --------------------------------------------------------------------------
    print("\n--- [U8] SO SÁNH SEARCH.CSV VS JOB.CSV & KHOÁ JOIN VỚI TRACKING ---")
    join_job = con.execute("""
        SELECT
            count(*) as total_tracking,
            count(j.id) as matched_jobs,
            count(distinct t.job_id) as distinct_tracking_jobs,
            count(distinct j.id) as distinct_job_table_ids
        FROM read_csv_auto('data/raw/tracking.csv') t
        LEFT JOIN read_csv_auto('data/raw/job.csv') j ON CAST(t.job_id AS VARCHAR) = CAST(j.id AS VARCHAR)
    """).fetchall()
    print(f"Join tracking.job_id -> job.id: total={join_job[0][0]}, matched={join_job[0][1]} ({join_job[0][1]/join_job[0][0]*100:.1f}%), distinct_t_jobs={join_job[0][2]}, distinct_j_ids={join_job[0][3]}")

    join_search = con.execute("""
        SELECT
            count(*) as total_tracking,
            count(s.job_id) as matched_search_jobs,
            count(distinct s.job_id) as distinct_search_table_jobs
        FROM read_csv_auto('data/raw/tracking.csv') t
        LEFT JOIN read_csv_auto('data/raw/search.csv') s ON CAST(t.job_id AS VARCHAR) = CAST(s.job_id AS VARCHAR)
    """).fetchall()
    print(f"Join tracking.job_id -> search.job_id: total={join_search[0][0]}, matched={join_search[0][1]} ({join_search[0][1]/join_search[0][0]*100:.1f}%), distinct_s_jobs={join_search[0][2]}")

    # --------------------------------------------------------------------------
    # U9: campaign.csv có cột ngân sách (budget_total / daily_budget) không?
    # --------------------------------------------------------------------------
    print("\n--- [U9] THÔNG TIN CAMPAIGN & NGÂN SÁCH ---")
    camp_stats = con.execute("""
        SELECT
            count(*) as total_campaigns,
            count(budget_total) as campaigns_with_budget,
            min(budget_total) as min_budget,
            max(budget_total) as max_budget,
            avg(budget_total) as avg_budget,
            count(distinct pacing_unit) as distinct_pacing_units,
            count(distinct unit_budget) as distinct_unit_budgets
        FROM read_csv_auto('data/raw/campaign.csv')
    """).fetchall()
    print(f"Thống kê budget trong campaign.csv: {camp_stats}")

    pacing_units = con.execute("""
        SELECT pacing_unit, unit_budget, count(*)
        FROM read_csv_auto('data/raw/campaign.csv')
        GROUP BY 1, 2 ORDER BY 3 DESC
    """).fetchall()
    print(f"Phân loại pacing_unit & unit_budget trong campaign.csv: {pacing_units}")

    camp_bids = con.execute("""
        SELECT bid_set, count(*)
        FROM read_csv_auto('data/raw/campaign.csv')
        GROUP BY 1 ORDER BY 2 DESC LIMIT 5
    """).fetchall()
    print(f"bid_set trong campaign.csv: {camp_bids}")

    # Join tracking.campaign_id -> campaign.id
    join_camp = con.execute("""
        SELECT
            count(*) as total_tracking,
            count(c.id) as matched_campaigns,
            count(distinct t.campaign_id) as distinct_tracking_camps,
            count(distinct c.id) as distinct_camp_table_ids
        FROM read_csv_auto('data/raw/tracking.csv') t
        LEFT JOIN read_csv_auto('data/raw/campaign.csv') c ON CAST(t.campaign_id AS VARCHAR) = CAST(c.id AS VARCHAR)
    """).fetchall()
    print(f"Join tracking.campaign_id -> campaign.id: total={join_camp[0][0]}, matched={join_camp[0][1]} ({join_camp[0][1]/join_camp[0][0]*100:.1f}%), distinct_t_camps={join_camp[0][2]}, distinct_c_ids={join_camp[0][3]}")

    # Join tracking.publisher_id -> master_publisher.id
    join_pub = con.execute("""
        SELECT
            count(*) as total_tracking,
            count(p.id) as matched_publishers,
            count(distinct t.publisher_id) as distinct_tracking_pubs,
            count(distinct p.id) as distinct_pub_table_ids
        FROM read_csv_auto('data/raw/tracking.csv') t
        LEFT JOIN read_csv_auto('data/raw/master_publisher.csv') p ON CAST(t.publisher_id AS VARCHAR) = CAST(p.id AS VARCHAR)
    """).fetchall()
    print(f"Join tracking.publisher_id -> master_publisher.id: total={join_pub[0][0]}, matched={join_pub[0][1]} ({join_pub[0][1]/join_pub[0][0]*100:.1f}%), distinct_t_pubs={join_pub[0][2]}, distinct_p_ids={join_pub[0][3]}")

    # Check join tracking with application
    print("\n--- MỐI QUAN HỆ GIỮA APPLICATION.CSV VÀ TRACKING.CSV ---")
    app_join_check = con.execute("""
        SELECT
            count(a.id) as total_applications,
            count(distinct a.job_id) as distinct_app_jobs,
            count(distinct a.publisher_id) as distinct_app_pubs
        FROM read_csv_auto('data/raw/application.csv') a
    """).fetchall()
    print(f"Application key counts: {app_join_check}")

    app_match_tracking_job = con.execute("""
        SELECT
            count(distinct a.job_id) as app_jobs,
            count(distinct t.job_id) as tracking_jobs,
            count(distinct a.job_id) filter (where t.job_id is not null) as common_jobs
        FROM read_csv_auto('data/raw/application.csv') a
        LEFT JOIN read_csv_auto('data/raw/tracking.csv') t ON a.job_id = t.job_id
    """).fetchall()
    print(f"Job ID overlap giữa application và tracking: {app_match_tracking_job}")

    return verdicts


def main() -> None:
    """Main entrypoint."""
    con = duckdb.connect(database=":memory:")
    raw_files = sorted(glob.glob("data/raw/*.csv"))
    if not raw_files:
        print("❌ Không tìm thấy file CSV nào trong thư mục data/raw/!")
        sys.exit(1)

    print(f"🔍 Bắt đầu khảo sát {len(raw_files)} file CSV trong data/raw/...\n")
    run_phase_1(con, raw_files)
    run_phase_2(con)
    print("\n✅ Hoàn thành profiling toàn bộ 18 file CSV!")


if __name__ == "__main__":
    main()
