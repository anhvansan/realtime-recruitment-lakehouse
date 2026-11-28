"""Unit tests for DashboardRepository and all 5 SQL queries (Task 5.2, Metric Catalog 2.3)."""

from __future__ import annotations

import csv
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from deltalake import write_deltalake

from src.dashboard.db import create_root_connection
from src.dashboard.repository import AlertThresholds, DashboardRepository, DataSourcePaths


def _make_silver_table(events: list[dict[str, Any]]) -> pa.Table:
    return pa.table(
        {
            "schema_version": pa.array(["v1"] * len(events), type=pa.string()),
            "event_id": pa.array([e["event_id"] for e in events], type=pa.string()),
            "event_time": pa.array([e["event_time"] for e in events], type=pa.timestamp("us")),
            "produced_at": pa.array([e["produced_at"] for e in events], type=pa.timestamp("us")),
            "event_type": pa.array([e["event_type"] for e in events], type=pa.string()),
            "job_id": pa.array([e["job_id"] for e in events], type=pa.string()),
            "campaign_id": pa.array([e["campaign_id"] for e in events], type=pa.string()),
            "group_id": pa.array([e.get("group_id") for e in events], type=pa.string()),
            "publisher_id": pa.array([e["publisher_id"] for e in events], type=pa.string()),
            "bid": pa.array([e["bid"] for e in events], type=pa.decimal128(18, 4)),
            "spend": pa.array([e["spend"] for e in events], type=pa.decimal128(18, 4)),
            "source_row_id": pa.array([e["source_row_id"] for e in events], type=pa.int64()),
            "event_date": pa.array([e["event_date"] for e in events], type=pa.date32()),
            "processed_at": pa.array([e["processed_at"] for e in events], type=pa.timestamp("us")),
        }
    )


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8", newline="\n") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def _build_fixture_repo(tmp_path: Path) -> tuple[DashboardRepository, Any]:
    base_proc = datetime(2026, 1, 1, 10, 15, 5)
    events: list[dict[str, Any]] = []
    rid = 1

    def add_ev(
        eid: str,
        ts: datetime,
        etype: str,
        *,
        cid: str = "C1",
        jid: str = "J1",
        pid: str = "P1",
        spend: str = "0.0000",
        latency_s: int = 10,
    ) -> None:
        nonlocal rid
        bid = Decimal(spend) if etype == "click" else None
        events.append(
            {
                "event_id": eid,
                "event_time": ts,
                "produced_at": base_proc - timedelta(seconds=latency_s),
                "processed_at": base_proc,
                "event_type": etype,
                "job_id": jid,
                "campaign_id": cid,
                "publisher_id": pid,
                "bid": bid,
                "spend": Decimal(spend),
                "source_row_id": rid,
                "event_date": ts.date(),
            }
        )
        rid += 1

    # Section 2.3 Campaign C1 (daily_budget = 1000):
    # Hours 00..04 total = 280.0000 (skip hour 02 to verify continuous spine), hour 05 = 120.0000 -> cum_spend=400.0000
    add_ev("c1_h0", datetime(2026, 1, 1, 0, 15), "click", spend="100.0000", latency_s=5)
    add_ev("c1_h1", datetime(2026, 1, 1, 1, 20), "click", spend="80.0000", latency_s=10)
    add_ev("c1_h4", datetime(2026, 1, 1, 4, 45), "click", spend="100.0000", latency_s=15)
    add_ev("c1_h5", datetime(2026, 1, 1, 5, 30), "click", spend="120.0000", latency_s=20)
    # In-progress hour 10: sim_now = 10:15:00
    add_ev("c1_h10", datetime(2026, 1, 1, 10, 15), "click", spend="25.0000", latency_s=30)

    # Publisher quality trailing 60m ending at 10:15:00 (09:15 < ts <= 10:15):
    # P9: 130 clicks, 0 applies -> R1 = True
    t_win = datetime(2026, 1, 1, 9, 45)
    for i in range(130):
        add_ev(f"p9_c_{i}", t_win, "click", cid="C2", pid="P9", spend="1.0000", latency_s=10)
    # P_BOUND: 99 clicks, 0 applies -> R1 = False
    for i in range(99):
        add_ev(f"pb_c_{i}", t_win, "click", cid="C2", pid="P_BOUND", spend="1.0000", latency_s=10)
    # Healthy peers for R2 median
    for pid, applies in (("P_G1", 25), ("P_G2", 15), ("P_G3", 20), ("P_G4", 18)):
        for i in range(50):
            add_ev(f"{pid}_c_{i}", t_win, "click", cid="C2", pid=pid, spend="1.0000", latency_s=10)
        for i in range(applies):
            add_ev(f"{pid}_a_{i}", t_win, "apply", cid="C2", pid=pid, spend="0.0000", latency_s=10)
    # P_LOW: 60 clicks, 1 apply -> CVR ≈ 0.0167 < 0.2 * median -> R2 = True
    for i in range(60):
        add_ev(f"pl_c_{i}", t_win, "click", cid="C2", pid="P_LOW", spend="1.0000", latency_s=10)
    add_ev("pl_a_0", t_win, "apply", cid="C2", pid="P_LOW", spend="0.0000", latency_s=10)
    # P_ZERO: 0 clicks, 2 applies -> click_to_apply_rate IS NULL
    add_ev("pz_a_0", t_win, "apply", cid="C2", pid="P_ZERO", spend="0.0000", latency_s=10)
    add_ev("pz_a_1", t_win, "apply", cid="C2", pid="P_ZERO", spend="0.0000", latency_s=10)

    silver_dir = tmp_path / "silver"
    write_deltalake(str(silver_dir), _make_silver_table(events), partition_by=["event_date"])

    # Bronze: Silver + 5 duplicate rows -> dedup_rate = 5 / (silver + 5)
    bronze_events = events + [dict(events[0]), dict(events[1]), dict(events[2]), dict(events[3]), dict(events[4])]
    bronze_dir = tmp_path / "bronze"
    write_deltalake(str(bronze_dir), _make_silver_table(bronze_events), partition_by=["event_date"])

    # DLQ: 2 invalid rows
    dlq_dir = tmp_path / "dlq"
    dlq_table = pa.table(
        {
            "payload_raw": pa.array(["x" * 220, '{"bad": 1}'], type=pa.string()),
            "error_type": pa.array(["corrupt_json", "missing_required"], type=pa.string()),
            "error_detail": pa.array(["Invalid JSON", "Missing field"], type=pa.string()),
            "source_topic": pa.array(["cpc_events_raw", "cpc_events_raw"], type=pa.string()),
            "source_partition": pa.array([0, 1], type=pa.int32()),
            "source_offset": pa.array([10, 11], type=pa.int64()),
            "failed_at": pa.array([base_proc, base_proc], type=pa.timestamp("us")),
            "stage": pa.array(["ingest", "ingest"], type=pa.string()),
        }
    )
    write_deltalake(str(dlq_dir), dlq_table)

    # Gold efficiency parquet fixture
    gold_eff_path = tmp_path / "fct_recruitment_efficiency.parquet"
    eff_table = pa.table(
        {
            "job_id": pa.array(["J1", "J2"], type=pa.string()),
            "job_title": pa.array(["Data Engineer", "Zero Job"], type=pa.string()),
            "campaign_id": pa.array(["C1", "C1"], type=pa.string()),
            "campaign_name": pa.array(["Campaign C1", "Campaign C1"], type=pa.string()),
            "company_id": pa.array(["COMP1", "COMP1"], type=pa.string()),
            "company_name": pa.array(["HireBoost", "HireBoost"], type=pa.string()),
            "event_date": pa.array([date(2026, 1, 1), date(2026, 1, 1)], type=pa.date32()),
            "spend": pa.array([Decimal("200.0000"), Decimal("0.0000")], type=pa.decimal128(18, 4)),
            "clicks": pa.array([100, 0], type=pa.int64()),
            "applies": pa.array([20, 1], type=pa.int64()),
            "qualified": pa.array([5, 0], type=pa.int64()),
            "unqualified": pa.array([2, 0], type=pa.int64()),
            "cpc": pa.array([Decimal("2.0000"), None], type=pa.decimal128(18, 4)),
            "cpa": pa.array([Decimal("10.0000"), Decimal("0.0000")], type=pa.decimal128(18, 4)),
            "cpqa": pa.array([Decimal("40.0000"), None], type=pa.decimal128(18, 4)),
            "qualified_rate": pa.array([0.25, 0.0], type=pa.float64()),
            "click_to_apply_rate": pa.array([0.20, None], type=pa.float64()),
        }
    )
    pq.write_table(eff_table, str(gold_eff_path))

    budget_csv = tmp_path / "seed_campaign_budget.csv"
    _write_csv(
        budget_csv,
        ["campaign_id", "campaign_name", "daily_budget", "is_outlier_budget", "budget_source"],
        [
            {
                "campaign_id": "C1",
                "campaign_name": "Campaign C1",
                "daily_budget": "1000.0000",
                "is_outlier_budget": "false",
                "budget_source": "campaign.csv",
            },
            {
                "campaign_id": "C2",
                "campaign_name": "Campaign C2",
                "daily_budget": "500.0000",
                "is_outlier_budget": "false",
                "budget_source": "campaign.csv",
            },
        ],
    )

    publisher_csv = tmp_path / "seed_publisher.csv"
    _write_csv(
        publisher_csv,
        ["publisher_id", "publisher_name"],
        [
            {"publisher_id": "P9", "publisher_name": "Pub Nine"},
            {"publisher_id": "P_LOW", "publisher_name": "Pub Low"},
        ],
    )

    con = create_root_connection(configure_s3=False)
    repo = DashboardRepository(
        paths=DataSourcePaths(
            silver_path=silver_dir.as_posix(),
            bronze_path=bronze_dir.as_posix(),
            dlq_path=dlq_dir.as_posix(),
            gold_efficiency_path=gold_eff_path.as_posix(),
            budget_csv=budget_csv.as_posix(),
            publisher_csv=publisher_csv.as_posix(),
        ),
        thresholds=AlertThresholds(),
        root_conn=con,
    )
    return repo, con


def test_all_five_dashboard_queries_against_section_2_3_examples(tmp_path: Path) -> None:
    repo, con = _build_fixture_repo(tmp_path)
    try:
        # 1. sim_now.sql
        sim_info = repo.get_sim_now()
        assert sim_info["sim_now"] == datetime(2026, 1, 1, 10, 15, 0)
        assert sim_info["sim_date"] == date(2026, 1, 1)
        assert sim_info["total_silver_rows"] == 575

        # 2. pacing.sql (Section 2.3 closed hour 05 & in-progress hour 10:15:00)
        pacing_df = repo.get_pacing(sim_now=sim_info["sim_now"])
        c1 = pacing_df[pacing_df["campaign_id"] == "C1"].copy()
        c1["hour"] = pd.to_datetime(c1["hour_ts"]).dt.hour
        by_h = c1.set_index("hour")

        assert len(c1) == 11  # hours 00..10 continuous spine
        assert Decimal(str(by_h.loc[2, "spend_hour"])) == Decimal("0.0000")
        assert Decimal(str(by_h.loc[2, "cum_spend"])) == Decimal("180.0000")

        assert Decimal(str(by_h.loc[5, "cum_spend"])) == Decimal("400.0000")
        assert Decimal(str(by_h.loc[5, "ideal_cum_spend"])) == Decimal("250.0000")
        assert float(by_h.loc[5, "pacing_ratio"]) == pytest.approx(1.6, rel=1e-6)
        assert float(by_h.loc[5, "budget_utilization"]) == pytest.approx(0.40, rel=1e-6)
        assert Decimal(str(by_h.loc[5, "burn_rate"])) == Decimal("120.0000")
        assert Decimal(str(by_h.loc[5, "projected_eod_spend"])) == Decimal("2560.0000")

        assert Decimal(str(by_h.loc[10, "ideal_cum_spend"])) == Decimal("427.0833")

        # Verify EXPLAIN ANALYZE shows event_date filter pushdown
        plan = repo.explain_pacing(sim_now=sim_info["sim_now"])
        assert "event_date" in plan

        # 3. publisher_quality.sql (Section 2.3 P9=130 vs 99, R2, zero denominator -> NULL)
        pq_df = repo.get_publisher_quality(sim_now=sim_info["sim_now"]).set_index("publisher_id")
        assert pq_df.loc["P9", "publisher_name"] == "Pub Nine"
        assert bool(pq_df.loc["P9", "fraud_flag_r1"]) is True
        assert bool(pq_df.loc["P_BOUND", "fraud_flag_r1"]) is False
        assert bool(pq_df.loc["P_LOW", "fraud_flag_r2"]) is True
        assert bool(pq_df.loc["P_G1", "fraud_flag_r2"]) is False
        assert pd.isna(pq_df.loc["P_ZERO", "click_to_apply_rate_60m"])

        # 4. funnel.sql
        funnel_df = repo.get_funnel_efficiency(event_date=date(2026, 1, 1)).set_index("job_id")
        assert Decimal(str(funnel_df.loc["J1", "cpqa"])) == Decimal("40.0000")
        assert float(funnel_df.loc["J1", "qualified_rate"]) == pytest.approx(0.25)
        assert pd.isna(funnel_df.loc["J2", "cpc"])
        assert pd.isna(funnel_df.loc["J2", "cpqa"])
        assert pd.isna(funnel_df.loc["J2", "click_to_apply_rate"])

        # 5. health.sql + DLQ breakdown & samples
        health_df = repo.get_pipeline_health()
        hrow = health_df.iloc[0]
        assert int(hrow["silver_rows"]) == 575
        assert int(hrow["bronze_rows"]) == 580
        assert int(hrow["dlq_rows"]) == 2
        assert float(hrow["dedup_rate"]) == pytest.approx(5.0 / 580.0, rel=1e-6)
        assert float(hrow["dlq_rate"]) == pytest.approx(2.0 / 582.0, rel=1e-6)
        assert float(hrow["e2e_latency_p50_s"]) == pytest.approx(10.0, rel=1e-6)
        assert float(hrow["e2e_latency_p95_s"]) >= 10.0

        dlq_bd = repo.get_dlq_breakdown()
        assert len(dlq_bd) == 2
        dlq_samples = repo.get_dlq_samples(limit=10)
        assert len(dlq_samples) == 2
        assert max(len(s) for s in dlq_samples["payload_preview"]) <= 160
    finally:
        con.close()
        assert datetime.now(timezone.utc) is not None
