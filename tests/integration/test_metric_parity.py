"""Integration test: Fast Path (Dashboard pacing.sql) vs Deep Path (dbt Gold) parity (Task 4.7).

Builds a local partitioned Delta Lake table via ``deltalake`` (no Docker or Spark required,
so it runs cleanly in GitHub Actions CI) covering both closed hours and an in-progress hour
at ``sim_now``, then asserts <= 1e-6 divergence across all pacing metrics.
"""

from __future__ import annotations

import csv
import re
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pytest
from deltalake import write_deltalake

from src.dashboard.db import create_root_connection
from src.dashboard.repository import AlertThresholds, DashboardRepository, DataSourcePaths

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
GOLD_PACING_SQL = (
    PROJECT_ROOT / "dbt_cpc" / "models" / "gold" / "fct_campaign_hourly_pacing.sql"
)


def _render_deep_path_gold_sql(silver_dir: Path, budget_csv: Path) -> str:
    """Render the actual dbt Gold pacing SQL against local DuckDB views over Delta + seed CSV."""
    raw = GOLD_PACING_SQL.read_text(encoding="utf-8")
    stripped = re.sub(r"\{\{\s*config\(.*?\)\s*\}\}", "", raw, flags=re.DOTALL)
    rendered = re.sub(r"\{\{\s*ref\(['\"]([^'\"]+)['\"]\)\s*\}\}", r"\1", stripped)
    silver_sql = silver_dir.as_posix().replace("'", "''")
    budget_sql = budget_csv.as_posix().replace("'", "''")

    return f"""
    with stg_cpc_events as (
        select
            cast(event_id as varchar) as event_id,
            cast(event_time as timestamp) as event_time,
            date_trunc('hour', cast(event_time as timestamp)) as event_hour,
            cast(produced_at as timestamp) as produced_at,
            cast(processed_at as timestamp) as processed_at,
            cast(event_date as date) as event_date,
            cast(event_type as varchar) as event_type,
            cast(job_id as varchar) as job_id,
            cast(campaign_id as varchar) as campaign_id,
            cast(group_id as varchar) as group_id,
            cast(publisher_id as varchar) as publisher_id,
            cast(bid as decimal(18, 4)) as bid,
            cast(spend as decimal(18, 4)) as spend,
            cast(source_row_id as bigint) as source_row_id
        from delta_scan('{silver_sql}')
    ),
    dim_campaign_budget as (
        select
            cast(campaign_id as varchar) as campaign_id,
            cast(campaign_name as varchar) as campaign_name,
            cast(daily_budget as decimal(18, 4)) as daily_budget,
            cast(is_outlier_budget as boolean) as is_outlier_budget,
            cast(budget_source as varchar) as budget_source
        from read_csv_auto('{budget_sql}', header = true, all_varchar = true)
    ),
    gold_result as (
        {rendered}
    )
    select * from gold_result
    order by campaign_id, hour_ts
    """


def test_fast_path_and_deep_path_metric_parity(tmp_path: Path) -> None:
    events: list[dict[str, Any]] = []
    rid = 1

    def add_ev(
        eid: str,
        ts: datetime,
        cid: str,
        spend: str,
        etype: str = "click",
    ) -> None:
        nonlocal rid
        events.append(
            {
                "schema_version": "v1",
                "event_id": eid,
                "event_time": ts,
                "produced_at": ts,
                "processed_at": ts,
                "event_type": etype,
                "job_id": "J1",
                "campaign_id": cid,
                "group_id": None,
                "publisher_id": "P1",
                "bid": Decimal(spend) if etype == "click" else None,
                "spend": Decimal(spend),
                "source_row_id": rid,
                "event_date": ts.date(),
            }
        )
        rid += 1

    # Campaign C1 (Section 2.3): closed hours 00..05 + in-progress hour 10 at sim_now = 10:15:00
    add_ev("c1_h0", datetime(2026, 1, 1, 0, 10), "C1", "100.0000")
    add_ev("c1_h1", datetime(2026, 1, 1, 1, 20), "C1", "80.0000")
    add_ev("c1_h4", datetime(2026, 1, 1, 4, 40), "C1", "100.0000")
    add_ev("c1_h5", datetime(2026, 1, 1, 5, 50), "C1", "120.0000")
    add_ev("c1_h9", datetime(2026, 1, 1, 9, 30), "C1", "45.5000")
    add_ev("c1_h10", datetime(2026, 1, 1, 10, 15), "C1", "30.2500")

    # Campaign C2: sparse hours (03 and 08 only, 0 in trailing 60m of sim_now) + non-click apply
    add_ev("c2_h3", datetime(2026, 1, 1, 3, 5), "C2", "200.0000")
    add_ev("c2_h8", datetime(2026, 1, 1, 8, 25), "C2", "150.7500")
    add_ev("c2_h9_apply", datetime(2026, 1, 1, 9, 55), "C2", "0.0000", etype="apply")

    silver_dir = tmp_path / "silver_parity"
    table = pa.table(
        {
            "schema_version": pa.array([e["schema_version"] for e in events], type=pa.string()),
            "event_id": pa.array([e["event_id"] for e in events], type=pa.string()),
            "event_time": pa.array([e["event_time"] for e in events], type=pa.timestamp("us")),
            "produced_at": pa.array([e["produced_at"] for e in events], type=pa.timestamp("us")),
            "event_type": pa.array([e["event_type"] for e in events], type=pa.string()),
            "job_id": pa.array([e["job_id"] for e in events], type=pa.string()),
            "campaign_id": pa.array([e["campaign_id"] for e in events], type=pa.string()),
            "group_id": pa.array([e["group_id"] for e in events], type=pa.string()),
            "publisher_id": pa.array([e["publisher_id"] for e in events], type=pa.string()),
            "bid": pa.array([e["bid"] for e in events], type=pa.decimal128(18, 4)),
            "spend": pa.array([e["spend"] for e in events], type=pa.decimal128(18, 4)),
            "source_row_id": pa.array([e["source_row_id"] for e in events], type=pa.int64()),
            "event_date": pa.array([e["event_date"] for e in events], type=pa.date32()),
            "processed_at": pa.array([e["processed_at"] for e in events], type=pa.timestamp("us")),
        }
    )
    write_deltalake(str(silver_dir), table, partition_by=["event_date"])

    budget_csv = tmp_path / "seed_campaign_budget.csv"
    with budget_csv.open("w", encoding="utf-8", newline="\n") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=[
                "campaign_id",
                "campaign_name",
                "daily_budget",
                "is_outlier_budget",
                "budget_source",
            ],
            lineterminator="\n",
        )
        writer.writeheader()
        writer.writerows(
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
                    "daily_budget": "2000.0000",
                    "is_outlier_budget": "false",
                    "budget_source": "campaign.csv",
                },
            ]
        )

    con = create_root_connection(configure_s3=False)
    try:
        repo = DashboardRepository(
            paths=DataSourcePaths(
                silver_path=silver_dir.as_posix(),
                bronze_path=silver_dir.as_posix(),
                dlq_path=silver_dir.as_posix(),
                gold_efficiency_path=(tmp_path / "unused.parquet").as_posix(),
                budget_csv=budget_csv.as_posix(),
                publisher_csv=budget_csv.as_posix(),
            ),
            thresholds=AlertThresholds(),
            root_conn=con,
        )

        fast_df = repo.get_pacing(
            sim_now=datetime(2026, 1, 1, 10, 15, 0),
            event_date=date(2026, 1, 1),
        )
        deep_df = con.execute(_render_deep_path_gold_sql(silver_dir, budget_csv)).df()

        assert len(fast_df) == len(deep_df) == 22  # 2 campaigns * 11 hours (00..10)

        key_cols = ["campaign_id", "event_date", "hour_ts"]
        merged = pd.merge(
            fast_df,
            deep_df,
            on=key_cols,
            suffixes=("_fast", "_deep"),
            how="inner",
        )
        assert len(merged) == 22

        metrics = (
            "spend_hour",
            "cum_spend",
            "ideal_cum_spend",
            "pacing_ratio",
            "budget_utilization",
            "burn_rate",
            "projected_eod_spend",
        )
        for _, row in merged.iterrows():
            for col in metrics:
                v_fast = float(row[f"{col}_fast"])
                v_deep = float(row[f"{col}_deep"])
                assert v_fast == pytest.approx(v_deep, abs=1e-6), (
                    f"Parity mismatch at {row['campaign_id']} {row['hour_ts']} "
                    f"col={col}: fast={v_fast} vs deep={v_deep}"
                )
    finally:
        con.close()
