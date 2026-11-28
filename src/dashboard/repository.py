"""Dashboard & Alerting query repository executing parameterized DuckDB SQL (Task 5.2).

Reads Silver/Bronze/DLQ Delta tables and Gold Parquet directly from MinIO (or local
Delta/Parquet fixtures in unit tests) via ``src/dashboard/db.py:run_query()``.
Follows Rule R9 (Metric Catalog) and Rule R6 (no credential logging).
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd
import yaml

from src.common.config import AppConfig, load
from src.dashboard.db import run_query

_QUERIES_DIR = Path(__file__).resolve().parent / "queries"
_DEFAULT_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


@dataclass(frozen=True)
class AlertThresholds:
    """Alerting & fraud heuristic thresholds loaded from ``config/alerts.yaml``."""

    pacing_fast: float = 1.2
    pacing_slow: float = 0.6
    budget_utilization_warning: float = 0.90
    data_stale_seconds: int = 120
    click_fraud_min_clicks: int = 100
    click_fraud_max_applies: int = 0
    click_fraud_r2_min_clicks: int = 50
    click_fraud_r2_median_factor: float = 0.2

    @classmethod
    def from_yaml(cls, yaml_path: Path | None = None) -> AlertThresholds:
        target = yaml_path or (_DEFAULT_PROJECT_ROOT / "config" / "alerts.yaml")
        if not target.exists():
            return cls()
        with target.open("r", encoding="utf-8") as fh:
            raw = yaml.safe_load(fh) or {}
        t = raw.get("thresholds", {})
        return cls(
            pacing_fast=float(t.get("pacing_fast", 1.2)),
            pacing_slow=float(t.get("pacing_slow", 0.6)),
            budget_utilization_warning=float(t.get("budget_utilization_warning", 0.90)),
            data_stale_seconds=int(t.get("data_stale_seconds", 120)),
            click_fraud_min_clicks=int(t.get("click_fraud_min_clicks", 100)),
            click_fraud_max_applies=int(t.get("click_fraud_max_applies", 0)),
            click_fraud_r2_min_clicks=int(t.get("click_fraud_r2_min_clicks", 50)),
            click_fraud_r2_median_factor=float(t.get("click_fraud_r2_median_factor", 0.2)),
        )


@dataclass(frozen=True)
class DataSourcePaths:
    """Resolved Lakehouse table URIs and local seed CSV paths for repository queries."""

    silver_path: str
    bronze_path: str
    dlq_path: str
    gold_efficiency_path: str
    budget_csv: str
    publisher_csv: str

    @classmethod
    def from_config(
        cls,
        cfg: AppConfig,
        *,
        project_root: Path | None = None,
    ) -> DataSourcePaths:
        root = project_root or _DEFAULT_PROJECT_ROOT
        return cls(
            silver_path=cfg.s3_uri(cfg.pipeline.storage.silver_path),
            bronze_path=cfg.s3_uri(cfg.pipeline.storage.bronze_path),
            dlq_path=cfg.s3_uri(cfg.pipeline.storage.dlq_path),
            gold_efficiency_path=cfg.s3_uri("gold/fct_recruitment_efficiency.parquet"),
            budget_csv=(root / "dbt_cpc" / "seeds" / "seed_campaign_budget.csv").as_posix(),
            publisher_csv=(root / "dbt_cpc" / "seeds" / "seed_publisher.csv").as_posix(),
        )


def _load_sql(filename: str) -> str:
    return (_QUERIES_DIR / filename).read_text(encoding="utf-8")


def _safe_sql_path(path_value: str) -> str:
    return path_value.replace("'", "''")


class DashboardRepository:
    """Query layer serving both Streamlit Dashboard panels and Alert Worker."""

    def __init__(
        self,
        *,
        cfg: AppConfig | None = None,
        paths: DataSourcePaths | None = None,
        thresholds: AlertThresholds | None = None,
        root_conn: duckdb.DuckDBPyConnection | None = None,
    ) -> None:
        self.cfg = cfg or load()
        self.paths = paths or DataSourcePaths.from_config(self.cfg)
        self.thresholds = thresholds or AlertThresholds.from_yaml()
        self.root_conn = root_conn
        self._s3_client: Any = None

    def _render_sql(self, filename: str) -> str:
        template = _load_sql(filename)
        return template.format(
            silver_path=_safe_sql_path(self.paths.silver_path),
            bronze_path=_safe_sql_path(self.paths.bronze_path),
            dlq_path=_safe_sql_path(self.paths.dlq_path),
            gold_efficiency_path=_safe_sql_path(self.paths.gold_efficiency_path),
            budget_csv=_safe_sql_path(self.paths.budget_csv),
            publisher_csv=_safe_sql_path(self.paths.publisher_csv),
        )

    def get_sim_now(self) -> dict[str, Any]:
        """Return ``sim_now = MAX(event_time)``, ``sim_date``, ``max_processed_at``, and row count."""
        df = run_query(self._render_sql("sim_now.sql"), root_conn=self.root_conn)
        if df.empty or pd.isna(df.iloc[0]["sim_now"]):
            return {
                "sim_now": None,
                "sim_date": None,
                "max_processed_at": None,
                "total_silver_rows": 0,
            }
        row = df.iloc[0]
        sim_now_val = pd.Timestamp(row["sim_now"]).to_pydatetime()
        sim_date_val = pd.Timestamp(row["sim_date"]).date()
        max_proc_val = (
            pd.Timestamp(row["max_processed_at"]).to_pydatetime()
            if not pd.isna(row["max_processed_at"])
            else None
        )
        return {
            "sim_now": sim_now_val,
            "sim_date": sim_date_val,
            "max_processed_at": max_proc_val,
            "total_silver_rows": int(row["total_silver_rows"]),
        }

    def get_pacing(
        self,
        sim_now: datetime | None = None,
        event_date: date | None = None,
    ) -> pd.DataFrame:
        """Compute hourly campaign pacing for ``event_date`` (defaults to ``sim_now`` date)."""
        resolved_sim_now = sim_now
        if resolved_sim_now is None:
            info = self.get_sim_now()
            resolved_sim_now = info["sim_now"]
        if resolved_sim_now is None:
            return pd.DataFrame()

        target_date = event_date or resolved_sim_now.date()
        sql = self._render_sql("pacing.sql")
        params = [resolved_sim_now, target_date, target_date, target_date, target_date]
        return run_query(sql, params, root_conn=self.root_conn)

    def explain_pacing(
        self,
        sim_now: datetime | None = None,
        event_date: date | None = None,
    ) -> str:
        """Run ``EXPLAIN ANALYZE`` on ``pacing.sql`` to verify ``event_date`` filter pushdown."""
        resolved_sim_now = sim_now or self.get_sim_now()["sim_now"]
        if resolved_sim_now is None:
            return ""
        target_date = event_date or resolved_sim_now.date()
        sql = f"EXPLAIN ANALYZE {self._render_sql('pacing.sql')}"
        params = [resolved_sim_now, target_date, target_date, target_date, target_date]
        df = run_query(sql, params, root_conn=self.root_conn)
        if df.empty:
            return ""
        return "\n".join(str(val) for val in df.iloc[:, -1].tolist())

    def get_publisher_quality(self, sim_now: datetime | None = None) -> pd.DataFrame:
        """Compute trailing 60-minute publisher quality & fraud flags ending at ``sim_now``."""
        resolved_sim_now = sim_now
        if resolved_sim_now is None:
            resolved_sim_now = self.get_sim_now()["sim_now"]
        if resolved_sim_now is None:
            return pd.DataFrame()

        sql = self._render_sql("publisher_quality.sql")
        params = [
            resolved_sim_now,
            self.thresholds.click_fraud_min_clicks,
            self.thresholds.click_fraud_max_applies,
            self.thresholds.click_fraud_r2_min_clicks,
            self.thresholds.click_fraud_r2_median_factor,
        ]
        return run_query(sql, params, root_conn=self.root_conn)

    def get_funnel_efficiency(self, event_date: date | None = None) -> pd.DataFrame:
        """Read P3 recruitment efficiency metrics from Gold Parquet."""
        sql = self._render_sql("funnel.sql")
        return run_query(sql, [event_date, event_date], root_conn=self.root_conn)

    def get_pipeline_health(self, since_date: date | None = None) -> pd.DataFrame:
        """Compute P4 pipeline freshness, e2e latency p50/p95, dedup_rate, and dlq_rate."""
        sql = self._render_sql("health.sql")
        params = [since_date, since_date, since_date, since_date]
        return run_query(sql, params, root_conn=self.root_conn)

    def get_dlq_breakdown(self) -> pd.DataFrame:
        """Aggregate DLQ counts by ``error_type`` and ``stage``."""
        sql = f"""
        select
            cast(error_type as varchar) as error_type,
            cast(stage as varchar) as stage,
            count(*) as error_count,
            max(cast(failed_at as timestamp)) as last_failed_at
        from delta_scan('{_safe_sql_path(self.paths.dlq_path)}')
        group by 1, 2
        order by error_count desc, error_type asc
        """
        return run_query(sql, root_conn=self.root_conn)

    def get_dlq_samples(self, limit: int = 20) -> pd.DataFrame:
        """Return recent DLQ rows with truncated ``payload_raw`` for safe inspection."""
        sql = f"""
        select
            cast(error_type as varchar) as error_type,
            cast(error_detail as varchar) as error_detail,
            cast(stage as varchar) as stage,
            cast(source_partition as integer) as source_partition,
            cast(source_offset as bigint) as source_offset,
            cast(failed_at as timestamp) as failed_at,
            left(cast(payload_raw as varchar), 160) as payload_preview
        from delta_scan('{_safe_sql_path(self.paths.dlq_path)}')
        order by failed_at desc
        limit ?
        """
        return run_query(sql, [int(limit)], root_conn=self.root_conn)

    def _get_s3_client(self) -> Any:
        """Lazily initialize and cache the S3 client per repository instance."""
        if self._s3_client is None:
            import boto3
            from botocore.config import Config as BotoConfig

            endpoint = (
                self.cfg.s3_endpoint
                if Path("/.dockerenv").exists()
                or os.environ.get("S3_ENDPOINT_HOST_PORT") == "minio:9000"
                else self.cfg.s3_endpoint_external
            )
            self._s3_client = boto3.client(
                "s3",
                endpoint_url=endpoint,
                aws_access_key_id=self.cfg.s3_access_key,
                aws_secret_access_key=self.cfg.s3_secret_key,
                region_name=self.cfg.s3_region,
                config=BotoConfig(s3={"addressing_style": "path"}),
            )
        return self._s3_client

    def _read_s3_json(self, key: str) -> dict[str, Any] | None:
        """Read an optional JSON metadata object from MinIO without logging credentials."""
        from botocore.exceptions import ClientError

        s3 = self._get_s3_client()
        try:
            obj = s3.get_object(Bucket=self.cfg.s3_bucket, Key=key)
            return dict(json.loads(obj["Body"].read().decode("utf-8")))
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code in {"NoSuchKey", "404"}:
                return None
            raise

    def get_ops_latest(self) -> dict[str, Any] | None:
        """Fetch Spark StreamingQueryListener ``ops/latest.json`` from MinIO if present."""
        return self._read_s3_json("ops/latest.json")

    def get_gold_last_run(self) -> dict[str, Any] | None:
        """Fetch dbt loop ``gold/_last_run.json`` metadata from MinIO if present."""
        return self._read_s3_json("gold/_last_run.json")
