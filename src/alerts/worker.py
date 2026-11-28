"""Standalone Alert Worker evaluating Lakehouse rules via DashboardRepository (Task 5.8).

Reuses ``src/dashboard/repository.py`` (no duplicated SQL) and ``config/alerts.yaml``.
Implements cooldown deduplication, structured JSON stdout logging, S3 JSONL history
(``ops/alerts.jsonl``), and optional credential-safe webhook notifications (Rule R6).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import math
import os
import sys
import tempfile
import time
import urllib.error
import urllib.request
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from src.dashboard.db import DatabaseConnectionError
from src.dashboard.repository import AlertThresholds, DashboardRepository

logger = logging.getLogger(__name__)

_DEFAULT_ALERTS_YAML = Path(__file__).resolve().parent.parent.parent / "config" / "alerts.yaml"
_DEFAULT_STATE_FILE = Path(tempfile.gettempdir()) / "cpc_lakehouse_alert_state.json"


@dataclass(frozen=True)
class AlertWorkerConfig:
    """Alert Worker thresholds and channel configuration from ``config/alerts.yaml``."""

    thresholds: AlertThresholds
    dlq_rate_threshold: float = 0.05
    cooldown_seconds: int = 300
    dry_run: bool = True

    @classmethod
    def from_yaml(cls, yaml_path: Path | None = None) -> AlertWorkerConfig:
        target = yaml_path or _DEFAULT_ALERTS_YAML
        thresholds = AlertThresholds.from_yaml(target)
        dlq_rate_threshold = 0.05
        cooldown_seconds = 300
        dry_run = True

        if target.exists():
            with target.open("r", encoding="utf-8") as fh:
                raw = yaml.safe_load(fh) or {}
            th = raw.get("thresholds", {})
            ch = raw.get("channels", {})
            dlq_rate_threshold = float(th.get("dlq_rate_threshold", 0.05))
            cooldown_seconds = int(ch.get("cooldown_seconds", 300))
            dry_run = bool(ch.get("dry_run", True))

        env_dry_run = os.environ.get("ALERT_DRY_RUN")
        if env_dry_run is not None:
            dry_run = env_dry_run.strip().lower() in {"1", "true", "yes", "on"}

        return cls(
            thresholds=thresholds,
            dlq_rate_threshold=dlq_rate_threshold,
            cooldown_seconds=cooldown_seconds,
            dry_run=dry_run,
        )


@dataclass(frozen=True)
class Alert:
    """Structured alert event produced by ``evaluate_alerts``."""

    alert_id: str
    rule: str
    severity: str
    entity_id: str
    message: str
    sim_now: str | None
    fired_at: str
    value: float | None = None
    threshold: float | None = None

    def dedup_key(self) -> str:
        """Return deduplication key ``(rule, entity_id)`` for cooldown filtering."""
        return f"{self.rule}::{self.entity_id}"

    def to_dict(self) -> dict[str, Any]:
        """Serialize alert to JSON-friendly dictionary."""
        return asdict(self)


def _utc_iso(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _make_alert(
    *,
    rule: str,
    severity: str,
    entity_id: str,
    message: str,
    sim_now_iso: str | None,
    fired_at_iso: str,
    value: float | None = None,
    threshold: float | None = None,
) -> Alert:
    digest = hashlib.sha256(f"{rule}:{entity_id}:{fired_at_iso}".encode()).hexdigest()[:12]
    return Alert(
        alert_id=f"{rule}-{digest}",
        rule=rule,
        severity=severity,
        entity_id=entity_id,
        message=message,
        sim_now=sim_now_iso,
        fired_at=fired_at_iso,
        value=value,
        threshold=threshold,
    )


def _to_finite_float(val: Any) -> float | None:
    if val is None or isinstance(val, bool) or pd.isna(val):
        return None
    try:
        num = float(val)
    except (TypeError, ValueError):
        return None
    return num if math.isfinite(num) else None


def evaluate_alerts(
    *,
    pacing_df: pd.DataFrame,
    publisher_df: pd.DataFrame,
    health_df: pd.DataFrame,
    thresholds: AlertThresholds,
    sim_now: datetime | None = None,
    dlq_rate_threshold: float = 0.05,
    exclude_outlier_budgets: bool = True,
    fired_at: datetime | None = None,
) -> list[Alert]:
    """Pure rule evaluator over repository DataFrames (Pacing, Publisher, Freshness, DLQ)."""
    fired_ts = fired_at or datetime.now(timezone.utc)
    fired_at_iso = _utc_iso(fired_ts) or ""
    sim_now_iso = _utc_iso(sim_now)
    alerts: list[Alert] = []

    # 1. Pacing & budget utilization rules (latest hour snapshot per campaign)
    if pacing_df is not None and not pacing_df.empty:
        work_pacing = pacing_df.copy()
        if exclude_outlier_budgets and "is_outlier_budget" in work_pacing.columns:
            work_pacing = work_pacing[~work_pacing["is_outlier_budget"].fillna(False)].copy()

        if not work_pacing.empty:
            sort_cols = [c for c in ("campaign_id", "hour_ts") if c in work_pacing.columns]
            if sort_cols:
                work_pacing = work_pacing.sort_values(sort_cols)
            latest = work_pacing.groupby("campaign_id", as_index=False).tail(1)

            for _, row in latest.iterrows():
                cid = str(row["campaign_id"])
                cname = str(row.get("campaign_name", cid))
                entity = f"campaign:{cid}"

                util = _to_finite_float(row.get("budget_utilization"))
                if util is not None and util >= thresholds.budget_utilization_warning:
                    alerts.append(
                        _make_alert(
                            rule="budget_utilization_high",
                            severity="warning",
                            entity_id=entity,
                            message=(
                                f"Campaign {cname} ({cid}) used {util * 100:.1f}% of daily budget "
                                f"(threshold >= {thresholds.budget_utilization_warning * 100:.0f}%)."
                            ),
                            sim_now_iso=sim_now_iso,
                            fired_at_iso=fired_at_iso,
                            value=round(util, 6),
                            threshold=thresholds.budget_utilization_warning,
                        )
                    )

                pacing_ratio = _to_finite_float(row.get("pacing_ratio"))
                if pacing_ratio is not None and pacing_ratio > thresholds.pacing_fast:
                    alerts.append(
                        _make_alert(
                            rule="pacing_fast",
                            severity="warning",
                            entity_id=entity,
                            message=(
                                f"Campaign {cname} ({cid}) pacing too fast: {pacing_ratio:.2f}x "
                                f"(threshold > {thresholds.pacing_fast:.2f}x)."
                            ),
                            sim_now_iso=sim_now_iso,
                            fired_at_iso=fired_at_iso,
                            value=round(pacing_ratio, 6),
                            threshold=thresholds.pacing_fast,
                        )
                    )
                elif pacing_ratio is not None and pacing_ratio < thresholds.pacing_slow:
                    alerts.append(
                        _make_alert(
                            rule="pacing_slow",
                            severity="warning",
                            entity_id=entity,
                            message=(
                                f"Campaign {cname} ({cid}) pacing too slow: {pacing_ratio:.2f}x "
                                f"(threshold < {thresholds.pacing_slow:.2f}x)."
                            ),
                            sim_now_iso=sim_now_iso,
                            fired_at_iso=fired_at_iso,
                            value=round(pacing_ratio, 6),
                            threshold=thresholds.pacing_slow,
                        )
                    )

    # 2. Publisher click-fraud / quality radar (R1 & R2)
    if publisher_df is not None and not publisher_df.empty:
        for _, row in publisher_df.iterrows():
            pid = str(row["publisher_id"])
            pname = str(row.get("publisher_name", pid))
            entity = f"publisher:{pid}"
            clicks_60m = _to_finite_float(row.get("clicks_60m")) or 0.0
            applies_60m = _to_finite_float(row.get("applies_60m")) or 0.0

            if bool(row.get("fraud_flag_r1")):
                alerts.append(
                    _make_alert(
                        rule="publisher_fraud_r1",
                        severity="critical",
                        entity_id=entity,
                        message=(
                            f"Publisher {pname} ({pid}) triggered fraud R1: "
                            f"clicks_60m={int(clicks_60m)}, applies_60m={int(applies_60m)}."
                        ),
                        sim_now_iso=sim_now_iso,
                        fired_at_iso=fired_at_iso,
                        value=clicks_60m,
                        threshold=float(thresholds.click_fraud_min_clicks),
                    )
                )
            if bool(row.get("fraud_flag_r2")):
                rate = _to_finite_float(row.get("click_to_apply_rate_60m"))
                alerts.append(
                    _make_alert(
                        rule="publisher_fraud_r2",
                        severity="warning",
                        entity_id=entity,
                        message=(
                            f"Publisher {pname} ({pid}) triggered fraud R2: "
                            f"clicks_60m={int(clicks_60m)}, rate={rate if rate is not None else 'n/a'}."
                        ),
                        sim_now_iso=sim_now_iso,
                        fired_at_iso=fired_at_iso,
                        value=rate,
                        threshold=thresholds.click_fraud_r2_median_factor,
                    )
                )

    # 3 & 4. Pipeline data freshness and DLQ error rate
    if health_df is not None and not health_df.empty:
        hrow = health_df.iloc[0]
        freshness_s = _to_finite_float(hrow.get("data_freshness_s"))
        if freshness_s is None or freshness_s > float(thresholds.data_stale_seconds):
            alerts.append(
                _make_alert(
                    rule="data_freshness_stale",
                    severity="critical",
                    entity_id="pipeline:silver",
                    message=(
                        f"Silver data freshness stale: "
                        f"{f'{freshness_s:.1f}s' if freshness_s is not None else 'n/a'} "
                        f"(threshold > {thresholds.data_stale_seconds}s)."
                    ),
                    sim_now_iso=sim_now_iso,
                    fired_at_iso=fired_at_iso,
                    value=round(freshness_s, 3) if freshness_s is not None else None,
                    threshold=float(thresholds.data_stale_seconds),
                )
            )

        dlq_rate = _to_finite_float(hrow.get("dlq_rate"))
        if dlq_rate is not None and dlq_rate > dlq_rate_threshold:
            alerts.append(
                _make_alert(
                    rule="dlq_rate_high",
                    severity="critical",
                    entity_id="pipeline:dlq",
                    message=(
                        f"DLQ rate elevated at {dlq_rate * 100:.2f}% "
                        f"(threshold > {dlq_rate_threshold * 100:.2f}%)."
                    ),
                    sim_now_iso=sim_now_iso,
                    fired_at_iso=fired_at_iso,
                    value=round(dlq_rate, 6),
                    threshold=dlq_rate_threshold,
                )
            )

    return alerts


class CooldownStore:
    """In-memory and optional JSON-backed anti-spam cooldown store keyed by ``(rule, entity_id)``."""

    def __init__(self, state_file: Path | None = None) -> None:
        self.state_file = state_file
        self._last_fired: dict[str, datetime] = {}
        self._load()

    def _load(self) -> None:
        if self.state_file is None or not self.state_file.exists():
            return
        try:
            raw = json.loads(self.state_file.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return
        if not isinstance(raw, dict):
            return
        for key, ts_str in raw.items():
            if not isinstance(ts_str, str):
                continue
            try:
                dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                self._last_fired[str(key)] = dt.astimezone(timezone.utc)
            except ValueError:
                continue

    def _save(self) -> None:
        if self.state_file is None:
            return
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        payload = {k: v.strftime("%Y-%m-%dT%H:%M:%SZ") for k, v in sorted(self._last_fired.items())}
        self.state_file.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")

    def filter_and_record(
        self,
        alerts: list[Alert],
        *,
        now: datetime | None = None,
        cooldown_seconds: int = 300,
    ) -> tuple[list[Alert], list[Alert]]:
        """Return ``(fired_alerts, suppressed_alerts)`` and persist updated fire timestamps."""
        check_now = now or datetime.now(timezone.utc)
        if check_now.tzinfo is None:
            check_now = check_now.replace(tzinfo=timezone.utc)
        else:
            check_now = check_now.astimezone(timezone.utc)

        fired: list[Alert] = []
        suppressed: list[Alert] = []
        changed = False

        for alert in alerts:
            key = alert.dedup_key()
            prev = self._last_fired.get(key)
            if prev is not None and (check_now - prev).total_seconds() < cooldown_seconds:
                suppressed.append(alert)
                continue
            self._last_fired[key] = check_now
            fired.append(alert)
            changed = True

        if changed:
            self._save()
        return fired, suppressed


def append_alerts_to_s3(
    repo: DashboardRepository,
    alerts: list[Alert],
    *,
    key: str = "ops/alerts.jsonl",
) -> str:
    """Append fired alerts to ``s3://<bucket>/ops/alerts.jsonl`` without logging credentials."""
    if not alerts:
        return ""

    from botocore.exceptions import ClientError

    s3 = repo._get_s3_client()
    existing_text = ""
    try:
        obj = s3.get_object(Bucket=repo.cfg.s3_bucket, Key=key)
        existing_text = obj["Body"].read().decode("utf-8")
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code not in {"NoSuchKey", "404"}:
            raise

    new_lines = "\n".join(json.dumps(a.to_dict(), ensure_ascii=False) for a in alerts) + "\n"
    if existing_text and not existing_text.endswith("\n"):
        existing_text += "\n"
    combined = existing_text + new_lines

    s3.put_object(
        Bucket=repo.cfg.s3_bucket,
        Key=key,
        Body=combined.encode("utf-8"),
        ContentType="application/x-ndjson",
    )
    return f"s3://{repo.cfg.s3_bucket}/{key}"


def send_webhook(
    alerts: list[Alert],
    *,
    webhook_url: str | None = None,
    dry_run: bool = False,
    timeout_sec: float = 5.0,
) -> bool:
    """Optionally POST alerts to ``ALERT_WEBHOOK_URL``; never log the URL or token (Rule R6)."""
    target_url = (webhook_url if webhook_url is not None else os.environ.get("ALERT_WEBHOOK_URL", "")).strip()
    if not alerts or dry_run or not target_url:
        return False

    body = json.dumps(
        {"count": len(alerts), "alerts": [a.to_dict() for a in alerts]},
        ensure_ascii=False,
    ).encode("utf-8")
    req = urllib.request.Request(
        target_url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
            status = getattr(resp, "status", 200)
            logger.info("Webhook alert dispatch completed status=%s count=%d", status, len(alerts))
            return 200 <= int(status) < 300
    except urllib.error.HTTPError as exc:
        logger.warning("Webhook alert dispatch HTTP failure status=%s", exc.code)
        return False
    except Exception as exc:  # noqa: BLE001
        logger.warning("Webhook alert dispatch failed error_type=%s", type(exc).__name__)
        return False


def run_once(
    repo: DashboardRepository,
    worker_cfg: AlertWorkerConfig,
    cooldown_store: CooldownStore,
    *,
    write_s3: bool = True,
    exclude_outlier_budgets: bool = True,
) -> dict[str, Any]:
    """Execute one evaluation cycle over ``DashboardRepository`` and dispatch fired alerts."""
    sim_info = repo.get_sim_now()
    sim_now = sim_info.get("sim_now")
    pacing_df = repo.get_pacing(sim_now=sim_now) if sim_now is not None else pd.DataFrame()
    publisher_df = repo.get_publisher_quality(sim_now=sim_now) if sim_now is not None else pd.DataFrame()
    health_df = repo.get_pipeline_health()

    evaluated = evaluate_alerts(
        pacing_df=pacing_df,
        publisher_df=publisher_df,
        health_df=health_df,
        thresholds=worker_cfg.thresholds,
        sim_now=sim_now,
        dlq_rate_threshold=worker_cfg.dlq_rate_threshold,
        exclude_outlier_budgets=exclude_outlier_budgets,
    )
    fired, suppressed = cooldown_store.filter_and_record(
        evaluated,
        cooldown_seconds=worker_cfg.cooldown_seconds,
    )

    for alert in fired:
        print(json.dumps(alert.to_dict(), ensure_ascii=False), flush=True)

    s3_uri = ""
    if fired and write_s3:
        s3_uri = append_alerts_to_s3(repo, fired)

    webhook_sent = send_webhook(fired, dry_run=worker_cfg.dry_run)

    return {
        "sim_now": _utc_iso(sim_now),
        "evaluated": len(evaluated),
        "fired": len(fired),
        "suppressed": len(suppressed),
        "s3_uri": s3_uri,
        "webhook_sent": webhook_sent,
    }


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="CPC AdTech Lakehouse Alert Worker (Task 5.8).")
    parser.add_argument("--once", action="store_true", help="Run a single evaluation pass and exit.")
    parser.add_argument("--interval-sec", type=int, default=30, help="Polling interval in seconds.")
    parser.add_argument("--cooldown-sec", type=int, default=None, help="Override cooldown seconds.")
    parser.add_argument(
        "--state-file",
        type=Path,
        default=_DEFAULT_STATE_FILE,
        help="Path to JSON cooldown state file.",
    )
    parser.add_argument("--alerts-yaml", type=Path, default=None, help="Optional path to alerts.yaml.")
    parser.add_argument("--no-s3", action="store_true", help="Skip appending ops/alerts.jsonl to MinIO.")
    parser.add_argument(
        "--include-outliers",
        action="store_true",
        help="Include outlier budget campaigns (e.g. campaign_id=1) in pacing alerts.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint for Task 5.8 Alert Worker."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    args = _parse_args(argv)
    worker_cfg = AlertWorkerConfig.from_yaml(args.alerts_yaml)
    if args.cooldown_sec is not None:
        worker_cfg = AlertWorkerConfig(
            thresholds=worker_cfg.thresholds,
            dlq_rate_threshold=worker_cfg.dlq_rate_threshold,
            cooldown_seconds=int(args.cooldown_sec),
            dry_run=worker_cfg.dry_run,
        )

    repo = DashboardRepository(thresholds=worker_cfg.thresholds)
    cooldown_store = CooldownStore(state_file=args.state_file)

    try:
        while True:
            try:
                summary = run_once(
                    repo,
                    worker_cfg,
                    cooldown_store,
                    write_s3=not args.no_s3,
                    exclude_outlier_budgets=not args.include_outliers,
                )
                print(json.dumps({"summary": summary}, ensure_ascii=False), flush=True)
            except DatabaseConnectionError as exc:
                logger.warning("Alert Worker skipped pass due to connection error: %s", type(exc).__name__)
                if args.once:
                    return 1
            if args.once:
                return 0
            time.sleep(max(1, int(args.interval_sec)))
    except KeyboardInterrupt:
        logger.info("Alert Worker stopped by KeyboardInterrupt.")
        return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
