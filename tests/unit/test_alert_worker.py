"""Unit tests for Task 5.8 Alert Worker (rules, cooldown anti-spam, S3 JSONL, webhook safety)."""

from __future__ import annotations

import io
import json
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
from botocore.exceptions import ClientError

from src.alerts.worker import (
    Alert,
    AlertWorkerConfig,
    CooldownStore,
    append_alerts_to_s3,
    evaluate_alerts,
    send_webhook,
)
from src.common.config import AppConfig
from src.dashboard.repository import AlertThresholds, DashboardRepository, DataSourcePaths


def test_alert_worker_config_loads_alerts_yaml() -> None:
    cfg = AlertWorkerConfig.from_yaml()
    assert cfg.thresholds.pacing_fast == 1.2
    assert cfg.thresholds.pacing_slow == 0.6
    assert cfg.thresholds.data_stale_seconds == 120
    assert cfg.dlq_rate_threshold == 0.05
    assert cfg.cooldown_seconds == 300


def test_evaluate_alerts_covers_all_four_rule_groups() -> None:
    """Evaluates Pacing, Publisher R1/R2, Freshness (>120s), and DLQ rate rules cleanly."""
    thresholds = AlertThresholds()
    sim_now = datetime(2026, 4, 6, 17, 0, 42, tzinfo=timezone.utc)
    fired_at = datetime(2026, 11, 28, 10, 0, 0, tzinfo=timezone.utc)

    pacing_df = pd.DataFrame(
        [
            {
                "campaign_id": "1",
                "campaign_name": "Outlier Campaign",
                "is_outlier_budget": True,
                "hour_ts": datetime(2026, 4, 6, 17, 0, 0),
                "pacing_ratio": 0.0001,
                "budget_utilization": 0.0001,
            },
            {
                "campaign_id": "C1",
                "campaign_name": "Fast Campaign",
                "is_outlier_budget": False,
                "hour_ts": datetime(2026, 4, 6, 17, 0, 0),
                "pacing_ratio": 1.6,
                "budget_utilization": 0.95,
            },
            {
                "campaign_id": "C2",
                "campaign_name": "Slow Campaign",
                "is_outlier_budget": False,
                "hour_ts": datetime(2026, 4, 6, 17, 0, 0),
                "pacing_ratio": 0.4,
                "budget_utilization": 0.25,
            },
            {
                "campaign_id": "C3",
                "campaign_name": "Healthy Campaign",
                "is_outlier_budget": False,
                "hour_ts": datetime(2026, 4, 6, 17, 0, 0),
                "pacing_ratio": 1.0,
                "budget_utilization": 0.60,
            },
        ]
    )

    publisher_df = pd.DataFrame(
        [
            {
                "publisher_id": "P9",
                "publisher_name": "Fraud Pub",
                "clicks_60m": 130,
                "applies_60m": 0,
                "click_to_apply_rate_60m": 0.0,
                "fraud_flag_r1": True,
                "fraud_flag_r2": True,
            },
            {
                "publisher_id": "P2",
                "publisher_name": "Clean Pub",
                "clicks_60m": 50,
                "applies_60m": 5,
                "click_to_apply_rate_60m": 0.1,
                "fraud_flag_r1": False,
                "fraud_flag_r2": False,
            },
        ]
    )

    health_df = pd.DataFrame([{"data_freshness_s": 150.5, "dlq_rate": 0.08}])

    alerts = evaluate_alerts(
        pacing_df=pacing_df,
        publisher_df=publisher_df,
        health_df=health_df,
        thresholds=thresholds,
        sim_now=sim_now,
        dlq_rate_threshold=0.05,
        exclude_outlier_budgets=True,
        fired_at=fired_at,
    )

    pairs = {(a.rule, a.entity_id) for a in alerts}
    assert ("budget_utilization_high", "campaign:C1") in pairs
    assert ("pacing_fast", "campaign:C1") in pairs
    assert ("pacing_slow", "campaign:C2") in pairs
    assert ("pacing_slow", "campaign:1") not in pairs  # outlier excluded
    assert ("publisher_fraud_r1", "publisher:P9") in pairs
    assert ("publisher_fraud_r2", "publisher:P9") in pairs
    assert ("data_freshness_stale", "pipeline:silver") in pairs
    assert ("dlq_rate_high", "pipeline:dlq") in pairs
    assert all(a.sim_now == "2026-04-06T17:00:42Z" for a in alerts)
    assert all(a.fired_at == "2026-11-28T10:00:00Z" for a in alerts)

    # Healthy health_df triggers neither freshness nor dlq alerts
    healthy_alerts = evaluate_alerts(
        pacing_df=pd.DataFrame(),
        publisher_df=pd.DataFrame(),
        health_df=pd.DataFrame([{"data_freshness_s": 60.0, "dlq_rate": 0.01}]),
        thresholds=thresholds,
        sim_now=sim_now,
    )
    assert healthy_alerts == []


def test_cooldown_store_suppresses_duplicates_and_persists_across_runs(tmp_path: Path) -> None:
    """Same (rule, entity_id) fires once, is suppressed inside cooldown, and re-fires after."""
    state_file = tmp_path / "cooldown.json"
    alert = Alert(
        alert_id="data_freshness_stale-abc123",
        rule="data_freshness_stale",
        severity="critical",
        entity_id="pipeline:silver",
        message="Silver stale",
        sim_now="2026-04-06T17:00:42Z",
        fired_at="2026-11-28T10:00:00Z",
    )

    t0 = datetime(2026, 11, 28, 10, 0, 0, tzinfo=timezone.utc)
    store_run1 = CooldownStore(state_file=state_file)
    fired_1, suppressed_1 = store_run1.filter_and_record([alert], now=t0, cooldown_seconds=300)
    assert len(fired_1) == 1
    assert len(suppressed_1) == 0

    # Simulate a second CLI `--once` process loading state from disk 60s later
    store_run2 = CooldownStore(state_file=state_file)
    fired_2, suppressed_2 = store_run2.filter_and_record(
        [alert], now=t0 + timedelta(seconds=60), cooldown_seconds=300
    )
    assert len(fired_2) == 0
    assert len(suppressed_2) == 1

    # After 301s cooldown expires, fires again
    fired_3, suppressed_3 = store_run2.filter_and_record(
        [alert], now=t0 + timedelta(seconds=301), cooldown_seconds=300
    )
    assert len(fired_3) == 1
    assert len(suppressed_3) == 0


def test_send_webhook_safe_when_unset_and_redacts_url_on_error(monkeypatch, caplog) -> None:
    """Empty ALERT_WEBHOOK_URL no-ops cleanly; failed HTTP call never logs secret webhook URL."""
    alert = Alert(
        alert_id="pacing_fast-123",
        rule="pacing_fast",
        severity="warning",
        entity_id="campaign:C1",
        message="Fast pacing",
        sim_now="2026-04-06T17:00:42Z",
        fired_at="2026-11-28T10:00:00Z",
    )

    monkeypatch.delenv("ALERT_WEBHOOK_URL", raising=False)
    assert send_webhook([alert], dry_run=False) is False

    secret_url = "https://discord.com/api/webhooks/123456/SECRET_TOKEN_XYZ"
    monkeypatch.setenv("ALERT_WEBHOOK_URL", secret_url)

    def _boom(*args, **kwargs):
        raise urllib.error.URLError(f"failed reaching {secret_url}")

    monkeypatch.setattr(urllib.request, "urlopen", _boom)

    with caplog.at_level("WARNING"):
        assert send_webhook([alert], dry_run=False) is False

    assert "SECRET_TOKEN_XYZ" not in caplog.text
    assert secret_url not in caplog.text


def test_append_alerts_to_s3_writes_and_appends_jsonl(monkeypatch) -> None:
    """S3 JSONL sink handles initial NoSuchKey and appends subsequent alerts cleanly."""
    import boto3

    stored: dict[str, bytes] = {}
    mock_s3 = MagicMock()

    def _fake_get_object(Bucket: str, Key: str) -> dict:
        if Key not in stored:
            raise ClientError({"Error": {"Code": "NoSuchKey", "Message": "Not found"}}, "GetObject")
        return {"Body": io.BytesIO(stored[Key])}

    def _fake_put_object(Bucket: str, Key: str, Body: bytes, ContentType: str = "") -> dict:
        stored[Key] = Body
        return {"ETag": "mock"}

    mock_s3.get_object.side_effect = _fake_get_object
    mock_s3.put_object.side_effect = _fake_put_object
    monkeypatch.setattr(boto3, "client", lambda *args, **kwargs: mock_s3)

    repo = DashboardRepository(
        cfg=AppConfig(),
        paths=DataSourcePaths("a", "b", "c", "d", "e", "f"),
    )
    a1 = Alert("id1", "data_freshness_stale", "critical", "pipeline:silver", "stale", None, "2026-11-28T10:00:00Z")
    a2 = Alert("id2", "pacing_slow", "warning", "campaign:4", "slow", None, "2026-11-28T10:01:00Z")

    uri1 = append_alerts_to_s3(repo, [a1])
    uri2 = append_alerts_to_s3(repo, [a2])

    assert uri1 == "s3://lakehouse/ops/alerts.jsonl"
    assert uri2 == "s3://lakehouse/ops/alerts.jsonl"
    lines = stored["ops/alerts.jsonl"].decode("utf-8").strip().splitlines()
    assert len(lines) == 2
    assert json.loads(lines[0])["alert_id"] == "id1"
    assert json.loads(lines[1])["alert_id"] == "id2"
