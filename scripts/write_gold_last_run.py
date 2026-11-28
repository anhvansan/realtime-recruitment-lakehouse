#!/usr/bin/env python3
"""Write Gold layer last-run metadata to MinIO ``s3://<bucket>/gold/_last_run.json`` (Task 4.6).

Used by ``scripts/dbt_loop.sh`` after each ``dbt build`` cycle so the Streamlit dashboard
Panel 3 can display "gold cập nhật lúc ..." without Airflow.
Follows Rule R6: reads credentials via ``src.common.config.load()`` and never logs secrets.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import boto3
from botocore.config import Config

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.common.config import AppConfig, load  # noqa: E402

GOLD_MODELS: tuple[str, ...] = (
    "fct_campaign_hourly_pacing",
    "fct_publisher_quality",
    "fct_recruitment_efficiency",
)


def build_last_run_payload(
    *,
    status: str,
    duration_sec: float,
    models_built: int,
    run_at: datetime | None = None,
) -> dict[str, Any]:
    """Build deterministic JSON-serializable metadata for ``gold/_last_run.json``."""
    ts = (run_at or datetime.now(timezone.utc)).astimezone(timezone.utc)
    return {
        "last_run_utc": ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "duration_sec": round(float(duration_sec), 3),
        "status": status,
        "models_built": int(models_built),
        "gold_models": list(GOLD_MODELS),
    }


def _resolve_s3_endpoint(cfg: AppConfig, explicit_endpoint: str | None) -> str:
    if explicit_endpoint:
        return explicit_endpoint
    if os.environ.get("S3_ENDPOINT_EXTERNAL"):
        return cfg.s3_endpoint_external
    if Path("/.dockerenv").exists():
        return cfg.s3_endpoint
    return cfg.s3_endpoint_external


def upload_last_run_metadata(
    cfg: AppConfig,
    payload: dict[str, Any],
    *,
    endpoint: str | None = None,
    key: str = "gold/_last_run.json",
) -> str:
    """Upload ``_last_run.json`` payload to S3/MinIO and return its ``s3://`` URI."""
    resolved_endpoint = _resolve_s3_endpoint(cfg, endpoint)
    s3 = boto3.client(
        "s3",
        endpoint_url=resolved_endpoint,
        aws_access_key_id=cfg.s3_access_key,
        aws_secret_access_key=cfg.s3_secret_key,
        region_name=cfg.s3_region,
        config=Config(s3={"addressing_style": "path"}),
    )
    body = (json.dumps(payload, indent=2) + "\n").encode("utf-8")
    s3.put_object(
        Bucket=cfg.s3_bucket,
        Key=key,
        Body=body,
        ContentType="application/json",
    )
    return f"s3://{cfg.s3_bucket}/{key}"


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Write gold/_last_run.json metadata to MinIO after dbt build."
    )
    parser.add_argument(
        "--status",
        required=True,
        choices=["success", "failed"],
        help="Outcome of the dbt build run.",
    )
    parser.add_argument(
        "--duration-sec",
        type=float,
        required=True,
        help="Elapsed wall-clock duration in seconds.",
    )
    parser.add_argument(
        "--models-built",
        type=int,
        default=len(GOLD_MODELS),
        help="Number of Gold models targeted/built (default: 3).",
    )
    parser.add_argument(
        "--endpoint",
        type=str,
        default=None,
        help="Optional explicit S3 endpoint URL (defaults to S3_ENDPOINT_EXTERNAL on host).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print payload JSON without uploading to MinIO.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint for writing ``gold/_last_run.json``."""
    args = _parse_args(argv)
    payload = build_last_run_payload(
        status=args.status,
        duration_sec=args.duration_sec,
        models_built=args.models_built,
    )
    if args.dry_run:
        print(json.dumps(payload, indent=2))
        return 0

    cfg = load()
    uri = upload_last_run_metadata(cfg, payload, endpoint=args.endpoint)
    print(f"Updated {uri}: status={payload['status']} duration_sec={payload['duration_sec']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
