#!/usr/bin/env python3
"""Lakehouse Reconciliation Harness: Bronze vs Silver vs DLQ vs Replay Manifest (Task 3.5).

Reads Bronze, Silver, and DLQ Delta tables directly from MinIO via DuckDB ``delta_scan``
and evaluates checks (a)-(e) against ``data/manifests/replay_<run_id>.json`` and
``.late_ids.jsonl``:
  (a) DLQ counts by error_type == manifest injected_corrupt.by_error_type
  (b) Bronze row count == sent_valid_unique + injected_duplicates
  (c) Silver uniqueness: COUNT(*) == COUNT(DISTINCT event_id)
  (d) Every event_id in Bronze - Silver must be a late event exceeding watermark_delay
  (e) Silver SUM(spend) == expected_spend_total - SUM(spend of dropped late events)

Outputs a compact JSON report and exits with code 0 on PASS, 1 on FAIL.
Follows Rule R6: Never logs S3 credentials or DuckDB CREATE SECRET SQL.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any

import duckdb

# Ensure project root is importable when executed directly as a script
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.common.config import AppConfig, load  # noqa: E402


@dataclass(frozen=True)
class LateEventInfo:
    """Late-event metadata loaded from ``replay_<run_id>.late_ids.jsonl``."""

    event_id: str
    delay_minutes: float
    spend: Decimal


@dataclass(frozen=True)
class TableSnapshot:
    """Aggregated Bronze, Silver, and DLQ facts used for reconciliation."""

    bronze_row_count: int
    bronze_event_ids: frozenset[str]
    silver_row_count: int
    silver_distinct_event_ids: int
    silver_event_ids: frozenset[str]
    silver_spend_total: Decimal
    dlq_counts_by_type: dict[str, int]


def parse_watermark_minutes(watermark_delay: str) -> float:
    """Parse Spark watermark interval string (e.g. '10 minutes') into minutes."""
    cleaned = watermark_delay.strip().lower()
    match = re.fullmatch(
        r"(\d+(?:\.\d+)?)\s*(minute|minutes|min|mins|second|seconds|sec|secs|hour|hours|hr|hrs)",
        cleaned,
    )
    if match is None:
        raise ValueError(f"Unsupported watermark_delay format: {watermark_delay!r}")
    value = float(match.group(1))
    unit = match.group(2)
    if unit.startswith("sec"):
        return value / 60.0
    if unit.startswith("h"):
        return value * 60.0
    return value


def find_latest_manifest(manifest_dir: Path) -> Path:
    """Return the most recently modified ``replay_*.json`` manifest file."""
    candidates = sorted(
        manifest_dir.glob("replay_*.json"),
        key=lambda p: (p.stat().st_mtime, p.name),
        reverse=True,
    )
    if not candidates:
        raise FileNotFoundError(f"No replay_*.json manifest found in {manifest_dir}")
    return candidates[0]


def resolve_late_ids_path(manifest_path: Path, manifest: dict[str, Any]) -> Path | None:
    """Resolve ``.late_ids.jsonl`` path relative to manifest directory or project root."""
    raw_ref = manifest.get("late_ids_file")
    if isinstance(raw_ref, str) and raw_ref.strip():
        ref_path = Path(raw_ref)
        if ref_path.is_absolute() and ref_path.exists():
            return ref_path
        by_name = manifest_path.parent / ref_path.name
        if by_name.exists():
            return by_name
        by_root = _PROJECT_ROOT / ref_path
        if by_root.exists():
            return by_root

    sibling = manifest_path.with_suffix(".late_ids.jsonl")
    return sibling if sibling.exists() else None


def load_late_events(late_ids_path: Path | None) -> dict[str, LateEventInfo]:
    """Load late event IDs, simulated delay (minutes), and Decimal spend from JSONL."""
    if late_ids_path is None or not late_ids_path.exists():
        return {}

    records: dict[str, LateEventInfo] = {}
    with late_ids_path.open("r", encoding="utf-8") as fh:
        for line in fh:
            stripped = line.strip()
            if not stripped:
                continue
            item = json.loads(stripped)
            event_id = str(item["event_id"])
            records[event_id] = LateEventInfo(
                event_id=event_id,
                delay_minutes=float(item["delay_minutes"]),
                spend=Decimal(str(item["spend"])),
            )
    return records


def evaluate_reconciliation(
    manifest: dict[str, Any],
    late_events: dict[str, LateEventInfo],
    snapshot: TableSnapshot,
    *,
    watermark_minutes: float = 10.0,
) -> dict[str, Any]:
    """Evaluate checks (a)-(e) and return a structured JSON-serializable report."""
    # (a) DLQ counts by error_type == manifest
    raw_expected_dlq = manifest.get("injected_corrupt", {}).get("by_error_type", {})
    expected_dlq = {
        str(k): int(v) for k, v in sorted(raw_expected_dlq.items()) if int(v) > 0
    }
    actual_dlq = {
        str(k): int(v) for k, v in sorted(snapshot.dlq_counts_by_type.items()) if int(v) > 0
    }
    check_a_pass = actual_dlq == expected_dlq

    # (b) Bronze row count == sent_valid_unique + injected_duplicates
    expected_bronze = int(manifest["sent_valid_unique"]) + int(manifest["injected_duplicates"])
    check_b_pass = snapshot.bronze_row_count == expected_bronze

    # (c) Silver uniqueness: COUNT(*) == COUNT(DISTINCT event_id)
    check_c_pass = snapshot.silver_row_count == snapshot.silver_distinct_event_ids

    # (d) Every event_id in Bronze - Silver must be late (> watermark_minutes)
    missing_from_silver = sorted(snapshot.bronze_event_ids - snapshot.silver_event_ids)
    extra_in_silver = sorted(snapshot.silver_event_ids - snapshot.bronze_event_ids)

    dropped_late_ids: list[str] = []
    unexpected_missing_ids: list[str] = []
    for eid in missing_from_silver:
        late_info = late_events.get(eid)
        if late_info is not None and late_info.delay_minutes > watermark_minutes:
            dropped_late_ids.append(eid)
        else:
            unexpected_missing_ids.append(eid)

    check_d_pass = len(unexpected_missing_ids) == 0 and len(extra_in_silver) == 0

    # (e) Silver SUM(spend) == expected_spend_total - SUM(dropped late spend)
    expected_spend_total = Decimal(str(manifest["expected_spend_total"]))
    dropped_late_spend = sum(
        (late_events[eid].spend for eid in dropped_late_ids),
        start=Decimal("0.0000"),
    )
    expected_silver_spend = expected_spend_total - dropped_late_spend
    actual_silver_spend = snapshot.silver_spend_total
    check_e_pass = actual_silver_spend == expected_silver_spend

    all_passed = all(
        [check_a_pass, check_b_pass, check_c_pass, check_d_pass, check_e_pass]
    )

    return {
        "status": "PASS" if all_passed else "FAIL",
        "run_id": manifest.get("run_id"),
        "watermark_minutes": watermark_minutes,
        "checks": {
            "a_dlq_by_error_type": {
                "status": "PASS" if check_a_pass else "FAIL",
                "expected": expected_dlq,
                "actual": actual_dlq,
            },
            "b_bronze_row_count": {
                "status": "PASS" if check_b_pass else "FAIL",
                "expected": expected_bronze,
                "actual": snapshot.bronze_row_count,
            },
            "c_silver_uniqueness": {
                "status": "PASS" if check_c_pass else "FAIL",
                "silver_row_count": snapshot.silver_row_count,
                "silver_distinct_event_ids": snapshot.silver_distinct_event_ids,
            },
            "d_bronze_minus_silver_late_only": {
                "status": "PASS" if check_d_pass else "FAIL",
                "missing_from_silver_count": len(missing_from_silver),
                "dropped_late_over_watermark_count": len(dropped_late_ids),
                "unexpected_missing_count": len(unexpected_missing_ids),
                "unexpected_missing_sample": unexpected_missing_ids[:10],
                "extra_in_silver_count": len(extra_in_silver),
                "extra_in_silver_sample": extra_in_silver[:10],
            },
            "e_silver_spend_reconciliation": {
                "status": "PASS" if check_e_pass else "FAIL",
                "manifest_expected_spend_total": f"{expected_spend_total:.4f}",
                "dropped_late_spend": f"{dropped_late_spend:.4f}",
                "expected_silver_spend": f"{expected_silver_spend:.4f}",
                "actual_silver_spend": f"{actual_silver_spend:.4f}",
            },
        },
    }


def build_duckdb_connection(cfg: AppConfig) -> duckdb.DuckDBPyConnection:
    """Create an in-memory DuckDB connection configured for S3 Delta Lake scanning."""
    ext_dir = os.environ.get("DUCKDB_EXTENSION_DIRECTORY", "/opt/duckdb_extensions")
    conn_cfg: dict[str, Any] = {}
    if Path(ext_dir).exists():
        conn_cfg["extension_directory"] = ext_dir

    con = duckdb.connect(":memory:", config=conn_cfg)
    con.execute("SET memory_limit = '256MB'")
    con.execute("SET threads = 2")
    con.execute("LOAD httpfs")
    con.execute("LOAD delta")

    use_ssl_sql = "true" if cfg.s3_use_ssl else "false"
    # Rule R6: Never log this SQL string containing S3 credentials
    con.execute(
        f"""
        CREATE OR REPLACE SECRET lakehouse_s3 (
            TYPE S3,
            KEY_ID '{cfg.s3_access_key}',
            SECRET '{cfg.s3_secret_key}',
            ENDPOINT '{cfg.s3_endpoint_host_port}',
            URL_STYLE 'path',
            USE_SSL {use_ssl_sql},
            REGION '{cfg.s3_region}'
        )
        """
    )
    return con


def fetch_lakehouse_snapshot(cfg: AppConfig) -> TableSnapshot:
    """Query Bronze, Silver, and DLQ Delta tables on MinIO via DuckDB ``delta_scan``."""
    bronze_s3 = cfg.s3_uri(cfg.pipeline.storage.bronze_path)
    silver_s3 = cfg.s3_uri(cfg.pipeline.storage.silver_path)
    dlq_s3 = cfg.s3_uri(cfg.pipeline.storage.dlq_path)

    con = build_duckdb_connection(cfg)
    try:
        bronze_row = con.execute(
            f"SELECT COUNT(*) FROM delta_scan('{bronze_s3}')"
        ).fetchone()
        bronze_count = int(bronze_row[0]) if bronze_row else 0
        bronze_ids = frozenset(
            str(r[0])
            for r in con.execute(
                f"SELECT DISTINCT event_id FROM delta_scan('{bronze_s3}')"
            ).fetchall()
        )

        silver_row = con.execute(
            f"""
            SELECT
                COUNT(*),
                COUNT(DISTINCT event_id),
                COALESCE(SUM(spend), 0::DECIMAL(18,4))
            FROM delta_scan('{silver_s3}')
            """
        ).fetchone()
        silver_count = int(silver_row[0]) if silver_row else 0
        silver_distinct = int(silver_row[1]) if silver_row else 0
        silver_spend = (
            Decimal(str(silver_row[2])) if silver_row and silver_row[2] is not None else Decimal("0.0000")
        )
        silver_ids = frozenset(
            str(r[0])
            for r in con.execute(
                f"SELECT DISTINCT event_id FROM delta_scan('{silver_s3}')"
            ).fetchall()
        )

        dlq_counts: dict[str, int] = {}
        try:
            dlq_rows = con.execute(
                f"""
                SELECT error_type, COUNT(*)
                FROM delta_scan('{dlq_s3}')
                GROUP BY error_type
                ORDER BY error_type
                """
            ).fetchall()
            dlq_counts = {str(r[0]): int(r[1]) for r in dlq_rows}
        except duckdb.Error:
            dlq_counts = {}
    finally:
        con.close()

    return TableSnapshot(
        bronze_row_count=bronze_count,
        bronze_event_ids=bronze_ids,
        silver_row_count=silver_count,
        silver_distinct_event_ids=silver_distinct,
        silver_event_ids=silver_ids,
        silver_spend_total=silver_spend,
        dlq_counts_by_type=dlq_counts,
    )


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Reconcile Bronze, Silver, and DLQ Delta tables against replay manifest."
    )
    parser.add_argument(
        "--manifest",
        type=Path,
        default=None,
        help="Path to replay_<run_id>.json manifest (default: latest in --manifest-dir).",
    )
    parser.add_argument(
        "--manifest-dir",
        type=Path,
        default=_PROJECT_ROOT / "data" / "manifests",
        help="Directory searched when --manifest is omitted.",
    )
    parser.add_argument(
        "--watermark-minutes",
        type=float,
        default=None,
        help="Override watermark delay threshold in minutes (default: from pipeline.yaml).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint for Lakehouse reconciliation harness."""
    args = _parse_args(argv)
    cfg = load()

    manifest_path = (
        args.manifest if args.manifest is not None else find_latest_manifest(args.manifest_dir)
    )
    with manifest_path.open("r", encoding="utf-8") as fh:
        manifest: dict[str, Any] = json.load(fh)

    late_ids_path = resolve_late_ids_path(manifest_path, manifest)
    late_events = load_late_events(late_ids_path)

    watermark_minutes = (
        args.watermark_minutes
        if args.watermark_minutes is not None
        else parse_watermark_minutes(cfg.pipeline.streaming.watermark_delay)
    )

    snapshot = fetch_lakehouse_snapshot(cfg)
    report = evaluate_reconciliation(
        manifest=manifest,
        late_events=late_events,
        snapshot=snapshot,
        watermark_minutes=watermark_minutes,
    )
    report["manifest_file"] = manifest_path.as_posix()

    print(json.dumps(report, indent=2, sort_keys=False))
    return 0 if report["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
