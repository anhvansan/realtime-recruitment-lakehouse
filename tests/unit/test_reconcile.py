"""Host unit tests for Task 3.5 — Reconciliation Harness evaluation logic."""

from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path
from typing import Any

from tests.integration.reconcile import (
    LateEventInfo,
    TableSnapshot,
    evaluate_reconciliation,
    find_latest_manifest,
    load_late_events,
    parse_watermark_minutes,
    resolve_late_ids_path,
)


def _sample_manifest() -> dict[str, Any]:
    return {
        "run_id": "unit_test",
        "sent_valid_unique": 4,
        "injected_duplicates": 1,
        "injected_corrupt": {
            "total": 2,
            "by_error_type": {
                "corrupt_json": 1,
                "bad_enum": 1,
                "missing_required": 0,
            },
        },
        "expected_unique_valid_events": 4,
        "expected_spend_total": "10.5000",
        "late_ids_file": "data/manifests/replay_unit_test.late_ids.jsonl",
    }


def _sample_late_events() -> dict[str, LateEventInfo]:
    return {
        "e_late_over": LateEventInfo(
            event_id="e_late_over",
            delay_minutes=12.5,
            spend=Decimal("2.5000"),
        ),
        "e_late_within": LateEventInfo(
            event_id="e_late_within",
            delay_minutes=6.0,
            spend=Decimal("1.0000"),
        ),
    }


def test_reconcile_clean_pass() -> None:
    """PASS when Bronze, Silver, DLQ, and spend match manifest with 0 dropped late events."""
    manifest = _sample_manifest()
    late_events = _sample_late_events()
    all_ids = frozenset({"e1", "e2", "e_late_over", "e_late_within"})
    snapshot = TableSnapshot(
        bronze_row_count=5,
        bronze_event_ids=all_ids,
        silver_row_count=4,
        silver_distinct_event_ids=4,
        silver_event_ids=all_ids,
        silver_spend_total=Decimal("10.5000"),
        dlq_counts_by_type={"corrupt_json": 1, "bad_enum": 1},
    )

    report = evaluate_reconciliation(manifest, late_events, snapshot, watermark_minutes=10.0)
    assert report["status"] == "PASS"
    assert all(c["status"] == "PASS" for c in report["checks"].values())


def test_reconcile_pass_with_late_over_watermark_dropped() -> None:
    """PASS when missing Bronze-Silver ID is a late event >10m and spend difference matches."""
    manifest = _sample_manifest()
    late_events = _sample_late_events()
    bronze_ids = frozenset({"e1", "e2", "e_late_over", "e_late_within"})
    silver_ids = frozenset({"e1", "e2", "e_late_within"})
    snapshot = TableSnapshot(
        bronze_row_count=5,
        bronze_event_ids=bronze_ids,
        silver_row_count=3,
        silver_distinct_event_ids=3,
        silver_event_ids=silver_ids,
        silver_spend_total=Decimal("8.0000"),  # 10.5000 - 2.5000 (e_late_over)
        dlq_counts_by_type={"corrupt_json": 1, "bad_enum": 1},
    )

    report = evaluate_reconciliation(manifest, late_events, snapshot, watermark_minutes=10.0)
    assert report["status"] == "PASS"
    assert report["checks"]["d_bronze_minus_silver_late_only"]["dropped_late_over_watermark_count"] == 1
    assert report["checks"]["e_silver_spend_reconciliation"]["dropped_late_spend"] == "2.5000"


def test_reconcile_fail_when_non_late_silver_row_deleted() -> None:
    """FAIL checks (d) and (e) when a normal (non-late) Silver row is missing/deleted."""
    manifest = _sample_manifest()
    late_events = _sample_late_events()
    bronze_ids = frozenset({"e1", "e2", "e_late_over", "e_late_within"})
    silver_ids = frozenset({"e2", "e_late_over", "e_late_within"})  # e1 deleted!
    snapshot = TableSnapshot(
        bronze_row_count=5,
        bronze_event_ids=bronze_ids,
        silver_row_count=3,
        silver_distinct_event_ids=3,
        silver_event_ids=silver_ids,
        silver_spend_total=Decimal("7.5000"),  # e1 spend (3.0000) missing
        dlq_counts_by_type={"corrupt_json": 1, "bad_enum": 1},
    )

    report = evaluate_reconciliation(manifest, late_events, snapshot, watermark_minutes=10.0)
    assert report["status"] == "FAIL"
    assert report["checks"]["d_bronze_minus_silver_late_only"]["status"] == "FAIL"
    assert report["checks"]["d_bronze_minus_silver_late_only"]["unexpected_missing_sample"] == ["e1"]
    assert report["checks"]["e_silver_spend_reconciliation"]["status"] == "FAIL"


def test_reconcile_fail_when_within_watermark_late_event_missing() -> None:
    """FAIL check (d) if a late event within watermark (<= 10m) was silently dropped."""
    manifest = _sample_manifest()
    late_events = _sample_late_events()
    bronze_ids = frozenset({"e1", "e2", "e_late_over", "e_late_within"})
    silver_ids = frozenset({"e1", "e2", "e_late_over"})  # e_late_within (6.0m) dropped!
    snapshot = TableSnapshot(
        bronze_row_count=5,
        bronze_event_ids=bronze_ids,
        silver_row_count=3,
        silver_distinct_event_ids=3,
        silver_event_ids=silver_ids,
        silver_spend_total=Decimal("9.5000"),
        dlq_counts_by_type={"corrupt_json": 1, "bad_enum": 1},
    )

    report = evaluate_reconciliation(manifest, late_events, snapshot, watermark_minutes=10.0)
    assert report["status"] == "FAIL"
    assert report["checks"]["d_bronze_minus_silver_late_only"]["status"] == "FAIL"
    assert report["checks"]["d_bronze_minus_silver_late_only"]["unexpected_missing_sample"] == [
        "e_late_within"
    ]


def test_reconcile_fail_on_dlq_bronze_and_duplicate_silver_mismatches() -> None:
    """FAIL checks (a), (b), (c) when DLQ counts, Bronze count, or Silver uniqueness fail."""
    manifest = _sample_manifest()
    all_ids = frozenset({"e1", "e2", "e_late_over", "e_late_within"})
    snapshot = TableSnapshot(
        bronze_row_count=4,  # expected 5
        bronze_event_ids=all_ids,
        silver_row_count=5,  # duplicate row!
        silver_distinct_event_ids=4,
        silver_event_ids=all_ids,
        silver_spend_total=Decimal("10.5000"),
        dlq_counts_by_type={"corrupt_json": 1},  # missing bad_enum
    )

    report = evaluate_reconciliation(manifest, {}, snapshot, watermark_minutes=10.0)
    assert report["status"] == "FAIL"
    assert report["checks"]["a_dlq_by_error_type"]["status"] == "FAIL"
    assert report["checks"]["b_bronze_row_count"]["status"] == "FAIL"
    assert report["checks"]["c_silver_uniqueness"]["status"] == "FAIL"


def test_manifest_and_late_ids_helpers(tmp_path: Path) -> None:
    """Verify manifest discovery, late_ids JSONL parsing, and watermark string parsing."""
    assert parse_watermark_minutes("10 minutes") == 10.0
    assert parse_watermark_minutes("30 seconds") == 0.5
    assert parse_watermark_minutes("1 hour") == 60.0

    m1 = tmp_path / "replay_run1.json"
    m1.write_text(json.dumps({"run_id": "run1"}), encoding="utf-8")
    late_file = tmp_path / "replay_run1.late_ids.jsonl"
    late_file.write_text(
        '{"event_id":"evt-a","delay_minutes":11.2,"spend":"1.5000"}\n',
        encoding="utf-8",
    )

    found = find_latest_manifest(tmp_path)
    assert found == m1
    resolved_late = resolve_late_ids_path(found, json.loads(found.read_text(encoding="utf-8")))
    assert resolved_late == late_file

    parsed = load_late_events(resolved_late)
    assert parsed["evt-a"].delay_minutes == 11.2
    assert parsed["evt-a"].spend == Decimal("1.5000")
