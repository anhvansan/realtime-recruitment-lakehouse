"""Unit tests for Task 2.6 & 2.7 — Fault Injection, Rates, Contract Alignment & Manifest."""

from __future__ import annotations

import json
import random
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from src.common.schemas.cpc_event import validate_event
from src.producer.fault_injector import (
    CORRUPT_TYPES,
    FaultConfig,
    FaultInjector,
    apply_corruption,
    inject_bad_enum,
    inject_bad_timestamp,
    inject_corrupt_json,
    inject_malformed_decimal,
    inject_missing_required,
    inject_negative_amount,
)


def _sample_payload(idx: int, spend: str = "1.5000") -> dict[str, Any]:
    return {
        "schema_version": "v1",
        "event_id": f"{idx:040x}",
        "event_time": "2026-01-01T00:00:00Z",
        "produced_at": "2026-11-25T10:00:00Z",
        "event_type": "click",
        "job_id": "98",
        "campaign_id": str((idx % 5) + 1),
        "group_id": None,
        "publisher_id": "1",
        "bid": spend,
        "spend": spend,
        "source_row_id": idx + 1,
    }


def test_same_seed_produces_identical_manifest() -> None:
    """Running FaultInjector twice with identical seed yields identical manifest & late records."""
    base_time = datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC)
    inj_a = FaultInjector(seed=42)
    inj_b = FaultInjector(seed=42)

    for idx in range(2_000):
        payload = _sample_payload(idx, spend="2.2500" if idx % 3 == 0 else "0.0000")
        t = base_time + timedelta(seconds=idx)
        msgs_a = inj_a.process_event(payload, t)
        msgs_b = inj_b.process_event(payload, t)
        assert msgs_a == msgs_b

    manifest_a = inj_a.build_manifest(run_id="r1", speed_factor=600.0)
    manifest_b = inj_b.build_manifest(run_id="r1", speed_factor=600.0)
    assert manifest_a == manifest_b
    assert inj_a.late_records == inj_b.late_records


def test_fault_rates_converge_on_100k_events() -> None:
    """Over 100,000 events, duplicate (2%), late (2%), and corrupt (1%) stay within +-0.5 pp."""
    n = 100_000
    base_time = datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC)
    inj = FaultInjector(seed=12345)

    for idx in range(n):
        payload = _sample_payload(idx)
        inj.process_event(payload, base_time + timedelta(seconds=idx))

    manifest = inj.build_manifest(run_id="bench100k", speed_factor=600.0)
    assert manifest["rows_read"] == n

    dup_pct = (manifest["injected_duplicates"] / n) * 100.0
    late_pct = (manifest["injected_late"]["total"] / n) * 100.0
    corrupt_pct = (manifest["injected_corrupt"]["total"] / n) * 100.0

    assert abs(dup_pct - 2.0) <= 0.5, f"Duplicate rate {dup_pct:.3f}% deviated > 0.5 pp"
    assert abs(late_pct - 2.0) <= 0.5, f"Late rate {late_pct:.3f}% deviated > 0.5 pp"
    assert abs(corrupt_pct - 1.0) <= 0.5, f"Corrupt rate {corrupt_pct:.3f}% deviated > 0.5 pp"

    # Corrupt events must be excluded from expected_unique_valid_events
    assert (
        manifest["expected_unique_valid_events"] + manifest["injected_corrupt"]["total"] == n
    )
    # Verify all 6 corruption types are evenly represented
    by_type = manifest["injected_corrupt"]["by_error_type"]
    assert set(by_type.keys()) == set(CORRUPT_TYPES)
    assert max(by_type.values()) - min(by_type.values()) <= 1


@pytest.mark.parametrize(
    ("corrupt_fn", "expected_error"),
    [
        (inject_corrupt_json, "corrupt_json"),
        (inject_missing_required, "missing_required"),
        (inject_bad_enum, "bad_enum"),
        (lambda p: inject_malformed_decimal(p, use_float=False), "malformed_decimal"),
        (lambda p: inject_malformed_decimal(p, use_float=True), "malformed_decimal"),
        (inject_negative_amount, "negative_amount"),
        (inject_bad_timestamp, "bad_timestamp"),
    ],
)
def test_each_corrupt_injector_matches_contract_error_type(
    corrupt_fn: Any, expected_error: str
) -> None:
    """Every pure corruption helper triggers the exact matching error_type in validate_event."""
    bad_raw = corrupt_fn(_sample_payload(0))
    res = validate_event(bad_raw)
    assert res.is_valid is False
    assert expected_error in res.error_types


def test_apply_corruption_cycles_all_six_types() -> None:
    rng = random.Random(7)
    base = _sample_payload(1)
    for ctype in CORRUPT_TYPES:
        raw = apply_corruption(base, ctype, rng=rng)
        res = validate_event(raw)
        assert res.is_valid is False
        assert ctype in res.error_types


def test_corrupt_excluded_duplicate_once_late_preserves_event_time() -> None:
    """Verify spend accounting and simulated delay invariants explicitly."""
    t0 = datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC)

    # Corrupt only -> 0 valid events, 0.0000 expected spend
    inj_c = FaultInjector(
        seed=1, config=FaultConfig(duplicate_rate=0.0, late_rate=0.0, corrupt_rate=1.0)
    )
    out_c = inj_c.process_event(_sample_payload(0, spend="5.0000"), t0)
    assert len(out_c) == 1 and out_c[0].kind == "corrupt"
    mc = inj_c.build_manifest(run_id="c", speed_factor=600.0)
    assert mc["expected_unique_valid_events"] == 0
    assert mc["expected_spend_total"] == "0.0000"

    # Duplicate only -> 2 emitted messages (same event_id), 1 unique valid event, 1x spend
    inj_d = FaultInjector(
        seed=1, config=FaultConfig(duplicate_rate=1.0, late_rate=0.0, corrupt_rate=0.0)
    )
    out_d = inj_d.process_event(_sample_payload(1, spend="3.2500"), t0)
    assert len(out_d) == 2
    assert out_d[0].raw_json == out_d[1].raw_json
    assert t0 <= out_d[1].emit_sim_time <= t0 + timedelta(seconds=60)
    md = inj_d.build_manifest(run_id="d", speed_factor=600.0)
    assert md["expected_unique_valid_events"] == 1
    assert md["expected_spend_total"] == "3.2500"

    # Late only -> original event_time untouched, emit_sim_time delayed 5..15 minutes
    inj_l = FaultInjector(
        seed=1, config=FaultConfig(duplicate_rate=0.0, late_rate=1.0, corrupt_rate=0.0)
    )
    out_l = inj_l.process_event(_sample_payload(2, spend="4.0000"), t0)
    assert len(out_l) == 1 and out_l[0].kind == "late"
    assert json.loads(out_l[0].raw_json)["event_time"] == "2026-01-01T00:00:00Z"
    assert t0 + timedelta(minutes=5) <= out_l[0].emit_sim_time <= t0 + timedelta(minutes=15)
    assert len(inj_l.late_records) == 1
    assert 5.0 <= inj_l.late_records[0].delay_minutes <= 15.0
