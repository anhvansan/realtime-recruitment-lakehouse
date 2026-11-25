"""Unit tests for Task 2.5 — TimeMapper and Replay Producer v1 event building."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from src.common.schemas.cpc_event import validate_event
from src.producer.replay_producer import build_event_payload, compute_event_id
from src.producer.time_mapper import TimeMapper, ensure_utc, parse_sim_start


def test_time_mapper_60x_compression_numeric_example() -> None:
    """speed_factor=60 maps 1 simulated hour to exactly 1 real wall-clock minute."""
    orig_t0 = datetime(2022, 7, 7, 3, 0, 0, tzinfo=UTC)
    sim_start = parse_sim_start("2026-01-01")
    mapper = TimeMapper(
        orig_t0=orig_t0,
        sim_start=sim_start,
        t_start_monotonic=1000.0,
        speed_factor=60.0,
    )

    # Boundary start (delta = 0)
    assert mapper.map_sim_time(orig_t0) == datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC)
    assert mapper.target_wall_time(orig_t0) == pytest.approx(1000.0)
    assert mapper.behind_schedule_ms(orig_t0, 999.5) == 0.0

    # 1 simulated hour later -> sim_time +1h, wall clock +60s (1 real minute)
    one_hour_later = orig_t0 + timedelta(hours=1)
    assert mapper.map_sim_time(one_hour_later) == datetime(2026, 1, 1, 1, 0, 0, tzinfo=UTC)
    assert mapper.target_wall_time(one_hour_later) == pytest.approx(1060.0)
    assert mapper.behind_schedule_ms(one_hour_later, 1060.25) == pytest.approx(250.0)


def test_time_mapper_monotonic_sequence_and_naive_input() -> None:
    """Sorted original timestamps map to non-decreasing UTC simulated event_times."""
    orig_t0 = datetime(2022, 7, 7, 3, 42, 58)  # naive treated as UTC
    mapper = TimeMapper(
        orig_t0=orig_t0,
        sim_start=parse_sim_start("2026-01-01T00:00:00Z"),
        t_start_monotonic=0.0,
        speed_factor=600.0,
    )

    timestamps = [
        orig_t0,
        orig_t0 + timedelta(milliseconds=500),
        orig_t0 + timedelta(seconds=48),
        orig_t0 + timedelta(hours=2, minutes=15),
    ]
    mapped = [mapper.map_sim_time(ts) for ts in timestamps]
    assert all(ts.tzinfo == UTC for ts in mapped)
    assert mapped == sorted(mapped)
    assert ensure_utc(mapped[-1]) > mapped[0]


def test_time_mapper_rejects_non_positive_speed_factor() -> None:
    with pytest.raises(ValueError, match="speed_factor"):
        TimeMapper(
            orig_t0=datetime(2022, 7, 7, tzinfo=UTC),
            sim_start=parse_sim_start("2026-01-01"),
            t_start_monotonic=0.0,
            speed_factor=0.0,
        )


def test_build_event_payload_passes_v1_contract_for_click_and_application() -> None:
    """Payloads emitted for both tracking clicks and application funnel events pass validate_event."""
    orig_t0 = datetime(2022, 7, 7, 3, 42, 58, tzinfo=UTC)
    mapper = TimeMapper(
        orig_t0=orig_t0,
        sim_start=parse_sim_start("2026-01-01"),
        t_start_monotonic=100.0,
        speed_factor=600.0,
    )
    fixed_now = datetime(2026, 11, 25, 9, 0, 0, tzinfo=UTC)

    click_row = {
        "source_file": "tracking.csv",
        "source_row_id": 15,
        "orig_ts": orig_t0 + timedelta(seconds=30),
        "event_type": "click",
        "job_id": "98",
        "campaign_id": "4",
        "group_id": None,
        "publisher_id": "1",
        "bid_dec": Decimal("2.5000"),
    }
    click_wire = build_event_payload(click_row, mapper, produced_at=fixed_now)
    assert click_wire["event_id"] == compute_event_id("tracking.csv", 15)
    assert click_wire["bid"] == "2.5000"
    assert click_wire["spend"] == "2.5000"
    assert validate_event(json.dumps(click_wire)).is_valid is True

    app_row = {
        "source_file": "application.csv",
        "source_row_id": 7,
        "orig_ts": orig_t0 + timedelta(minutes=5),
        "event_type": "qualified",
        "job_id": "187",
        "campaign_id": "48",
        "group_id": "12",
        "publisher_id": "",  # simulates fallback to 'unknown'
        "bid_dec": Decimal("0.0000"),
    }
    app_wire = build_event_payload(app_row, mapper, produced_at=fixed_now)
    assert app_wire["publisher_id"] == "unknown"
    assert app_wire["spend"] == "0.0000"
    assert validate_event(json.dumps(app_wire)).is_valid is True
