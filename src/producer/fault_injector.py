"""Deterministic fault injection and ground-truth manifest generator (Task 2.6).

Injects controlled anomalies according to a seeded RNG:
- 2% Duplicate: re-send exact same message (same event_id) after 0-60 simulated seconds.
- 2% Late: keep original event_time, delay emission by 5-15 simulated minutes.
- 1% Corrupt: evenly split across corrupt_json, missing_required, bad_enum,
  malformed_decimal, negative_amount, and bad_timestamp.

Maintains exact ground-truth counters so corrupt messages are NEVER counted in
expected_unique_valid_events or expected_spend_total, and exports late_ids.jsonl
for downstream watermark reconciliation (Task 3.5).
"""

from __future__ import annotations

import json
import random
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any

CORRUPT_TYPES: tuple[str, ...] = (
    "corrupt_json",
    "missing_required",
    "bad_enum",
    "malformed_decimal",
    "negative_amount",
    "bad_timestamp",
)


@dataclass(frozen=True)
class FaultConfig:
    """Configuration rates and delay ranges for fault injection."""

    duplicate_rate: float = 0.02
    late_rate: float = 0.02
    corrupt_rate: float = 0.01
    dup_delay_min_sec: float = 0.0
    dup_delay_max_sec: float = 60.0
    late_delay_min_min: float = 5.0
    late_delay_max_min: float = 15.0
    watermark_threshold_min: float = 10.0

    def __post_init__(self) -> None:
        total = self.duplicate_rate + self.late_rate + self.corrupt_rate
        if total < 0 or total > 1.0:
            raise ValueError(f"Sum of fault rates must be in [0, 1], got {total}")


@dataclass(frozen=True)
class ScheduledMessage:
    """A single wire message scheduled for emission at emit_sim_time."""

    emit_sim_time: datetime
    key: str
    raw_json: str
    kind: str  # "clean" | "duplicate" | "late" | "corrupt"
    error_type: str | None = None
    delay_minutes: float = 0.0


@dataclass(frozen=True)
class LateRecord:
    """Ground-truth record for a late-injected event (used by Task 3.5 reconcile)."""

    event_id: str
    delay_minutes: float
    spend: str

    def to_json(self) -> str:
        """Serialize as compact single-line JSON for late_ids.jsonl."""
        return json.dumps(
            {
                "event_id": self.event_id,
                "delay_minutes": round(self.delay_minutes, 4),
                "spend": self.spend,
            },
            separators=(",", ":"),
        )


def inject_corrupt_json(payload: dict[str, Any]) -> str:
    """Return a syntactically truncated/corrupt JSON string."""
    encoded = json.dumps(payload, separators=(",", ":"))
    cut = max(2, len(encoded) // 2)
    return encoded[:cut]


def inject_missing_required(payload: dict[str, Any]) -> str:
    """Drop required field 'campaign_id' and return JSON string."""
    mutated = deepcopy(payload)
    mutated.pop("campaign_id", None)
    return json.dumps(mutated, separators=(",", ":"))


def inject_bad_enum(payload: dict[str, Any]) -> str:
    """Set an unsupported event_type outside the 4 valid funnel values."""
    mutated = deepcopy(payload)
    mutated["event_type"] = "invalid_event_xyz"
    return json.dumps(mutated, separators=(",", ":"))


def inject_malformed_decimal(payload: dict[str, Any], *, use_float: bool = False) -> str:
    """Inject malformed spend (either raw JSON float or non-numeric string)."""
    mutated = deepcopy(payload)
    mutated["spend"] = 12.5 if use_float else "not_a_decimal"
    return json.dumps(mutated, separators=(",", ":"))


def inject_negative_amount(payload: dict[str, Any]) -> str:
    """Inject negative spend violating non-negativity contract."""
    mutated = deepcopy(payload)
    mutated["spend"] = "-5.0000"
    return json.dumps(mutated, separators=(",", ":"))


def inject_bad_timestamp(payload: dict[str, Any]) -> str:
    """Inject naive non-ISO timestamp violating UTC ISO-8601 contract."""
    mutated = deepcopy(payload)
    mutated["event_time"] = "2026-01-01 00:00:00"
    return json.dumps(mutated, separators=(",", ":"))


def apply_corruption(
    payload: dict[str, Any],
    error_type: str,
    rng: random.Random | None = None,
) -> str:
    """Apply the requested corruption type to a payload dict and return raw JSON string."""
    if error_type == "corrupt_json":
        return inject_corrupt_json(payload)
    if error_type == "missing_required":
        return inject_missing_required(payload)
    if error_type == "bad_enum":
        return inject_bad_enum(payload)
    if error_type == "malformed_decimal":
        use_float = bool(rng.random() < 0.5) if rng is not None else False
        return inject_malformed_decimal(payload, use_float=use_float)
    if error_type == "negative_amount":
        return inject_negative_amount(payload)
    if error_type == "bad_timestamp":
        return inject_bad_timestamp(payload)
    raise ValueError(f"Unsupported corruption type: {error_type!r}")


class FaultInjector:
    """Deterministic seeded fault injector and ground-truth manifest accumulator."""

    def __init__(self, seed: int = 42, config: FaultConfig | None = None) -> None:
        self.seed = seed
        self.config = config or FaultConfig()
        self.rng = random.Random(seed)
        self._corrupt_idx = 0

        # Ground-truth counters
        self.rows_read: int = 0
        self.sent_valid_unique: int = 0
        self.injected_duplicates: int = 0
        self.injected_late: int = 0
        self.injected_late_over_10m: int = 0
        self.corrupt_by_type: dict[str, int] = dict.fromkeys(CORRUPT_TYPES, 0)
        self.expected_spend_total: Decimal = Decimal("0.0000")
        self.late_records: list[LateRecord] = []

    def process_event(
        self,
        payload: dict[str, Any],
        sim_time: datetime,
    ) -> list[ScheduledMessage]:
        """Decide whether to emit clean, duplicate, late, or corrupt message(s)."""
        self.rows_read += 1
        key = str(payload.get("campaign_id") or "")
        spend_dec = Decimal(str(payload["spend"]))
        event_id = str(payload["event_id"])

        draw = self.rng.random()
        corrupt_cutoff = self.config.corrupt_rate
        late_cutoff = corrupt_cutoff + self.config.late_rate
        dup_cutoff = late_cutoff + self.config.duplicate_rate

        # 1. Corrupt (~1%): evenly cycled across all 6 error types; NOT counted as valid/spend
        if draw < corrupt_cutoff:
            ctype = CORRUPT_TYPES[self._corrupt_idx % len(CORRUPT_TYPES)]
            self._corrupt_idx += 1
            self.corrupt_by_type[ctype] += 1
            corrupt_raw = apply_corruption(payload, ctype, rng=self.rng)
            return [
                ScheduledMessage(
                    emit_sim_time=sim_time,
                    key=key,
                    raw_json=corrupt_raw,
                    kind="corrupt",
                    error_type=ctype,
                )
            ]

        raw_valid = json.dumps(payload, separators=(",", ":"))

        # 2. Late (~2%): keep original event_time, delay send 5-15 simulated minutes
        if draw < late_cutoff:
            delay_min = self.rng.uniform(
                self.config.late_delay_min_min, self.config.late_delay_max_min
            )
            self.sent_valid_unique += 1
            self.expected_spend_total += spend_dec
            self.injected_late += 1
            if delay_min > self.config.watermark_threshold_min:
                self.injected_late_over_10m += 1
            self.late_records.append(
                LateRecord(
                    event_id=event_id,
                    delay_minutes=delay_min,
                    spend=f"{spend_dec:.4f}",
                )
            )
            return [
                ScheduledMessage(
                    emit_sim_time=sim_time + timedelta(minutes=delay_min),
                    key=key,
                    raw_json=raw_valid,
                    kind="late",
                    delay_minutes=delay_min,
                )
            ]

        # 3. Duplicate (~2%): emit original now + identical duplicate after 0-60s simulated
        if draw < dup_cutoff:
            dup_delay_sec = self.rng.uniform(
                self.config.dup_delay_min_sec, self.config.dup_delay_max_sec
            )
            self.sent_valid_unique += 1
            self.expected_spend_total += spend_dec
            self.injected_duplicates += 1
            return [
                ScheduledMessage(
                    emit_sim_time=sim_time,
                    key=key,
                    raw_json=raw_valid,
                    kind="clean",
                ),
                ScheduledMessage(
                    emit_sim_time=sim_time + timedelta(seconds=dup_delay_sec),
                    key=key,
                    raw_json=raw_valid,
                    kind="duplicate",
                ),
            ]

        # 4. Clean (~95%)
        self.sent_valid_unique += 1
        self.expected_spend_total += spend_dec
        return [
            ScheduledMessage(
                emit_sim_time=sim_time,
                key=key,
                raw_json=raw_valid,
                kind="clean",
            )
        ]

    def build_manifest(
        self,
        *,
        run_id: str,
        speed_factor: float,
        no_faults: bool = False,
        late_ids_file: str | None = None,
        interrupted: bool = False,
    ) -> dict[str, Any]:
        """Build ground-truth replay manifest dict for reconciliation (Task 3.5)."""
        corrupt_total = sum(self.corrupt_by_type.values())
        return {
            "run_id": run_id,
            "seed": self.seed,
            "speed_factor": speed_factor,
            "no_faults": no_faults,
            "interrupted": interrupted,
            "rows_read": self.rows_read,
            "sent_valid_unique": self.sent_valid_unique,
            "injected_duplicates": self.injected_duplicates,
            "injected_late": {
                "total": self.injected_late,
                "over_10_minutes": self.injected_late_over_10m,
                "within_10_minutes": self.injected_late - self.injected_late_over_10m,
            },
            "injected_corrupt": {
                "total": corrupt_total,
                "by_error_type": dict(self.corrupt_by_type),
            },
            "expected_unique_valid_events": self.sent_valid_unique,
            "expected_spend_total": f"{self.expected_spend_total:.4f}",
            "late_ids_file": late_ids_file,
        }
