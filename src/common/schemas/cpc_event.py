"""Canonical CPC event schema (v1) and pure validation contract.

Defines CpcEventV1, ValidationResult, and validate_event(raw: str) aligned with:
- Task 2.1 (4 funnel event types: click, apply, qualified, unqualified)
- ADR-007 (DECIMAL(18,4) serialized as string in JSON, ISO-8601 UTC timestamps)
- ADR-009 (spend = bid for click, '0.0000' for non-click events)
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Literal, cast

from pydantic import BaseModel, ConfigDict, Field, field_validator

SUPPORTED_SCHEMA_VERSION = "v1"
UTC = timezone.utc

EventType = Literal["click", "apply", "qualified", "unqualified"]
VALID_EVENT_TYPES: tuple[str, ...] = ("click", "apply", "qualified", "unqualified")

ErrorType = Literal[
    "corrupt_json",
    "missing_required",
    "bad_enum",
    "malformed_decimal",
    "negative_amount",
    "bad_timestamp",
    "schema_version_unsupported",
]

REQUIRED_NON_EMPTY_STRING_FIELDS: tuple[str, ...] = (
    "event_id",
    "event_time",
    "produced_at",
    "event_type",
    "job_id",
    "campaign_id",
    "publisher_id",
    "spend",
)

ALLOWED_FIELDS: frozenset[str] = frozenset(
    {
        "schema_version",
        "event_id",
        "event_time",
        "produced_at",
        "event_type",
        "job_id",
        "campaign_id",
        "group_id",
        "publisher_id",
        "bid",
        "spend",
        "source_row_id",
    }
)

_UTC_TS_REGEX = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|\+00:00)$")
_DECIMAL_STR_REGEX = re.compile(r"^[+-]?\d+(?:\.\d+)?$")
_MAX_ABS_DECIMAL_18_4 = Decimal("100000000000000")  # 10^14 (14 integer + 4 scale = DECIMAL(18,4))
_SCHEMA_FILE = Path(__file__).resolve().with_name("cpc_event.v1.json")


class ValidationErrorDetail(BaseModel):
    """Structured validation failure reason mapped to canonical error_type."""

    error_type: ErrorType
    field: str | None = None
    detail: str


class CpcEventV1(BaseModel):
    """Validated canonical CPC event payload for Redpanda topic cpc_events_raw."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["v1"] = "v1"
    event_id: str = Field(min_length=1, max_length=64)
    event_time: datetime
    produced_at: datetime
    event_type: EventType
    job_id: str = Field(min_length=1)
    campaign_id: str = Field(min_length=1)
    group_id: str | None = None
    publisher_id: str = Field(min_length=1)
    bid: Decimal | None = None
    spend: Decimal
    source_row_id: int = Field(ge=0)

    @field_validator("event_time", "produced_at")
    @classmethod
    def _ensure_utc(cls, value: datetime) -> datetime:
        """Ensure timestamps always carry UTC tzinfo."""
        if value.tzinfo is None:
            raise ValueError("Timestamp must be timezone-aware UTC")
        return value.astimezone(UTC)

    def to_wire_dict(self) -> dict[str, Any]:
        """Serialize to JSON-ready dict complying with ADR-007 (DECIMAL as string, UTC ISO)."""
        return {
            "schema_version": self.schema_version,
            "event_id": self.event_id,
            "event_time": _format_utc(self.event_time),
            "produced_at": _format_utc(self.produced_at),
            "event_type": self.event_type,
            "job_id": self.job_id,
            "campaign_id": self.campaign_id,
            "group_id": self.group_id,
            "publisher_id": self.publisher_id,
            "bid": f"{self.bid:.4f}" if self.bid is not None else None,
            "spend": f"{self.spend:.4f}",
            "source_row_id": self.source_row_id,
        }


class ValidationResult(BaseModel):
    """Outcome of validating a raw JSON event message."""

    is_valid: bool
    event: CpcEventV1 | None = None
    errors: list[ValidationErrorDetail] = Field(default_factory=list)

    @property
    def primary_error_type(self) -> ErrorType | None:
        """Return first error_type for DLQ routing, or None when valid."""
        return self.errors[0].error_type if self.errors else None

    @property
    def error_types(self) -> list[ErrorType]:
        """Return list of error_type codes raised for this record."""
        return [err.error_type for err in self.errors]


def load_json_schema() -> dict[str, Any]:
    """Load the Draft 2020-12 JSON Schema definition from cpc_event.v1.json."""
    with _SCHEMA_FILE.open(encoding="utf-8") as fh:
        return json.load(fh)


def _format_utc(dt: datetime) -> str:
    """Format a UTC datetime in canonical ISO-8601 Z representation."""
    utc_dt = dt.astimezone(UTC)
    if utc_dt.microsecond:
        return utc_dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    return utc_dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _reject_non_finite(token: str) -> Any:
    """Reject non-standard JSON constants such as NaN, Infinity, -Infinity."""
    raise ValueError(f"Non-standard JSON constant not allowed: {token}")


def _parse_utc_timestamp(value: Any, field: str) -> tuple[datetime | None, ValidationErrorDetail | None]:
    """Validate ISO-8601 UTC string and parse into timezone-aware UTC datetime."""
    if not isinstance(value, str) or not _UTC_TS_REGEX.fullmatch(value):
        return None, ValidationErrorDetail(
            error_type="bad_timestamp",
            field=field,
            detail=f"Expected ISO-8601 UTC string ending with Z or +00:00, got {value!r}",
        )
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)
    except ValueError as exc:
        return None, ValidationErrorDetail(
            error_type="bad_timestamp",
            field=field,
            detail=f"Invalid calendar timestamp {value!r}: {exc}",
        )
    return parsed, None


def _parse_decimal_amount(
    value: Any, field: str
) -> tuple[Decimal | None, ValidationErrorDetail | None]:
    """Validate string-encoded non-negative DECIMAL(18,4) amount (rejects raw float/int)."""
    if not isinstance(value, str):
        return None, ValidationErrorDetail(
            error_type="malformed_decimal",
            field=field,
            detail=(
                f"Field '{field}' must be string-encoded decimal per ADR-007, "
                f"got {type(value).__name__}"
            ),
        )

    stripped = value.strip()
    if not stripped or not _DECIMAL_STR_REGEX.fullmatch(stripped):
        return None, ValidationErrorDetail(
            error_type="malformed_decimal",
            field=field,
            detail=f"Invalid decimal string for '{field}': {value!r}",
        )

    try:
        amount = Decimal(stripped)
    except InvalidOperation as exc:
        return None, ValidationErrorDetail(
            error_type="malformed_decimal",
            field=field,
            detail=f"Cannot parse decimal for '{field}': {exc}",
        )

    if amount < Decimal("0"):
        return None, ValidationErrorDetail(
            error_type="negative_amount",
            field=field,
            detail=f"Field '{field}' must be non-negative, got {stripped}",
        )

    frac_part = stripped.split(".", 1)[1] if "." in stripped else ""
    if len(frac_part) > 4 or abs(amount) >= _MAX_ABS_DECIMAL_18_4:
        return None, ValidationErrorDetail(
            error_type="malformed_decimal",
            field=field,
            detail=f"Field '{field}' exceeds DECIMAL(18,4) precision/scale bounds: {value!r}",
        )

    return amount, None


def _is_missing_string(value: Any) -> bool:
    """Return True when a required string field is missing, None, empty, or wrong type."""
    return value is None or (isinstance(value, str) and value.strip() == "")


def validate_event(raw: str) -> ValidationResult:
    """Validate raw JSON message string and classify errors for Bronze/DLQ routing."""
    if not isinstance(raw, str):
        return ValidationResult(
            is_valid=False,
            errors=[
                ValidationErrorDetail(
                    error_type="corrupt_json",
                    detail=f"Expected raw JSON string, got {type(raw).__name__}",
                )
            ],
        )

    try:
        payload = json.loads(raw, parse_constant=_reject_non_finite)
    except (json.JSONDecodeError, ValueError) as exc:
        return ValidationResult(
            is_valid=False,
            errors=[ValidationErrorDetail(error_type="corrupt_json", detail=f"Invalid JSON: {exc}")],
        )

    if not isinstance(payload, dict):
        return ValidationResult(
            is_valid=False,
            errors=[
                ValidationErrorDetail(
                    error_type="corrupt_json",
                    detail=f"Top-level JSON value must be an object, got {type(payload).__name__}",
                )
            ],
        )

    errors: list[ValidationErrorDetail] = []

    # 1. schema_version check
    raw_ver = payload.get("schema_version")
    if _is_missing_string(raw_ver):
        errors.append(
            ValidationErrorDetail(
                error_type="missing_required",
                field="schema_version",
                detail="Missing required field 'schema_version'",
            )
        )
    elif raw_ver != SUPPORTED_SCHEMA_VERSION:
        errors.append(
            ValidationErrorDetail(
                error_type="schema_version_unsupported",
                field="schema_version",
                detail=(
                    f"Unsupported schema_version {raw_ver!r}; "
                    f"expected {SUPPORTED_SCHEMA_VERSION!r}"
                ),
            )
        )

    # 2. Required fields presence & type checks
    missing_fields: set[str] = set()
    for field in REQUIRED_NON_EMPTY_STRING_FIELDS:
        val = payload.get(field)
        if _is_missing_string(val):
            missing_fields.add(field)
            errors.append(
                ValidationErrorDetail(
                    error_type="missing_required",
                    field=field,
                    detail=f"Missing or empty required field '{field}'",
                )
            )
        elif field in ("event_id", "job_id", "campaign_id", "publisher_id") and not isinstance(
            val, str
        ):
            missing_fields.add(field)
            errors.append(
                ValidationErrorDetail(
                    error_type="missing_required",
                    field=field,
                    detail=f"Field '{field}' must be a non-empty string, got {type(val).__name__}",
                )
            )

    if "event_id" not in missing_fields and isinstance(payload.get("event_id"), str):
        if len(payload["event_id"]) > 64:
            errors.append(
                ValidationErrorDetail(
                    error_type="missing_required",
                    field="event_id",
                    detail="Field 'event_id' exceeds maximum length 64",
                )
            )

    # Validate source_row_id presence, integer type, and non-negativity
    row_id = payload.get("source_row_id")
    if "source_row_id" not in payload or row_id is None:
        errors.append(
            ValidationErrorDetail(
                error_type="missing_required",
                field="source_row_id",
                detail="Missing required field 'source_row_id'",
            )
        )
    elif not isinstance(row_id, int) or isinstance(row_id, bool):
        errors.append(
            ValidationErrorDetail(
                error_type="missing_required",
                field="source_row_id",
                detail=f"Field 'source_row_id' must be integer, got {type(row_id).__name__}",
            )
        )
    elif row_id < 0:
        errors.append(
            ValidationErrorDetail(
                error_type="negative_amount",
                field="source_row_id",
                detail=f"Field 'source_row_id' must be >= 0, got {row_id}",
            )
        )

    # Reject unknown fields (additionalProperties: false)
    extra_fields = sorted(set(payload.keys()) - ALLOWED_FIELDS)
    if extra_fields:
        errors.append(
            ValidationErrorDetail(
                error_type="missing_required",
                field=extra_fields[0],
                detail=f"Unexpected fields not permitted by schema v1: {extra_fields}",
            )
        )

    # Validate optional group_id type when provided
    group_id = payload.get("group_id")
    if group_id is not None and not isinstance(group_id, str):
        errors.append(
            ValidationErrorDetail(
                error_type="missing_required",
                field="group_id",
                detail=f"Optional field 'group_id' must be string or null, got {type(group_id).__name__}",
            )
        )

    # 3. event_type enum check
    raw_event_type = payload.get("event_type")
    if "event_type" not in missing_fields and (
        not isinstance(raw_event_type, str) or raw_event_type not in VALID_EVENT_TYPES
    ):
        errors.append(
            ValidationErrorDetail(
                error_type="bad_enum",
                field="event_type",
                detail=f"Invalid event_type {raw_event_type!r}; allowed={list(VALID_EVENT_TYPES)}",
            )
        )

    # Conditional requirement: bid is mandatory when event_type == 'click'
    raw_bid = payload.get("bid")
    if raw_event_type == "click" and _is_missing_string(raw_bid):
        missing_fields.add("bid")
        errors.append(
            ValidationErrorDetail(
                error_type="missing_required",
                field="bid",
                detail="Field 'bid' is required when event_type == 'click'",
            )
        )

    # 4. Timestamp checks
    parsed_event_time: datetime | None = None
    if "event_time" not in missing_fields:
        parsed_event_time, err = _parse_utc_timestamp(payload.get("event_time"), "event_time")
        if err:
            errors.append(err)

    parsed_produced_at: datetime | None = None
    if "produced_at" not in missing_fields:
        parsed_produced_at, err = _parse_utc_timestamp(payload.get("produced_at"), "produced_at")
        if err:
            errors.append(err)

    # 5. Decimal amount checks (spend required, bid required for click or validated when non-null)
    parsed_spend: Decimal | None = None
    if "spend" not in missing_fields:
        parsed_spend, err = _parse_decimal_amount(payload.get("spend"), "spend")
        if err:
            errors.append(err)

    parsed_bid: Decimal | None = None
    if "bid" not in missing_fields and raw_bid is not None:
        parsed_bid, err = _parse_decimal_amount(raw_bid, "bid")
        if err:
            errors.append(err)

    if errors:
        return ValidationResult(is_valid=False, event=None, errors=errors)

    assert parsed_event_time is not None
    assert parsed_produced_at is not None
    assert parsed_spend is not None
    assert isinstance(row_id, int) and not isinstance(row_id, bool)

    event = CpcEventV1(
        schema_version=SUPPORTED_SCHEMA_VERSION,
        event_id=payload["event_id"],
        event_time=parsed_event_time,
        produced_at=parsed_produced_at,
        event_type=cast(EventType, raw_event_type),
        job_id=payload["job_id"],
        campaign_id=payload["campaign_id"],
        group_id=group_id,
        publisher_id=payload["publisher_id"],
        bid=parsed_bid,
        spend=parsed_spend,
        source_row_id=row_id,
    )
    return ValidationResult(is_valid=True, event=event, errors=[])
