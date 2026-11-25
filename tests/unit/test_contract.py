"""Unit tests for Task 2.2 — Canonical Data Contract & pure validate_event()."""

from __future__ import annotations

import json
from copy import deepcopy
from decimal import Decimal
from typing import Any

import pytest
from jsonschema import Draft202012Validator

from src.common.schemas.cpc_event import (
    CpcEventV1,
    load_json_schema,
    validate_event,
)


def _valid_payload(**overrides: Any) -> dict[str, Any]:
    """Return a clean baseline valid click event payload."""
    base: dict[str, Any] = {
        "schema_version": "v1",
        "event_id": "a94a8fe5ccb19ba61c4c0873d391e987982fbbd3",
        "event_time": "2026-01-01T10:15:30Z",
        "produced_at": "2026-01-01T10:15:31.123456+00:00",
        "event_type": "click",
        "job_id": "98",
        "campaign_id": "4",
        "group_id": "12",
        "publisher_id": "1",
        "bid": "1.5000",
        "spend": "1.5000",
        "source_row_id": 1,
    }
    base.update(overrides)
    return base


def _to_json(payload: dict[str, Any]) -> str:
    return json.dumps(payload)


def test_json_schema_itself_is_valid_draft_2020_12() -> None:
    """Verify cpc_event.v1.json conforms to JSON Schema Draft 2020-12."""
    schema = load_json_schema()
    Draft202012Validator.check_schema(schema)
    validator = Draft202012Validator(schema)

    valid_click = _valid_payload()
    assert list(validator.iter_errors(valid_click)) == []

    valid_apply = _valid_payload(
        event_type="apply",
        group_id=None,
        bid=None,
        spend="0.0000",
    )
    assert list(validator.iter_errors(valid_apply)) == []

    # Click missing bid violates JSON Schema if/then constraint
    bad_click = _valid_payload(bid=None)
    assert len(list(validator.iter_errors(bad_click))) > 0

    # Numeric float bid violates JSON Schema string constraint
    float_bid = _valid_payload(bid=12.5)
    assert len(list(validator.iter_errors(float_bid))) > 0


# ---------------------------------------------------------------------------
# 1. corrupt_json (valid boundary vs violation)
# ---------------------------------------------------------------------------
def test_corrupt_json_valid_and_invalid() -> None:
    # Valid boundary: minimal valid JSON object with nullable optional fields
    valid_raw = _to_json(_valid_payload(group_id=None, bid="0.0000", spend="0.0000"))
    ok = validate_event(valid_raw)
    assert ok.is_valid is True
    assert ok.errors == []

    # Violations: malformed syntax, non-object JSON, NaN token
    for bad_raw in ('{"schema_version": "v1"', "[1, 2, 3]", '{"bid": NaN}'):
        res = validate_event(bad_raw)
        assert res.is_valid is False
        assert res.primary_error_type == "corrupt_json"


# ---------------------------------------------------------------------------
# 2. missing_required (valid boundary vs violation)
# ---------------------------------------------------------------------------
def test_missing_required_valid_and_invalid() -> None:
    # Valid boundary: non-click event with group_id=None and bid=None
    ok_non_click = validate_event(
        _to_json(_valid_payload(event_type="qualified", group_id=None, bid=None, spend="0.0000"))
    )
    assert ok_non_click.is_valid is True
    assert ok_non_click.event is not None
    assert ok_non_click.event.bid is None

    # Violation A: missing required top-level key
    payload_missing = deepcopy(_valid_payload())
    del payload_missing["campaign_id"]
    res_missing = validate_event(_to_json(payload_missing))
    assert res_missing.is_valid is False
    assert "missing_required" in res_missing.error_types

    # Violation B: click event with bid=None or whitespace
    res_click_no_bid = validate_event(_to_json(_valid_payload(event_type="click", bid=None)))
    assert res_click_no_bid.is_valid is False
    assert "missing_required" in res_click_no_bid.error_types

    # Violation C: unexpected extra property
    res_extra = validate_event(_to_json(_valid_payload(unexpected_col="x")))
    assert res_extra.is_valid is False
    assert "missing_required" in res_extra.error_types


# ---------------------------------------------------------------------------
# 3. bad_enum (valid boundary vs violation)
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("event_type", ["click", "apply", "qualified", "unqualified"])
def test_bad_enum_valid_funnel_types(event_type: str) -> None:
    payload = _valid_payload(
        event_type=event_type,
        bid="2.0000" if event_type == "click" else "0.0000",
        spend="2.0000" if event_type == "click" else "0.0000",
    )
    res = validate_event(_to_json(payload))
    assert res.is_valid is True


def test_bad_enum_rejects_impression_and_unknown() -> None:
    # 'impression' was excluded in Task 2.1
    for bad_type in ("impression", "CLICK", "conversion", "unknown"):
        res = validate_event(_to_json(_valid_payload(event_type=bad_type)))
        assert res.is_valid is False
        assert "bad_enum" in res.error_types


# ---------------------------------------------------------------------------
# 4. malformed_decimal (valid boundary vs float/int/non-numeric/overflow)
# ---------------------------------------------------------------------------
def test_malformed_decimal_valid_and_invalid() -> None:
    # Valid boundary: max DECIMAL(18,4) precision and 0
    ok_max = validate_event(
        _to_json(_valid_payload(bid="99999999999999.9999", spend="0.0000"))
    )
    assert ok_max.is_valid is True
    assert ok_max.event is not None
    assert ok_max.event.bid == Decimal("99999999999999.9999")

    # Violation A: float JSON number in bid / spend (IEEE 754 risk)
    res_float_bid = validate_event(_to_json(_valid_payload(bid=12.5)))
    assert res_float_bid.is_valid is False
    assert res_float_bid.primary_error_type == "malformed_decimal"

    res_float_spend = validate_event(_to_json(_valid_payload(spend=12.5)))
    assert res_float_spend.is_valid is False
    assert res_float_spend.primary_error_type == "malformed_decimal"

    # Violation B: int JSON number
    res_int_bid = validate_event(_to_json(_valid_payload(bid=12)))
    assert res_int_bid.is_valid is False
    assert res_int_bid.primary_error_type == "malformed_decimal"

    # Violation C: non-numeric string and scale/precision overflow
    for bad_dec in ("abc", "$1.50", "1.12345", "100000000000000.0000"):
        res_bad = validate_event(_to_json(_valid_payload(bid=bad_dec)))
        assert res_bad.is_valid is False, f"Expected failure for {bad_dec!r}"
        assert "malformed_decimal" in res_bad.error_types


# ---------------------------------------------------------------------------
# 5. negative_amount (valid boundary vs negative bid/spend/source_row_id)
# ---------------------------------------------------------------------------
def test_negative_amount_valid_and_invalid() -> None:
    # Valid boundary: zero bid/spend and source_row_id = 0
    ok_zero = validate_event(
        _to_json(_valid_payload(bid="0.0000", spend="0.0000", source_row_id=0))
    )
    assert ok_zero.is_valid is True

    # Violations: negative spend, negative bid, negative source_row_id
    for overrides in (
        {"spend": "-0.0001"},
        {"bid": "-5.0000"},
        {"source_row_id": -1},
    ):
        res = validate_event(_to_json(_valid_payload(**overrides)))
        assert res.is_valid is False
        assert "negative_amount" in res.error_types


# ---------------------------------------------------------------------------
# 6. bad_timestamp (valid UTC boundary vs naive / non-UTC / impossible date)
# ---------------------------------------------------------------------------
def test_bad_timestamp_valid_and_invalid() -> None:
    # Valid boundary: exact midnight Z and microsecond +00:00
    ok_ts = validate_event(
        _to_json(
            _valid_payload(
                event_time="2026-01-01T00:00:00Z",
                produced_at="2026-12-31T23:59:59.999999+00:00",
            )
        )
    )
    assert ok_ts.is_valid is True

    # Violations: naive timestamp, local +07:00 offset, impossible Feb 30 date
    for overrides in (
        {"event_time": "2026-01-01T00:00:00"},
        {"event_time": "2026-01-01T07:00:00+07:00"},
        {"produced_at": "2026-02-30T12:00:00Z"},
    ):
        res = validate_event(_to_json(_valid_payload(**overrides)))
        assert res.is_valid is False
        assert "bad_timestamp" in res.error_types


# ---------------------------------------------------------------------------
# 7. schema_version_unsupported (valid 'v1' vs unsupported versions)
# ---------------------------------------------------------------------------
def test_schema_version_unsupported_valid_and_invalid() -> None:
    # Valid boundary
    ok = validate_event(_to_json(_valid_payload(schema_version="v1")))
    assert ok.is_valid is True

    # Violations: 'v2' and integer 1
    for bad_ver in ("v2", 1):
        res = validate_event(_to_json(_valid_payload(schema_version=bad_ver)))
        assert res.is_valid is False
        assert "schema_version_unsupported" in res.error_types


def test_wire_roundtrip_matches_json_schema() -> None:
    """Ensure CpcEventV1.to_wire_dict() satisfies both jsonschema and validate_event()."""
    raw = _to_json(_valid_payload(bid="3.5", spend="3.5"))
    parsed = validate_event(raw)
    assert parsed.is_valid is True
    assert isinstance(parsed.event, CpcEventV1)

    wire = parsed.event.to_wire_dict()
    assert wire["bid"] == "3.5000"
    assert wire["spend"] == "3.5000"
    assert wire["event_time"].endswith("Z")

    validator = Draft202012Validator(load_json_schema())
    assert list(validator.iter_errors(wire)) == []
    assert validate_event(json.dumps(wire)).is_valid is True
