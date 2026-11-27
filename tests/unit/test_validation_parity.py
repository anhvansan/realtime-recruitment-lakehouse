"""Cross-validation parity tests: Python validate_event() vs PySpark validate_df()."""

from __future__ import annotations

import json
import tempfile
from collections.abc import Callable
from copy import deepcopy
from pathlib import Path
from typing import Any, TypeVar

from src.common.schemas.cpc_event import validate_event
from src.streaming.spark_session import build_spark
from src.streaming.validation import validate_df

try:
    import pytest
except ImportError:  # pragma: no cover - allows standalone execution inside spark container
    pytest = None  # type: ignore[assignment]

_F = TypeVar("_F", bound=Callable[..., Any])


def _mark_spark(func: _F) -> _F:
    if pytest is not None:
        return pytest.mark.spark(func)  # type: ignore[return-value]
    return func


def _valid_payload(**overrides: Any) -> dict[str, Any]:
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


def _build_parity_cases() -> list[tuple[int, str, str]]:
    """Return shared corpus covering valid boundaries and all 7 canonical error_types."""
    missing_camp = deepcopy(_valid_payload())
    del missing_camp["campaign_id"]

    missing_sv = deepcopy(_valid_payload())
    del missing_sv["schema_version"]

    return [
        # --- Valid boundaries ---
        (1, "valid_click", json.dumps(_valid_payload())),
        (
            2,
            "valid_apply_null_opt",
            json.dumps(_valid_payload(event_type="apply", group_id=None, bid=None, spend="0.0000")),
        ),
        (
            3,
            "valid_qualified_zero",
            json.dumps(
                _valid_payload(
                    event_type="qualified",
                    bid="0.0000",
                    spend="0.0000",
                    source_row_id=0,
                )
            ),
        ),
        (
            4,
            "valid_unqualified_max_decimal",
            json.dumps(
                _valid_payload(
                    event_type="unqualified",
                    bid="99999999999999.9999",
                    spend="0",
                    event_time="2026-12-31T23:59:59.999999+00:00",
                )
            ),
        ),
        # --- 1. corrupt_json ---
        (10, "corrupt_truncated", '{"schema_version": "v1"'),
        (11, "corrupt_array", "[1, 2, 3]"),
        (12, "corrupt_nan", '{"bid": NaN}'),
        (13, "corrupt_blank", "   "),
        # --- 2. schema_version_unsupported ---
        (20, "sv_v2", json.dumps(_valid_payload(schema_version="v2"))),
        (21, "sv_int_1", json.dumps(_valid_payload(schema_version=1))),
        # --- 3. missing_required ---
        (30, "missing_schema_version", json.dumps(missing_sv)),
        (31, "missing_campaign_id", json.dumps(missing_camp)),
        (32, "whitespace_job_id", json.dumps(_valid_payload(job_id="   "))),
        (33, "click_null_bid", json.dumps(_valid_payload(event_type="click", bid=None))),
        (34, "extra_field", json.dumps(_valid_payload(unexpected_col="x"))),
        (35, "source_row_id_string", json.dumps(_valid_payload(source_row_id="1"))),
        (36, "source_row_id_bool", json.dumps(_valid_payload(source_row_id=True))),
        (37, "group_id_int", json.dumps(_valid_payload(group_id=123))),
        (38, "event_id_over_64", json.dumps(_valid_payload(event_id="x" * 65))),
        # --- 4. bad_enum ---
        (40, "bad_enum_impression", json.dumps(_valid_payload(event_type="impression"))),
        (41, "bad_enum_uppercase", json.dumps(_valid_payload(event_type="CLICK"))),
        # --- 5. bad_timestamp ---
        (42, "bad_ts_naive", json.dumps(_valid_payload(event_time="2026-01-01T00:00:00"))),
        (43, "bad_ts_offset_07", json.dumps(_valid_payload(event_time="2026-01-01T07:00:00+07:00"))),
        (44, "bad_ts_feb_30", json.dumps(_valid_payload(produced_at="2026-02-30T12:00:00Z"))),
        # --- 6. malformed_decimal (including raw JSON float/int trap!) ---
        (50, "malformed_float_spend", json.dumps(_valid_payload(spend=12.5))),
        (51, "malformed_float_bid", json.dumps(_valid_payload(bid=12.5))),
        (52, "malformed_int_bid", json.dumps(_valid_payload(bid=12))),
        (53, "malformed_alpha", json.dumps(_valid_payload(spend="abc"))),
        (54, "malformed_5_decimals", json.dumps(_valid_payload(bid="1.12345"))),
        (55, "malformed_overflow", json.dumps(_valid_payload(spend="100000000000000.0000"))),
        # --- 7. negative_amount ---
        (60, "negative_spend", json.dumps(_valid_payload(spend="-0.0001"))),
        (61, "negative_bid", json.dumps(_valid_payload(bid="-5.0000"))),
        (62, "negative_source_row_id", json.dumps(_valid_payload(source_row_id=-1))),
    ]


PARITY_CASES = _build_parity_cases()


def test_parity_corpus_covers_all_7_error_types() -> None:
    """Host unit check: parity corpus covers valid events and all 7 canonical error types."""
    valid_count = 0
    observed_errors: set[str] = set()
    for _, _, raw in PARITY_CASES:
        res = validate_event(raw)
        if res.is_valid:
            valid_count += 1
        elif res.primary_error_type is not None:
            observed_errors.add(res.primary_error_type)

    assert valid_count == 4
    assert observed_errors == {
        "corrupt_json",
        "schema_version_unsupported",
        "missing_required",
        "bad_enum",
        "bad_timestamp",
        "malformed_decimal",
        "negative_amount",
    }


def _run_spark_parity_check(base_dir: Path) -> int:
    """Run all PARITY_CASES through Python validate_event and Spark validate_df."""
    from pyspark.sql.types import LongType, StringType, StructField, StructType

    spark = build_spark(
        "test_validation_parity",
        local_only=True,
        warehouse_dir=str(base_dir / "warehouse"),
    )
    try:
        schema = StructType(
            [
                StructField("kafka_offset", LongType(), False),
                StructField("payload_raw", StringType(), False),
                StructField("kafka_topic", StringType(), False),
            ]
        )
        rows = [(case_id, raw, "cpc_events_raw") for case_id, _, raw in PARITY_CASES]
        df = spark.createDataFrame(rows, schema=schema)

        valid_df, invalid_df = validate_df(df)
        valid_rows = {int(r["kafka_offset"]): r for r in valid_df.collect()}
        invalid_rows = {int(r["source_offset"]): r for r in invalid_df.collect()}

        assert len(valid_rows) + len(invalid_rows) == len(PARITY_CASES), (
            f"Row routing mismatch: valid={len(valid_rows)}, invalid={len(invalid_rows)}, "
            f"total={len(PARITY_CASES)}"
        )

        for case_id, label, raw in PARITY_CASES:
            py_res = validate_event(raw)
            if py_res.is_valid:
                assert case_id in valid_rows, (
                    f"[{case_id}:{label}] Python=VALID but Spark routed to DLQ: "
                    f"{invalid_rows.get(case_id)}"
                )
                assert case_id not in invalid_rows
                assert valid_rows[case_id]["event_date"] is not None
            else:
                assert case_id in invalid_rows, (
                    f"[{case_id}:{label}] Python=INVALID({py_res.primary_error_type}) "
                    f"but Spark routed to Bronze"
                )
                assert case_id not in valid_rows
                spark_err = invalid_rows[case_id]["error_type"]
                assert spark_err == py_res.primary_error_type, (
                    f"[{case_id}:{label}] error_type mismatch: "
                    f"Python={py_res.primary_error_type!r} vs Spark={spark_err!r}"
                )
                assert invalid_rows[case_id]["stage"] == "ingest"
                assert invalid_rows[case_id]["error_detail"]
    finally:
        spark.stop()

    return len(PARITY_CASES)


@_mark_spark
def test_python_and_spark_validation_parity(tmp_path: Path) -> None:
    """Verify 100% row-by-row parity between validate_event() and validate_df()."""
    assert pytest is not None
    pytest.importorskip("pyspark")
    _run_spark_parity_check(tmp_path)


if __name__ == "__main__":
    test_parity_corpus_covers_all_7_error_types()
    with tempfile.TemporaryDirectory() as tmp:
        checked = _run_spark_parity_check(Path(tmp))
    print(f"PASS: 100% Python <-> PySpark validation parity across {checked}/{checked} cases")
