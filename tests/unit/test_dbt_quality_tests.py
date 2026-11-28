"""Offline unit & negative tests for dbt singular quality tests and Task 4.6 loop hygiene."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import duckdb

from scripts.write_gold_last_run import GOLD_MODELS, build_last_run_payload

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
TESTS_DIR = PROJECT_ROOT / "dbt_cpc" / "tests"
DBT_LOOP_SH = PROJECT_ROOT / "scripts" / "dbt_loop.sh"


def _render_singular_test_sql(filename: str) -> str:
    raw = (TESTS_DIR / filename).read_text(encoding="utf-8")
    stripped = re.sub(r"\{\{\s*config\(.*?\)\s*\}\}", "", raw, flags=re.DOTALL)
    stripped = re.sub(r"\{\{\s*ref\(['\"]([^'\"]+)['\"]\)\s*\}\}", r"\1", stripped)
    return re.sub(
        r"\{\{\s*source\(\s*['\"]lakehouse_silver['\"]\s*,\s*['\"]cpc_events['\"]\s*\)\s*\}\}",
        "raw_silver_cpc_events",
        stripped,
    )


def _setup_duckdb_quality_fixture() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(":memory:")
    con.execute("SET TimeZone = 'UTC'")
    con.execute(
        """
        CREATE TABLE raw_silver_cpc_events (
            event_id VARCHAR,
            event_time TIMESTAMP,
            produced_at TIMESTAMP,
            processed_at TIMESTAMP,
            event_date DATE,
            event_type VARCHAR,
            job_id VARCHAR,
            campaign_id VARCHAR,
            group_id VARCHAR,
            publisher_id VARCHAR,
            bid DECIMAL(18, 4),
            spend DECIMAL(18, 4),
            source_row_id BIGINT
        );
        CREATE TABLE stg_cpc_events (
            event_id VARCHAR,
            event_time TIMESTAMP,
            event_hour TIMESTAMP,
            produced_at TIMESTAMP,
            processed_at TIMESTAMP,
            event_date DATE,
            event_type VARCHAR,
            job_id VARCHAR,
            campaign_id VARCHAR,
            group_id VARCHAR,
            publisher_id VARCHAR,
            bid DECIMAL(18, 4),
            spend DECIMAL(18, 4),
            source_row_id BIGINT
        );
        CREATE TABLE fct_campaign_hourly_pacing (
            campaign_id VARCHAR,
            event_date DATE,
            hour_ts TIMESTAMP,
            spend_hour DECIMAL(18, 4),
            cum_spend DECIMAL(18, 4),
            ideal_cum_spend DECIMAL(18, 4),
            pacing_ratio DOUBLE
        );
        CREATE TABLE fct_publisher_quality (
            publisher_id VARCHAR,
            hour_ts TIMESTAMP,
            spend DECIMAL(18, 4)
        );
        CREATE TABLE fct_recruitment_efficiency (
            job_id VARCHAR,
            campaign_id VARCHAR,
            event_date DATE,
            spend DECIMAL(18, 4)
        );
        """
    )

    ts = datetime(2026, 1, 1, 5, 30, tzinfo=timezone.utc).replace(tzinfo=None)
    hr = ts.replace(minute=0, second=0, microsecond=0)
    d = ts.date()

    for eid, etype, spend in (
        ("e1", "click", Decimal("10.0000")),
        ("e2", "click", Decimal("10.0000")),
        ("e3", "apply", Decimal("0.0000")),
        ("e4", "qualified", Decimal("0.0000")),
    ):
        bid = spend if etype == "click" else None
        con.execute(
            "INSERT INTO raw_silver_cpc_events VALUES (?, ?, ?, ?, ?, ?, 'J1', 'C1', NULL, 'P1', ?, ?, 1)",
            [eid, ts, ts, ts, d, etype, bid, spend],
        )
        con.execute(
            "INSERT INTO stg_cpc_events VALUES (?, ?, ?, ?, ?, ?, ?, 'J1', 'C1', NULL, 'P1', ?, ?, 1)",
            [eid, ts, hr, ts, ts, d, etype, bid, spend],
        )

    con.execute(
        "INSERT INTO fct_campaign_hourly_pacing VALUES ('C1', ?, ?, 20.0000, 20.0000, 250.0000, 0.08)",
        [d, hr],
    )
    con.execute("INSERT INTO fct_publisher_quality VALUES ('P1', ?, 20.0000)", [hr])
    con.execute("INSERT INTO fct_recruitment_efficiency VALUES ('J1', 'C1', ?, 20.0000)", [d])
    return con


def test_assert_non_negative_spend_passes_clean_and_fails_on_negative_spend() -> None:
    con = _setup_duckdb_quality_fixture()
    sql = _render_singular_test_sql("assert_non_negative_spend.sql")

    assert con.execute(sql).fetchall() == []

    # Inject 1 negative spend row (-1.0000) -> singular test must catch exact 1 violating row
    ts = datetime(2026, 1, 1, 5, 35, tzinfo=timezone.utc).replace(tzinfo=None)
    con.execute(
        "INSERT INTO stg_cpc_events VALUES ('bad_neg', ?, ?, ?, ?, ?, 'click', 'J1', 'C1', NULL, 'P1', -1.0000, -1.0000, 99)",
        [ts, ts.replace(minute=0, second=0), ts, ts, ts.date()],
    )
    violations = con.execute(sql).fetchall()
    assert len(violations) == 1
    assert violations[0][0] == "stg_cpc_events"
    assert violations[0][1] == "bad_neg"
    assert violations[0][2] == Decimal("-1.0000")


def test_assert_non_negative_pacing_ratio_catches_negative_ratio() -> None:
    con = _setup_duckdb_quality_fixture()
    sql = _render_singular_test_sql("assert_non_negative_pacing_ratio.sql")
    assert con.execute(sql).fetchall() == []

    con.execute(
        "INSERT INTO fct_campaign_hourly_pacing VALUES ('C_BAD', DATE '2026-01-01', TIMESTAMP '2026-01-01 05:00:00', 0.0000, -5.0000, 100.0000, -0.05)"
    )
    violations = con.execute(sql).fetchall()
    assert len(violations) == 1
    assert violations[0][0] == "C_BAD"


def test_assert_hour_ts_in_sim_range_catches_out_of_range_hour() -> None:
    con = _setup_duckdb_quality_fixture()
    sql = _render_singular_test_sql("assert_hour_ts_in_sim_range.sql")
    assert con.execute(sql).fetchall() == []

    con.execute(
        "INSERT INTO fct_campaign_hourly_pacing VALUES ('C1', DATE '2026-01-01', TIMESTAMP '2026-01-01 23:00:00', 0.0000, 20.0000, 1000.0000, 0.02)"
    )
    violations = con.execute(sql).fetchall()
    assert len(violations) == 1
    assert violations[0][2] == datetime(2026, 1, 1, 23, 0)


def test_assert_campaign_funnel_monotonic_warn_severity_and_cumulative_grain() -> None:
    raw = (TESTS_DIR / "assert_campaign_funnel_monotonic_warn.sql").read_text(encoding="utf-8")
    assert "severity = 'warn'" in raw

    con = _setup_duckdb_quality_fixture()
    sql = _render_singular_test_sql("assert_campaign_funnel_monotonic_warn.sql")
    assert con.execute(sql).fetchall() == []

    # Campaign C_WARN has 0 clicks but 1 apply -> triggers 1 warn row
    ts = datetime(2026, 1, 1, 5, 40, tzinfo=timezone.utc).replace(tzinfo=None)
    con.execute(
        "INSERT INTO stg_cpc_events VALUES ('w1', ?, ?, ?, ?, ?, 'apply', 'J1', 'C_WARN', NULL, 'P1', NULL, 0.0000, 100)",
        [ts, ts.replace(minute=0, second=0), ts, ts, ts.date()],
    )
    violations = con.execute(sql).fetchall()
    assert len(violations) == 1
    assert violations[0][0] == "C_WARN"


def test_assert_stg_silver_event_id_reconcile() -> None:
    con = _setup_duckdb_quality_fixture()
    sql = _render_singular_test_sql("assert_stg_silver_event_id_reconcile.sql")
    assert con.execute(sql).fetchall() == []

    con.execute("DELETE FROM stg_cpc_events WHERE event_id = 'e4'")
    violations = con.execute(sql).fetchall()
    assert violations == [(3, 4)]


def test_write_gold_last_run_payload_and_dbt_loop_script_hygiene() -> None:
    payload = build_last_run_payload(
        status="success",
        duration_sec=8.4567,
        models_built=3,
        run_at=datetime(2026, 11, 28, 7, 30, 0, tzinfo=timezone.utc),
    )
    assert payload == {
        "last_run_utc": "2026-11-28T07:30:00Z",
        "duration_sec": 8.457,
        "status": "success",
        "models_built": 3,
        "gold_models": list(GOLD_MODELS),
    }

    raw_bytes = DBT_LOOP_SH.read_bytes()
    assert b"\r\n" not in raw_bytes, "scripts/dbt_loop.sh must use LF line endings"
    script_text = raw_bytes.decode("utf-8")
    assert "flock -n" in script_text
    assert "--once" in script_text
    assert "WSLENV" in script_text
    assert "scripts/write_gold_last_run.py" in script_text
