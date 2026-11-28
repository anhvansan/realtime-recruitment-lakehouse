"""Offline unit tests for Gold dbt models and Metric Catalog Section 2.3 examples (Task 4.4)."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import duckdb
import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
GOLD_DIR = PROJECT_ROOT / "dbt_cpc" / "models" / "gold"


def _render_gold_sql_for_duckdb(model_name: str) -> str:
    """Strip dbt config block and resolve {{ ref('...') }} to local DuckDB table names."""
    raw_sql = (GOLD_DIR / f"{model_name}.sql").read_text(encoding="utf-8")
    stripped = re.sub(r"\{\{\s*config\(.*?\)\s*\}\}", "", raw_sql, flags=re.DOTALL)
    return re.sub(r"\{\{\s*ref\(['\"]([^'\"]+)['\"]\)\s*\}\}", r"\1", stripped)


def _create_empty_fixture_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("SET TimeZone = 'UTC'")
    con.execute(
        """
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
        CREATE TABLE dim_campaign_budget (
            campaign_id VARCHAR,
            campaign_name VARCHAR,
            company_id VARCHAR,
            pacing_unit VARCHAR,
            budget_total DECIMAL(18, 4),
            daily_budget DECIMAL(18, 4),
            is_active INTEGER,
            is_outlier_budget BOOLEAN,
            budget_source VARCHAR
        );
        CREATE TABLE dim_publisher (
            publisher_id VARCHAR,
            publisher_name VARCHAR,
            publisher_type INTEGER,
            min_bid DECIMAL(18, 4),
            max_bid DECIMAL(18, 4),
            cpc_increment INTEGER,
            is_active INTEGER
        );
        CREATE TABLE dim_job (
            job_id VARCHAR,
            job_title VARCHAR,
            company_id VARCHAR,
            group_id VARCHAR,
            campaign_id VARCHAR,
            work_schedule VARCHAR,
            role_location VARCHAR,
            job_budget DECIMAL(18, 4),
            status INTEGER,
            is_active INTEGER
        );
        CREATE TABLE dim_company (
            company_id VARCHAR,
            company_name VARCHAR,
            billing_id VARCHAR,
            customer_type INTEGER,
            status INTEGER,
            is_active INTEGER
        );
        """
    )


def _insert_event(
    con: duckdb.DuckDBPyConnection,
    *,
    event_id: str,
    ts: datetime,
    event_type: str,
    job_id: str,
    campaign_id: str,
    publisher_id: str,
    spend: str,
    row_id: int,
) -> None:
    naive_utc = ts.astimezone(timezone.utc).replace(tzinfo=None)
    hour_utc = naive_utc.replace(minute=0, second=0, microsecond=0)
    date_utc = naive_utc.date()
    bid = spend if event_type == "click" else "0.0000"
    con.execute(
        "INSERT INTO stg_cpc_events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, ?)",
        [
            event_id,
            naive_utc,
            hour_utc,
            naive_utc,
            naive_utc,
            date_utc,
            event_type,
            job_id,
            campaign_id,
            publisher_id,
            Decimal(bid),
            Decimal(spend),
            row_id,
        ],
    )


def test_gold_models_have_no_select_star_or_removed_metrics() -> None:
    for model_name in (
        "fct_campaign_hourly_pacing",
        "fct_publisher_quality",
        "fct_recruitment_efficiency",
    ):
        sql_text = (GOLD_DIR / f"{model_name}.sql").read_text(encoding="utf-8")
        assert re.search(r"select\s+\*", sql_text, flags=re.IGNORECASE) is None
        assert "impression" not in sql_text.lower()
        assert "ctr" not in sql_text.lower()
        assert "materialized='external'" in sql_text
        assert "format='parquet'" in sql_text
        assert f"/gold/{model_name}.parquet" in sql_text


def test_fct_campaign_hourly_pacing_section_2_3_closed_and_in_progress_hours() -> None:
    con = duckdb.connect(":memory:")
    _create_empty_fixture_tables(con)

    con.execute(
        "INSERT INTO dim_campaign_budget VALUES ('C1', 'Campaign C1', 'COMP1', 'Daily', 1000.0000, 1000.0000, 1, false, 'campaign.csv')"
    )

    # Hours 00..04 (skip hour 02 to verify continuous hour spine!) summing to 280.0000
    _insert_event(
        con,
        event_id="e0",
        ts=datetime(2026, 1, 1, 0, 30, tzinfo=timezone.utc),
        event_type="click",
        job_id="J1",
        campaign_id="C1",
        publisher_id="P1",
        spend="100.0000",
        row_id=1,
    )
    _insert_event(
        con,
        event_id="e1",
        ts=datetime(2026, 1, 1, 1, 30, tzinfo=timezone.utc),
        event_type="click",
        job_id="J1",
        campaign_id="C1",
        publisher_id="P1",
        spend="80.0000",
        row_id=2,
    )
    _insert_event(
        con,
        event_id="e4",
        ts=datetime(2026, 1, 1, 4, 15, tzinfo=timezone.utc),
        event_type="click",
        job_id="J1",
        campaign_id="C1",
        publisher_id="P1",
        spend="100.0000",
        row_id=3,
    )
    # Closed hour 05: spend = 120.0000 -> cum_spend = 400.0000
    _insert_event(
        con,
        event_id="e5",
        ts=datetime(2026, 1, 1, 5, 45, tzinfo=timezone.utc),
        event_type="click",
        job_id="J1",
        campaign_id="C1",
        publisher_id="P1",
        spend="120.0000",
        row_id=4,
    )
    # In-progress hour 10: sim_now = 10:15:00 UTC -> elapsed = 36900 / 86400 -> ideal = 427.0833
    _insert_event(
        con,
        event_id="e10",
        ts=datetime(2026, 1, 1, 10, 15, tzinfo=timezone.utc),
        event_type="click",
        job_id="J1",
        campaign_id="C1",
        publisher_id="P1",
        spend="50.0000",
        row_id=5,
    )

    sql = _render_gold_sql_for_duckdb("fct_campaign_hourly_pacing")
    rows = con.execute(
        f"""
        SELECT
            hour_ts,
            spend_hour,
            cum_spend,
            ideal_cum_spend,
            pacing_ratio,
            budget_utilization,
            burn_rate,
            projected_eod_spend
        FROM ({sql})
        ORDER BY hour_ts
        """
    ).fetchall()

    # Hour spine covers 00..10 inclusive = 11 rows (including empty hours 02, 03, 06..09)
    assert len(rows) == 11
    by_hour = {r[0].hour: r for r in rows}

    # Empty hour 02 carries cum_spend forward without gap
    assert by_hour[2][1] == Decimal("0.0000")
    assert by_hour[2][2] == Decimal("180.0000")

    # Section 2.3 Case 1: closed hour 05
    h5 = by_hour[5]
    assert h5[2] == Decimal("400.0000")
    assert h5[3] == Decimal("250.0000")
    assert h5[4] == pytest.approx(1.6, rel=1e-6)
    assert h5[5] == pytest.approx(0.40, rel=1e-6)
    assert h5[6] == Decimal("120.0000")
    assert h5[7] == Decimal("2560.0000")

    # Section 2.3 Case 2: in-progress hour 10 at sim_now = 10:15:00 UTC
    h10 = by_hour[10]
    assert h10[3] == Decimal("427.0833")


def test_fct_publisher_quality_fraud_flags_and_zero_denominator() -> None:
    con = duckdb.connect(":memory:")
    _create_empty_fixture_tables(con)

    con.execute(
        """
        INSERT INTO dim_publisher VALUES
            ('P9', 'Pub 9', 1, 1.0, 5.0, 1, 1),
            ('P_BOUND', 'Pub Bound', 1, 1.0, 5.0, 1, 1),
            ('P_LOW', 'Pub Low CVR', 1, 1.0, 5.0, 1, 1),
            ('P_GOOD', 'Pub Good', 1, 1.0, 5.0, 1, 1),
            ('P_GOOD2', 'Pub Good 2', 1, 1.0, 5.0, 1, 1),
            ('P_GOOD3', 'Pub Good 3', 1, 1.0, 5.0, 1, 1),
            ('P_ZERO', 'Pub Zero Click', 1, 1.0, 5.0, 1, 1)
        """
    )

    ts = datetime(2026, 1, 1, 8, 10, tzinfo=timezone.utc)
    rid = 1
    # P9: 130 clicks, 0 applies -> R1 = True
    for i in range(130):
        _insert_event(
            con,
            event_id=f"p9_c_{i}",
            ts=ts,
            event_type="click",
            job_id="J1",
            campaign_id="C1",
            publisher_id="P9",
            spend="1.0000",
            row_id=rid,
        )
        rid += 1

    # P_BOUND: 99 clicks, 0 applies -> R1 = False (boundary check from Section 2.3)
    for i in range(99):
        _insert_event(
            con,
            event_id=f"pb_c_{i}",
            ts=ts,
            event_type="click",
            job_id="J1",
            campaign_id="C1",
            publisher_id="P_BOUND",
            spend="1.0000",
            row_id=rid,
        )
        rid += 1

    # P_GOOD: 50 clicks, 25 applies -> CVR = 0.50 (raises hourly median)
    for i in range(50):
        _insert_event(
            con,
            event_id=f"pg_c_{i}",
            ts=ts,
            event_type="click",
            job_id="J1",
            campaign_id="C1",
            publisher_id="P_GOOD",
            spend="1.0000",
            row_id=rid,
        )
        rid += 1
    for i in range(25):
        _insert_event(
            con,
            event_id=f"pg_a_{i}",
            ts=ts,
            event_type="apply",
            job_id="J1",
            campaign_id="C1",
            publisher_id="P_GOOD",
            spend="0.0000",
            row_id=rid,
        )
        rid += 1

    # P_GOOD2 & P_GOOD3: healthy peers (CVR = 0.30 and 0.40) so hourly median ≈ 0.1583
    for pub_id, n_applies in (("P_GOOD2", 15), ("P_GOOD3", 20)):
        for i in range(50):
            _insert_event(
                con,
                event_id=f"{pub_id}_c_{i}",
                ts=ts,
                event_type="click",
                job_id="J1",
                campaign_id="C1",
                publisher_id=pub_id,
                spend="1.0000",
                row_id=rid,
            )
            rid += 1
        for i in range(n_applies):
            _insert_event(
                con,
                event_id=f"{pub_id}_a_{i}",
                ts=ts,
                event_type="apply",
                job_id="J1",
                campaign_id="C1",
                publisher_id=pub_id,
                spend="0.0000",
                row_id=rid,
            )
            rid += 1

    # P_LOW: 60 clicks, 1 apply -> CVR = 1/60 ≈ 0.0167 < 0.2 * median -> R2 = True
    for i in range(60):
        _insert_event(
            con,
            event_id=f"pl_c_{i}",
            ts=ts,
            event_type="click",
            job_id="J1",
            campaign_id="C1",
            publisher_id="P_LOW",
            spend="1.0000",
            row_id=rid,
        )
        rid += 1
    _insert_event(
        con,
        event_id="pl_a_0",
        ts=ts,
        event_type="apply",
        job_id="J1",
        campaign_id="C1",
        publisher_id="P_LOW",
        spend="0.0000",
        row_id=rid,
    )
    rid += 1

    # P_ZERO: 0 clicks, 2 applies -> click_to_apply_rate IS NULL
    for i in range(2):
        _insert_event(
            con,
            event_id=f"pz_a_{i}",
            ts=ts,
            event_type="apply",
            job_id="J1",
            campaign_id="C1",
            publisher_id="P_ZERO",
            spend="0.0000",
            row_id=rid,
        )
        rid += 1

    sql = _render_gold_sql_for_duckdb("fct_publisher_quality")
    rows = {
        r[0]: r
        for r in con.execute(
            f"""
            SELECT publisher_id, clicks, applies, click_to_apply_rate, fraud_flag_r1, fraud_flag_r2
            FROM ({sql})
            """
        ).fetchall()
    }

    assert rows["P9"][4] is True
    assert rows["P_BOUND"][4] is False
    assert rows["P_LOW"][5] is True
    assert rows["P_GOOD"][5] is False
    assert rows["P_ZERO"][3] is None


def test_fct_recruitment_efficiency_metrics_and_nullif_zero() -> None:
    con = duckdb.connect(":memory:")
    _create_empty_fixture_tables(con)

    con.execute(
        "INSERT INTO dim_company VALUES ('COMP1', 'HireBoost Co', NULL, 1, 1, 1)"
    )
    con.execute(
        "INSERT INTO dim_campaign_budget VALUES ('C1', 'Tech Campaign', 'COMP1', 'Daily', 1000.0, 1000.0, 1, false, 'campaign.csv')"
    )
    con.execute(
        """
        INSERT INTO dim_job VALUES
            ('J1', 'Data Engineer', 'COMP1', 'G1', 'C1', 'FULL_TIME', 'REMOTE', 500.0, 1, 1),
            ('J2', 'Zero Denominator Job', 'COMP1', 'G1', 'C1', 'FULL_TIME', 'REMOTE', 500.0, 1, 1)
        """
    )

    ts = datetime(2026, 1, 1, 9, 0, tzinfo=timezone.utc)
    rid = 1
    # J1: 4 clicks * 5.0 = 20.0 spend; 2 applies; 1 qualified; 1 unqualified
    for i in range(4):
        _insert_event(
            con,
            event_id=f"j1_c_{i}",
            ts=ts,
            event_type="click",
            job_id="J1",
            campaign_id="C1",
            publisher_id="P1",
            spend="5.0000",
            row_id=rid,
        )
        rid += 1
    for i in range(2):
        _insert_event(
            con,
            event_id=f"j1_a_{i}",
            ts=ts,
            event_type="apply",
            job_id="J1",
            campaign_id="C1",
            publisher_id="P1",
            spend="0.0000",
            row_id=rid,
        )
        rid += 1
    _insert_event(
        con,
        event_id="j1_q_0",
        ts=ts,
        event_type="qualified",
        job_id="J1",
        campaign_id="C1",
        publisher_id="P1",
        spend="0.0000",
        row_id=rid,
    )
    rid += 1
    _insert_event(
        con,
        event_id="j1_u_0",
        ts=ts,
        event_type="unqualified",
        job_id="J1",
        campaign_id="C1",
        publisher_id="P1",
        spend="0.0000",
        row_id=rid,
    )
    rid += 1

    # J2: only 1 unqualified event -> clicks=0, applies=0, qualified=0 -> all ratios NULL
    _insert_event(
        con,
        event_id="j2_u_0",
        ts=ts,
        event_type="unqualified",
        job_id="J2",
        campaign_id="C1",
        publisher_id="P1",
        spend="0.0000",
        row_id=rid,
    )

    sql = _render_gold_sql_for_duckdb("fct_recruitment_efficiency")
    rows = {
        r[0]: r
        for r in con.execute(
            f"""
            SELECT
                job_id,
                job_title,
                campaign_name,
                company_name,
                spend,
                clicks,
                applies,
                qualified,
                unqualified,
                cpc,
                cpa,
                cpqa,
                qualified_rate,
                click_to_apply_rate
            FROM ({sql})
            """
        ).fetchall()
    }

    j1 = rows["J1"]
    assert j1[1] == "Data Engineer"
    assert j1[2] == "Tech Campaign"
    assert j1[3] == "HireBoost Co"
    assert j1[4] == Decimal("20.0000")
    assert j1[5:9] == (4, 2, 1, 1)
    assert j1[9] == Decimal("5.0000")  # cpc
    assert j1[10] == Decimal("10.0000")  # cpa
    assert j1[11] == Decimal("20.0000")  # cpqa
    assert j1[12] == pytest.approx(0.5)  # qualified_rate
    assert j1[13] == pytest.approx(0.5)  # click_to_apply_rate

    j2 = rows["J2"]
    assert j2[9:] == (None, None, None, None, None)
