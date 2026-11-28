"""Offline unit tests for dbt_cpc configuration and staging model contracts (Tasks 4.1, 4.3)."""

from __future__ import annotations

import re
from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DBT_DIR = PROJECT_ROOT / "dbt_cpc"


def test_dbt_profiles_enforces_env_vars_and_k1_endpoint_rule() -> None:
    profiles_path = DBT_DIR / "profiles.yml"
    raw_text = profiles_path.read_text(encoding="utf-8")

    # Rule R6: No hard-coded credentials
    assert "minioadmin" not in raw_text.lower()
    assert "env_var('S3_ACCESS_KEY')" in raw_text
    assert "env_var('S3_SECRET_KEY')" in raw_text

    parsed = yaml.safe_load(raw_text)
    dev = parsed["dbt_cpc"]["outputs"]["dev"]
    assert dev["type"] == "duckdb"
    assert dev["path"] == ":memory:"
    assert {"httpfs", "delta"}.issubset(set(dev["extensions"]))
    assert dev["settings"]["TimeZone"] == "UTC"

    s3_secret = dev["secrets"][0]
    assert s3_secret["type"] == "s3"
    assert s3_secret["url_style"] == "path"
    assert s3_secret["use_ssl"] is False
    # Trap K1: Must strip http:// and https:// from endpoint
    assert "replace('http://', '')" in s3_secret["endpoint"]
    assert "replace('https://', '')" in s3_secret["endpoint"]


def test_dbt_sources_points_to_silver_delta_scan() -> None:
    sources_path = DBT_DIR / "models" / "staging" / "sources.yml"
    parsed = yaml.safe_load(sources_path.read_text(encoding="utf-8"))
    source = parsed["sources"][0]
    assert source["name"] == "lakehouse_silver"
    table = source["tables"][0]
    assert table["name"] == "cpc_events"
    ext_loc = table["config"]["external_location"]
    assert "delta_scan('s3://" in ext_loc
    assert "/silver/cpc_events')" in ext_loc


def test_stg_cpc_events_sql_explicit_columns_and_no_select_star() -> None:
    sql_path = DBT_DIR / "models" / "staging" / "stg_cpc_events.sql"
    sql_text = sql_path.read_text(encoding="utf-8")

    assert re.search(r"select\s+\*", sql_text, flags=re.IGNORECASE) is None
    assert "materialized='view'" in sql_text
    assert "source('lakehouse_silver', 'cpc_events')" in sql_text
    assert "date_trunc('hour', event_time) as event_hour" in sql_text

    expected_cols = (
        "event_id",
        "event_time",
        "event_hour",
        "produced_at",
        "processed_at",
        "event_date",
        "event_type",
        "job_id",
        "campaign_id",
        "group_id",
        "publisher_id",
        "bid",
        "spend",
        "source_row_id",
    )
    for col in expected_cols:
        assert col in sql_text, f"Missing expected column {col} in stg_cpc_events.sql"


def test_stg_cpc_events_schema_tests_declared() -> None:
    schema_path = DBT_DIR / "models" / "staging" / "schema.yml"
    parsed = yaml.safe_load(schema_path.read_text(encoding="utf-8"))
    model = parsed["models"][0]
    assert model["name"] == "stg_cpc_events"

    cols = {c["name"]: c for c in model["columns"]}
    assert "not_null" in cols["event_id"]["data_tests"]
    assert "unique" in cols["event_id"]["data_tests"]

    for req_col in ("event_time", "event_type", "campaign_id", "job_id", "publisher_id", "spend"):
        assert "not_null" in cols[req_col]["data_tests"], f"{req_col} missing not_null test"

    accepted = next(
        t["accepted_values"]
        for t in cols["event_type"]["data_tests"]
        if isinstance(t, dict) and "accepted_values" in t
    )
    assert accepted["arguments"]["values"] == ["click", "apply", "qualified", "unqualified"]

    expected_rels = {
        "campaign_id": ("dim_campaign_budget", "campaign_id"),
        "job_id": ("dim_job", "job_id"),
        "publisher_id": ("dim_publisher", "publisher_id"),
    }
    for col_name, (target_model, target_field) in expected_rels.items():
        rel = next(
            t["relationships"]
            for t in cols[col_name]["data_tests"]
            if isinstance(t, dict) and "relationships" in t
        )
        assert f"ref('{target_model}')" in rel["arguments"]["to"]
        assert rel["arguments"]["field"] == target_field


def test_dimension_models_no_select_star_and_schema_primary_keys() -> None:
    dims_dir = DBT_DIR / "models" / "dimensions"
    expected_dims = {
        "dim_campaign_budget": "campaign_id",
        "dim_job": "job_id",
        "dim_publisher": "publisher_id",
        "dim_company": "company_id",
        "dim_group": "group_id",
    }

    for model_name in expected_dims:
        sql_text = (dims_dir / f"{model_name}.sql").read_text(encoding="utf-8")
        assert re.search(r"select\s+\*", sql_text, flags=re.IGNORECASE) is None
        assert "materialized='view'" in sql_text

    parsed = yaml.safe_load((dims_dir / "schema.yml").read_text(encoding="utf-8"))
    models = {m["name"]: m for m in parsed["models"]}
    assert set(expected_dims).issubset(set(models))
    for model_name, pk in expected_dims.items():
        cols = {c["name"]: c for c in models[model_name]["columns"]}
        assert "not_null" in cols[pk]["data_tests"]
        assert "unique" in cols[pk]["data_tests"]
