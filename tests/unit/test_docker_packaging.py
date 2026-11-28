"""Offline packaging tests for Task 5.9 Dashboard & Alert Worker containers."""

from __future__ import annotations

from pathlib import Path

import yaml

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


def test_dashboard_dockerfile_prebakes_extensions_and_excludes_secrets() -> None:
    """Dockerfile uses python:3.11-slim, pre-installs DuckDB extensions, and excludes .env."""
    dockerfile = (_PROJECT_ROOT / "infra" / "dashboard" / "Dockerfile").read_text(encoding="utf-8")

    assert "FROM python:3.11-slim" in dockerfile
    assert "DUCKDB_EXTENSION_DIRECTORY=/opt/duckdb_extensions" in dockerfile
    assert "INSTALL httpfs" in dockerfile
    assert "INSTALL delta" in dockerfile
    assert "COPY src/ /app/src/" in dockerfile
    assert "COPY config/ /app/config/" in dockerfile
    assert "COPY dbt_cpc/seeds/ /app/dbt_cpc/seeds/" in dockerfile
    assert "COPY .env" not in dockerfile
    assert "USER appuser" in dockerfile
    assert "/_stcore/health" in dockerfile


def test_docker_compose_defines_dashboard_and_alert_worker_in_apps_profile() -> None:
    """docker-compose.yml pins RAM limits, apps profile, pre-baked extension dir, and healthchecks."""
    compose = yaml.safe_load((_PROJECT_ROOT / "docker-compose.yml").read_text(encoding="utf-8"))
    services = compose["services"]

    for svc_name, expected_mem in (("dashboard", "768m"), ("alert-worker", "384m")):
        svc = services[svc_name]
        assert svc["profiles"] == ["apps"]
        assert svc["mem_limit"] == expected_mem
        assert svc["depends_on"]["minio"]["condition"] == "service_healthy"
        assert svc["logging"] is not None
        env = svc["environment"]
        assert env["DUCKDB_EXTENSION_DIRECTORY"] == "/opt/duckdb_extensions"
        assert env["S3_ENDPOINT_HOST_PORT"] == "minio:9000"
        assert "http://minio:9000" in env["S3_ENDPOINT"]

    assert services["dashboard"]["ports"] == ["127.0.0.1:8501:8501"]
    assert "/_stcore/health" in " ".join(services["dashboard"]["healthcheck"]["test"])
    assert services["alert-worker"]["healthcheck"]["disable"] is True
