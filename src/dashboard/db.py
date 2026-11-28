"""Thread-safe DuckDB serving connection layer for Streamlit & Alert Worker (Task 5.1, ADR-004).

Uses an in-memory DuckDB root connection cached once via ``@st.cache_resource`` and
creates an isolated ``cursor()`` per query (closed in ``finally``) so concurrent
Streamlit sessions never share mutable cursor state or contend on ``.duckdb`` file locks.
Follows Rule R6: S3 credentials and ``CREATE SECRET`` SQL are never logged.
"""

from __future__ import annotations

import logging
import os
import re
import time
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd
import streamlit as st

from src.common.config import AppConfig, load

logger = logging.getLogger(__name__)

_TRANSIENT_ERROR_KEYWORDS: tuple[str, ...] = (
    "http",
    "connection",
    "timeout",
    "timed out",
    "503",
    "502",
    "500",
    "temporarily",
    "socket",
    "io error",
    "could not connect",
    "reset by peer",
    "broken pipe",
)


class DatabaseConnectionError(RuntimeError):
    """Structured connection/S3 failure raised after retries are exhausted."""

    def __init__(
        self,
        message: str = "Mất kết nối dữ liệu Lakehouse (MinIO/DuckDB). Vui lòng thử lại sau.",
        *,
        attempts: int = 1,
        cause: Exception | None = None,
    ) -> None:
        super().__init__(message)
        self.attempts = attempts
        self.cause = cause


def _strip_http_scheme(raw_endpoint: str) -> tuple[str, bool]:
    """Strip scheme for DuckDB S3 SECRET (Trap K1) and return ``(host_port, use_ssl)``."""
    cleaned = raw_endpoint.strip()
    use_ssl = cleaned.lower().startswith("https://")
    host_port = re.sub(r"^https?://", "", cleaned, flags=re.IGNORECASE).rstrip("/")
    return host_port, use_ssl


def resolve_duckdb_s3_endpoint(cfg: AppConfig) -> tuple[str, bool]:
    """Resolve DuckDB S3 endpoint ``host:port`` (without ``http://``) and ``use_ssl`` flag."""
    override = os.environ.get("S3_ENDPOINT_HOST_PORT", "").strip()
    if override:
        return _strip_http_scheme(override)
    if Path("/.dockerenv").exists():
        return cfg.s3_endpoint_host_port, cfg.s3_use_ssl
    return _strip_http_scheme(cfg.s3_endpoint_external)


def _load_extension(con: duckdb.DuckDBPyConnection, ext_name: str) -> None:
    """Load DuckDB extension, falling back to INSTALL+LOAD when not pre-baked."""
    try:
        con.execute(f"LOAD {ext_name}")
    except duckdb.Error:
        con.execute(f"INSTALL {ext_name}")
        con.execute(f"LOAD {ext_name}")


def _sql_quote(value: str) -> str:
    """Escape single quotes for SQL string literals."""
    return value.replace("'", "''")


def create_root_connection(
    cfg: AppConfig | None = None,
    *,
    extension_directory: str | None = None,
    configure_s3: bool = True,
) -> duckdb.DuckDBPyConnection:
    """Create and configure the in-memory DuckDB root connection for serving."""
    resolved_cfg = cfg or load()
    ext_dir = extension_directory or os.environ.get("DUCKDB_EXTENSION_DIRECTORY", "").strip()
    conn_cfg: dict[str, Any] = {}
    if ext_dir:
        conn_cfg["extension_directory"] = ext_dir

    con = duckdb.connect(":memory:", config=conn_cfg)
    con.execute("SET memory_limit = '256MB'")
    con.execute("SET threads = 2")
    con.execute("SET TimeZone = 'UTC'")

    _load_extension(con, "httpfs")
    _load_extension(con, "delta")

    if configure_s3:
        endpoint, use_ssl = resolve_duckdb_s3_endpoint(resolved_cfg)
        use_ssl_sql = "true" if use_ssl else "false"
        # Rule R6: Never log this SQL statement or S3 credentials.
        con.execute(
            f"""
            CREATE OR REPLACE SECRET lakehouse_s3 (
                TYPE S3,
                KEY_ID '{_sql_quote(resolved_cfg.s3_access_key)}',
                SECRET '{_sql_quote(resolved_cfg.s3_secret_key)}',
                ENDPOINT '{_sql_quote(endpoint)}',
                URL_STYLE 'path',
                USE_SSL {use_ssl_sql},
                REGION '{_sql_quote(resolved_cfg.s3_region)}'
            )
            """
        )
        logger.info(
            "Initialized DuckDB :memory: root connection (endpoint=%s, use_ssl=%s, memory_limit=256MB, threads=2)",
            endpoint,
            use_ssl,
        )
    else:
        logger.info(
            "Initialized DuckDB :memory: root connection (s3_secret=disabled, memory_limit=256MB, threads=2)"
        )
    return con


@st.cache_resource(show_spinner=False)
def get_root_connection() -> duckdb.DuckDBPyConnection:
    """Return process-wide cached DuckDB root connection for Streamlit threads."""
    return create_root_connection()


def is_transient_db_error(exc: Exception) -> bool:
    """Return True for transient S3/HTTP/network errors that warrant retry."""
    if isinstance(exc, (duckdb.IOException, duckdb.HTTPException, OSError, TimeoutError)):
        return True
    if isinstance(exc, duckdb.Error):
        msg = str(exc).lower()
        return any(token in msg for token in _TRANSIENT_ERROR_KEYWORDS)
    return False


def run_query(
    sql: str,
    params: Sequence[Any] | None = None,
    *,
    root_conn: duckdb.DuckDBPyConnection | None = None,
    max_attempts: int = 3,
    base_backoff_sec: float = 0.25,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> pd.DataFrame:
    """Execute parameterized SQL on an isolated DuckDB cursor with retry and latency logging."""
    conn = root_conn if root_conn is not None else get_root_connection()
    bound_params = list(params) if params is not None else []
    attempts = max(1, max_attempts)

    for attempt in range(1, attempts + 1):
        cur = conn.cursor()
        t0 = time.perf_counter()
        try:
            df = cur.execute(sql, bound_params).df()
            elapsed_ms = (time.perf_counter() - t0) * 1000.0
            logger.info("query_ms=%.1f rows=%d attempt=%d", elapsed_ms, len(df), attempt)
            return df
        except Exception as exc:
            elapsed_ms = (time.perf_counter() - t0) * 1000.0
            if not is_transient_db_error(exc):
                logger.error("query_failed_non_transient query_ms=%.1f error_type=%s", elapsed_ms, type(exc).__name__)
                raise

            logger.warning(
                "Transient DuckDB/S3 error on attempt %d/%d (query_ms=%.1f, error_type=%s)",
                attempt,
                attempts,
                elapsed_ms,
                type(exc).__name__,
            )
            if attempt >= attempts:
                raise DatabaseConnectionError(attempts=attempt, cause=exc) from exc

            delay = base_backoff_sec * (2 ** (attempt - 1))
            sleep_fn(delay)
        finally:
            cur.close()

    raise DatabaseConnectionError(attempts=attempts)
