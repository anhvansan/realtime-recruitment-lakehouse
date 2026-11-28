"""Unit tests for DuckDB dashboard connection layer (Task 5.1, ADR-004, Rules R6/K1)."""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd
import pyarrow as pa
import pytest
from deltalake import write_deltalake

from src.common.config import AppConfig
from src.dashboard.db import (
    DatabaseConnectionError,
    create_root_connection,
    resolve_duckdb_s3_endpoint,
    run_query,
)


def _write_local_delta_fixture(delta_dir: Path) -> Path:
    """Write a small partitioned Delta Lake table locally using deltalake (no Spark)."""
    table = pa.table(
        {
            "event_id": pa.array(["e1", "e2", "e3"], type=pa.string()),
            "campaign_id": pa.array(["C1", "C1", "C2"], type=pa.string()),
            "spend": pa.array(
                [Decimal("1.5000"), Decimal("2.2500"), Decimal("4.0000")],
                type=pa.decimal128(18, 4),
            ),
            "event_date": pa.array(
                [date(2026, 1, 1), date(2026, 1, 1), date(2026, 1, 2)],
                type=pa.date32(),
            ),
        }
    )
    write_deltalake(str(delta_dir), table, partition_by=["event_date"])
    return delta_dir


def test_run_query_reads_local_delta_table_created_by_deltalake(tmp_path: Path) -> None:
    delta_dir = _write_local_delta_fixture(tmp_path / "silver_cpc_events")
    con = create_root_connection(configure_s3=False)
    try:
        df = run_query(
            f"""
            SELECT campaign_id, SUM(spend) AS total_spend
            FROM delta_scan('{delta_dir.as_posix()}')
            WHERE event_date = ?
            GROUP BY 1
            ORDER BY 1
            """,
            [date(2026, 1, 1)],
            root_conn=con,
        )
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 1
        assert df.iloc[0]["campaign_id"] == "C1"
        assert Decimal(str(df.iloc[0]["total_spend"])) == Decimal("3.7500")
    finally:
        con.close()


def test_run_query_five_concurrent_threads_no_lock_or_race(tmp_path: Path) -> None:
    delta_dir = _write_local_delta_fixture(tmp_path / "silver_concurrent")
    con = create_root_connection(configure_s3=False)
    sql = f"SELECT COUNT(*) AS cnt, SUM(spend) AS total_spend FROM delta_scan('{delta_dir.as_posix()}')"

    def _worker(_idx: int) -> tuple[int, Decimal]:
        df = run_query(sql, root_conn=con)
        return int(df.iloc[0]["cnt"]), Decimal(str(df.iloc[0]["total_spend"]))

    try:
        with ThreadPoolExecutor(max_workers=5) as pool:
            results = list(pool.map(_worker, range(25)))

        assert len(results) == 25
        for cnt, total_spend in results:
            assert cnt == 3
            assert total_spend == Decimal("7.7500")
    finally:
        con.close()


def test_run_query_retries_transient_error_with_backoff_and_raises_structured_error() -> None:
    class _FakeResult:
        def df(self) -> pd.DataFrame:
            return pd.DataFrame({"ok": [1]})

    class _FakeCursor:
        def __init__(self, state: dict[str, Any], fail_times: int) -> None:
            self._state = state
            self._fail_times = fail_times
            self.closed = False

        def execute(self, _sql: str, _params: list[Any]) -> _FakeResult:
            self._state["calls"] += 1
            if self._state["calls"] <= self._fail_times:
                raise duckdb.IOException("HTTP 503 Service Temporarily Unavailable")
            return _FakeResult()

        def close(self) -> None:
            self.closed = True

    class _FakeConn:
        def __init__(self, fail_times: int) -> None:
            self.state: dict[str, Any] = {"calls": 0}
            self.fail_times = fail_times
            self.cursors: list[_FakeCursor] = []

        def cursor(self) -> _FakeCursor:
            cur = _FakeCursor(self.state, self.fail_times)
            self.cursors.append(cur)
            return cur

    # Succeeds on 3rd attempt after 2 transient errors
    sleeps: list[float] = []
    conn_recover = _FakeConn(fail_times=2)
    df = run_query(
        "SELECT 1 AS ok",
        root_conn=conn_recover,  # type: ignore[arg-type]
        max_attempts=3,
        base_backoff_sec=0.1,
        sleep_fn=sleeps.append,
    )
    assert int(df.iloc[0]["ok"]) == 1
    assert sleeps == [pytest.approx(0.1), pytest.approx(0.2)]
    assert all(c.closed for c in conn_recover.cursors)

    # Exhausts all 3 attempts -> raises structured DatabaseConnectionError
    sleeps_fail: list[float] = []
    conn_always_fail = _FakeConn(fail_times=10)
    with pytest.raises(DatabaseConnectionError) as exc_info:
        run_query(
            "SELECT 1 AS ok",
            root_conn=conn_always_fail,  # type: ignore[arg-type]
            max_attempts=3,
            base_backoff_sec=0.1,
            sleep_fn=sleeps_fail.append,
        )
    assert exc_info.value.attempts == 3
    assert isinstance(exc_info.value.cause, duckdb.IOException)
    assert sleeps_fail == [pytest.approx(0.1), pytest.approx(0.2)]
    assert all(c.closed for c in conn_always_fail.cursors)


def test_no_s3_credentials_leaked_in_logs(
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("S3_ENDPOINT_HOST_PORT", raising=False)
    sentinel_key = "SENTINEL_ACCESS_KEY_98765"
    sentinel_secret = "SENTINEL_SECRET_VALUE_43210"
    cfg = AppConfig(
        _env_file=None,  # type: ignore[call-arg]
        S3_ACCESS_KEY=sentinel_key,
        S3_SECRET_KEY=sentinel_secret,
        S3_ENDPOINT_EXTERNAL="http://localhost:9000",
        S3_REGION="us-east-1",
    )

    with caplog.at_level(logging.DEBUG):
        con = create_root_connection(cfg=cfg, configure_s3=True)
        try:
            df = run_query("SELECT 42 AS answer", root_conn=con)
            assert int(df.iloc[0]["answer"]) == 42
        finally:
            con.close()

    assert sentinel_key not in caplog.text
    assert sentinel_secret not in caplog.text
    assert "CREATE OR REPLACE SECRET" not in caplog.text
    assert "query_ms=" in caplog.text


def test_resolve_duckdb_s3_endpoint_strips_scheme_k1(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("S3_ENDPOINT_HOST_PORT", raising=False)
    cfg_http = AppConfig(
        _env_file=None,  # type: ignore[call-arg]
        S3_ENDPOINT_EXTERNAL="http://localhost:9000/",
    )
    host_port, use_ssl = resolve_duckdb_s3_endpoint(cfg_http)
    assert host_port == "localhost:9000"
    assert use_ssl is False

    monkeypatch.setenv("S3_ENDPOINT_HOST_PORT", "https://minio.internal:9443/")
    host_port_ssl, use_ssl_true = resolve_duckdb_s3_endpoint(cfg_http)
    assert host_port_ssl == "minio.internal:9443"
    assert use_ssl_true is True
