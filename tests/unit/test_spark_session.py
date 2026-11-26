"""Unit and Spark integration tests for src/streaming/spark_session.py."""

from __future__ import annotations

import io
import logging
import tempfile
from collections.abc import Callable
from decimal import Decimal
from pathlib import Path
from typing import Any, TypeVar

from src.common.config import AppConfig, load
from src.streaming.spark_session import (
    DELTA_CATALOG,
    DELTA_EXTENSION,
    ROCKSDB_STATE_STORE_PROVIDER,
    _apply_hadoop_conf,
    build_spark,
    build_spark_conf,
    redact_spark_conf,
)

try:
    import pytest
except ImportError:  # pragma: no cover - allows standalone run inside spark container
    pytest = None  # type: ignore[assignment]

_F = TypeVar("_F", bound=Callable[..., Any])


def _mark_spark(func: _F) -> _F:
    if pytest is not None:
        return pytest.mark.spark(func)  # type: ignore[return-value]
    return func


def test_build_spark_conf_local_only() -> None:
    """Verify local_only=True sets Delta/UTC/16GB defaults without S3A settings."""
    cfg = load()
    conf = build_spark_conf("unit_local_only", cfg=cfg, local_only=True)

    assert conf["spark.app.name"] == "unit_local_only"
    assert conf["spark.master"] == "local[2]"
    assert conf["spark.driver.memory"] == "1536m"
    assert conf["spark.sql.shuffle.partitions"] == "4"
    assert conf["spark.sql.session.timeZone"] == "UTC"
    assert conf["spark.sql.warehouse.dir"] == "/tmp/spark-warehouse"
    assert conf["spark.sql.extensions"] == DELTA_EXTENSION
    assert conf["spark.sql.catalog.spark_catalog"] == DELTA_CATALOG
    assert "spark.sql.streaming.stateStore.providerClass" not in conf
    assert not any(key.startswith("spark.hadoop.fs.s3a.") for key in conf)


def test_build_spark_conf_with_s3a_and_rocksdb() -> None:
    """Verify local_only=False populates S3A settings and enable_rocksdb=True sets RocksDB."""
    cfg = AppConfig(
        S3_ENDPOINT="http://minio:9000",
        S3_ACCESS_KEY="test-access-key-value",
        S3_SECRET_KEY="test-secret-key-value",
    )
    conf = build_spark_conf(
        "unit_s3a_rocksdb",
        cfg=cfg,
        local_only=False,
        enable_rocksdb=True,
    )

    assert conf["spark.hadoop.fs.s3a.impl"] == "org.apache.hadoop.fs.s3a.S3AFileSystem"
    assert conf["spark.hadoop.fs.s3a.endpoint"] == "http://minio:9000"
    assert conf["spark.hadoop.fs.s3a.access.key"] == "test-access-key-value"
    assert conf["spark.hadoop.fs.s3a.secret.key"] == "test-secret-key-value"
    assert conf["spark.hadoop.fs.s3a.path.style.access"] == "true"
    assert conf["spark.hadoop.fs.s3a.connection.ssl.enabled"] == "false"
    assert (
        conf["spark.sql.streaming.stateStore.providerClass"]
        == ROCKSDB_STATE_STORE_PROVIDER
    )


def test_redact_spark_conf_hides_credentials() -> None:
    """Ensure Rule R6: S3 access/secret keys are always masked before logging."""
    cfg = AppConfig(
        S3_ACCESS_KEY="my-super-access-key",
        S3_SECRET_KEY="my-super-secret-key",
    )
    conf = build_spark_conf("unit_redact", cfg=cfg, local_only=False)
    redacted = redact_spark_conf(conf)

    assert redacted["spark.hadoop.fs.s3a.access.key"] == "***REDACTED***"
    assert redacted["spark.hadoop.fs.s3a.secret.key"] == "***REDACTED***"
    assert redacted["spark.hadoop.fs.s3a.endpoint"] == cfg.s3_endpoint
    serialized = str(redacted)
    assert "my-super-access-key" not in serialized
    assert "my-super-secret-key" not in serialized


def test_apply_hadoop_conf_sets_s3a_keys_on_existing_session() -> None:
    """Verify _apply_hadoop_conf strips 'spark.hadoop.' and sets S3A keys on active sessions."""

    class _DummyHadoopConf:
        def __init__(self) -> None:
            self.values: dict[str, str] = {}

        def set(self, key: str, value: str) -> None:
            self.values[key] = value

    class _DummyJsc:
        def __init__(self) -> None:
            self.conf = _DummyHadoopConf()

        def hadoopConfiguration(self) -> _DummyHadoopConf:
            return self.conf

    class _DummySpark:
        def __init__(self) -> None:
            self._jsc = _DummyJsc()

    cfg = AppConfig(
        S3_ENDPOINT="http://minio:9000",
        S3_ACCESS_KEY="dummy-key",
        S3_SECRET_KEY="dummy-secret",
    )
    conf = build_spark_conf("unit_existing_session", cfg=cfg, local_only=False)
    dummy_spark: Any = _DummySpark()
    _apply_hadoop_conf(dummy_spark, conf)

    applied = dummy_spark._jsc.conf.values
    assert applied["fs.s3a.endpoint"] == "http://minio:9000"
    assert applied["fs.s3a.access.key"] == "dummy-key"
    assert applied["fs.s3a.secret.key"] == "dummy-secret"
    assert applied["fs.s3a.path.style.access"] == "true"
    assert applied["fs.s3a.connection.ssl.enabled"] == "false"
    assert "spark.master" not in applied
    assert "spark.sql.shuffle.partitions" not in applied


def _run_local_delta_roundtrip(base_dir: Path) -> None:
    """Execute a real local-only SparkSession Delta write/read roundtrip."""
    from pyspark.sql.types import DecimalType, IntegerType, StringType, StructField, StructType

    delta_path = str(base_dir / "delta_local_test")
    warehouse_path = str(base_dir / "spark_warehouse")

    spark = build_spark(
        "test_spark_session_local",
        local_only=True,
        warehouse_dir=warehouse_path,
    )
    try:
        assert spark.conf.get("spark.sql.shuffle.partitions") == "4"
        assert spark.conf.get("spark.sql.session.timeZone") == "UTC"

        schema = StructType(
            [
                StructField("event_id", IntegerType(), nullable=False),
                StructField("spend", DecimalType(18, 4), nullable=False),
                StructField("status", StringType(), nullable=False),
            ]
        )
        rows = [
            (1, Decimal("12.5000"), "ok"),
            (2, Decimal("3.2500"), "ok"),
        ]
        df = spark.createDataFrame(rows, schema=schema)
        df.write.format("delta").mode("overwrite").save(delta_path)

        read_back = spark.read.format("delta").load(delta_path).orderBy("event_id").collect()
        assert len(read_back) == 2
        assert read_back[0]["event_id"] == 1
        assert read_back[0]["spend"] == Decimal("12.5000")
        assert read_back[1]["spend"] == Decimal("3.2500")
    finally:
        spark.stop()


@_mark_spark
def test_build_spark_local_delta_roundtrip(tmp_path: Path, caplog: Any) -> None:
    """Verify build_spark(local_only=True) writes/reads local Delta and logs no credentials."""
    assert pytest is not None
    pytest.importorskip("pyspark")

    cfg = load()
    with caplog.at_level(logging.INFO, logger="src.streaming.spark_session"):
        _run_local_delta_roundtrip(tmp_path)

    if cfg.s3_access_key:
        assert cfg.s3_access_key not in caplog.text
    if cfg.s3_secret_key:
        assert cfg.s3_secret_key not in caplog.text


if __name__ == "__main__":
    test_build_spark_conf_local_only()
    test_build_spark_conf_with_s3a_and_rocksdb()
    test_redact_spark_conf_hides_credentials()

    log_stream = io.StringIO()
    handler = logging.StreamHandler(log_stream)
    spark_logger = logging.getLogger("src.streaming.spark_session")
    spark_logger.setLevel(logging.INFO)
    spark_logger.addHandler(handler)

    try:
        with tempfile.TemporaryDirectory() as tmp:
            _run_local_delta_roundtrip(Path(tmp))
    finally:
        spark_logger.removeHandler(handler)

    cfg = load()
    captured = log_stream.getvalue()
    assert cfg.s3_access_key not in captured
    assert cfg.s3_secret_key not in captured
    print("PASS: test_spark_session local Delta write/read roundtrip & credential-free logs")
