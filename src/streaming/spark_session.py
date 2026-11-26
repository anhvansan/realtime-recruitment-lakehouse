"""Centralized SparkSession factory for CPC AdTech Lakehouse.

Builds a single local-mode SparkSession sized for a 16GB RAM host (mem_limit: 2304m,
driver heap 1536m, shuffle partitions 4, UTC timezone, Delta Lake extensions/catalog).

RocksDB state store note:
    Default heap-backed state store is kept enabled initially ("measure first, enable later");
    set ``enable_rocksdb=True`` or ``spark.enable_rocksdb: true`` in ``config/pipeline.yaml``
    after measuring state memory pressure in Task 3.7 / 3.9.

Security (Rule R6):
    Never log S3 credentials (``S3_ACCESS_KEY``, ``S3_SECRET_KEY``). Use
    ``redact_spark_conf`` before inspecting or logging Spark config dictionaries.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from src.common.config import AppConfig, load

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

DELTA_EXTENSION = "io.delta.sql.DeltaSparkSessionExtension"
DELTA_CATALOG = "org.apache.spark.sql.delta.catalog.DeltaCatalog"
ROCKSDB_STATE_STORE_PROVIDER = (
    "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider"
)
_SENSITIVE_KEY_TOKENS = ("secret", "password", "token", "access.key")


def redact_spark_conf(conf: dict[str, str]) -> dict[str, str]:
    """Return a copy of Spark configuration with sensitive credential values masked."""
    redacted: dict[str, str] = {}
    for key, value in conf.items():
        lower_key = key.lower()
        if any(token in lower_key for token in _SENSITIVE_KEY_TOKENS):
            redacted[key] = "***REDACTED***"
        else:
            redacted[key] = value
    return redacted


def build_spark_conf(
    app_name: str,
    cfg: AppConfig | None = None,
    *,
    local_only: bool = False,
    master: str | None = None,
    driver_memory: str | None = None,
    shuffle_partitions: int | None = None,
    enable_rocksdb: bool | None = None,
    warehouse_dir: str | None = None,
    ui_enabled: bool = False,
    extra_conf: dict[str, str] | None = None,
) -> dict[str, str]:
    """Build Spark configuration map from AppConfig and explicit overrides."""
    resolved_cfg = cfg or load()
    spark_cfg = resolved_cfg.pipeline.spark

    resolved_master = master or spark_cfg.master
    resolved_driver_mem = driver_memory or spark_cfg.driver_memory
    resolved_shuffle = (
        shuffle_partitions if shuffle_partitions is not None else spark_cfg.shuffle_partitions
    )
    resolved_warehouse = warehouse_dir or spark_cfg.warehouse_dir
    use_rocksdb = enable_rocksdb if enable_rocksdb is not None else spark_cfg.enable_rocksdb

    conf: dict[str, str] = {
        "spark.app.name": app_name,
        "spark.master": resolved_master,
        "spark.driver.memory": resolved_driver_mem,
        "spark.sql.shuffle.partitions": str(resolved_shuffle),
        "spark.sql.session.timeZone": spark_cfg.timezone,
        "spark.sql.warehouse.dir": resolved_warehouse,
        "spark.sql.extensions": DELTA_EXTENSION,
        "spark.sql.catalog.spark_catalog": DELTA_CATALOG,
        "spark.ui.enabled": str(ui_enabled).lower(),
    }

    if use_rocksdb:
        conf["spark.sql.streaming.stateStore.providerClass"] = ROCKSDB_STATE_STORE_PROVIDER

    if not local_only:
        conf.update(
            {
                "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
                "spark.hadoop.fs.s3a.endpoint": resolved_cfg.s3_endpoint,
                "spark.hadoop.fs.s3a.access.key": resolved_cfg.s3_access_key,
                "spark.hadoop.fs.s3a.secret.key": resolved_cfg.s3_secret_key,
                "spark.hadoop.fs.s3a.path.style.access": "true",
                "spark.hadoop.fs.s3a.connection.ssl.enabled": str(resolved_cfg.s3_use_ssl).lower(),
                "spark.hadoop.fs.s3a.aws.credentials.provider": (
                    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
                ),
            }
        )

    if extra_conf:
        conf.update(extra_conf)

    return conf


def _apply_hadoop_conf(spark: SparkSession, conf: dict[str, str]) -> None:
    """Apply ``spark.hadoop.*`` settings onto an existing SparkSession's Hadoop Configuration.

    Required when ``SparkSession.builder.getOrCreate()`` attaches to an already-active
    session (e.g. inside ``make spark-shell``), where Spark ignores new ``spark.hadoop.*``
    builder configs.
    """
    jsc: Any = spark._jsc
    if jsc is None:
        return
    hadoop_conf = jsc.hadoopConfiguration()
    for key, value in conf.items():
        if key.startswith("spark.hadoop."):
            hadoop_conf.set(key.removeprefix("spark.hadoop."), value)


def build_spark(
    app_name: str,
    cfg: AppConfig | None = None,
    *,
    local_only: bool = False,
    master: str | None = None,
    driver_memory: str | None = None,
    shuffle_partitions: int | None = None,
    enable_rocksdb: bool | None = None,
    warehouse_dir: str | None = None,
    ui_enabled: bool = False,
    log_level: str = "WARN",
    extra_conf: dict[str, str] | None = None,
) -> SparkSession:
    """Create or retrieve a configured SparkSession with Delta Lake and optional S3A."""
    resolved_cfg = cfg or load()
    conf = build_spark_conf(
        app_name=app_name,
        cfg=resolved_cfg,
        local_only=local_only,
        master=master,
        driver_memory=driver_memory,
        shuffle_partitions=shuffle_partitions,
        enable_rocksdb=enable_rocksdb,
        warehouse_dir=warehouse_dir,
        ui_enabled=ui_enabled,
        extra_conf=extra_conf,
    )

    from pyspark.sql import SparkSession

    spark_builder: Any = SparkSession.builder
    builder = spark_builder.appName(conf["spark.app.name"]).master(conf["spark.master"])
    for key, value in conf.items():
        if key in ("spark.app.name", "spark.master"):
            continue
        builder = builder.config(key, value)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(log_level)
    if not local_only:
        _apply_hadoop_conf(spark, conf)

    use_rocksdb = (
        enable_rocksdb if enable_rocksdb is not None else resolved_cfg.pipeline.spark.enable_rocksdb
    )
    if local_only:
        logger.info(
            "Initialized SparkSession app=%s master=%s driver_memory=%s "
            "shuffle_partitions=%s local_only=%s enable_rocksdb=%s",
            conf["spark.app.name"],
            conf["spark.master"],
            conf["spark.driver.memory"],
            conf["spark.sql.shuffle.partitions"],
            local_only,
            use_rocksdb,
        )
    else:
        logger.info(
            "Initialized SparkSession app=%s master=%s driver_memory=%s "
            "shuffle_partitions=%s local_only=%s enable_rocksdb=%s s3_endpoint=%s",
            conf["spark.app.name"],
            conf["spark.master"],
            conf["spark.driver.memory"],
            conf["spark.sql.shuffle.partitions"],
            local_only,
            use_rocksdb,
            resolved_cfg.s3_endpoint,
        )

    return spark


def list_s3a_paths(spark: SparkSession, uri: str) -> list[str]:
    """List top-level entries under an S3A/Hadoop URI without logging credentials."""
    jvm: Any = spark._jvm
    jsc: Any = spark._jsc
    if jvm is None or jsc is None:
        raise RuntimeError("SparkSession JVM gateway is not initialized.")
    hadoop_conf = jsc.hadoopConfiguration()
    java_uri = jvm.java.net.URI.create(uri)
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(java_uri, hadoop_conf)
    target_path = jvm.org.apache.hadoop.fs.Path(uri)
    if not fs.exists(target_path):
        return []

    paths: list[str] = []
    for status in fs.listStatus(target_path):
        entry = str(status.getPath().toString())
        if status.isDirectory() and not entry.endswith("/"):
            entry = f"{entry}/"
        paths.append(entry)
    return sorted(paths)
