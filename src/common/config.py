"""Central configuration loader for CPC AdTech Lakehouse.

Loads environment variables (via .env) and pipeline parameters (via config/pipeline.yaml).
Enforces decoupling: no credentials or endpoints are hard-coded.
Provides s3_endpoint_host_port and s3_use_ssl required for DuckDB secrets.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class KafkaConfig(BaseModel):
    raw_topic: str = "cpc_events_raw"
    dlq_topic: str = "cpc_events_dlq"
    partitions: int = 3
    replication_factor: int = 1


class StreamingConfig(BaseModel):
    watermark_interval: str = "10 minutes"
    ingest_trigger: str = "10 seconds"
    refine_trigger: str = "10 seconds"
    max_offsets_per_trigger: int = 50000


class StoragePathConfig(BaseModel):
    bucket: str = "lakehouse"
    bronze_path: str = "bronze/cpc_events"
    silver_path: str = "silver/cpc_events"
    dlq_path: str = "dlq/cpc_events_dlq"
    ops_path: str = "ops/stream_metrics"
    checkpoint_base: str = "checkpoints"


class PipelineYamlConfig(BaseModel):
    kafka: KafkaConfig = Field(default_factory=KafkaConfig)
    streaming: StreamingConfig = Field(default_factory=StreamingConfig)
    storage: StoragePathConfig = Field(default_factory=StoragePathConfig)


class AppConfig(BaseSettings):
    """Application settings loaded from environment variables and config files."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Kafka
    kafka_bootstrap_internal: str = Field(
        default="redpanda:9092",
        validation_alias="KAFKA_BOOTSTRAP_INTERNAL",
    )
    kafka_bootstrap_external: str = Field(
        default="localhost:19092",
        validation_alias="KAFKA_BOOTSTRAP_EXTERNAL",
    )

    # MinIO / S3
    s3_endpoint: str = Field(
        default="http://minio:9000",
        validation_alias="S3_ENDPOINT",
    )
    s3_endpoint_external: str = Field(
        default="http://localhost:9000",
        validation_alias="S3_ENDPOINT_EXTERNAL",
    )
    s3_access_key: str = Field(
        default="minioadmin",
        validation_alias="S3_ACCESS_KEY",
    )
    s3_secret_key: str = Field(
        default="minioadmin",
        validation_alias="S3_SECRET_KEY",
    )
    s3_bucket: str = Field(
        default="lakehouse",
        validation_alias="S3_BUCKET",
    )
    s3_region: str = Field(
        default="us-east-1",
        validation_alias="S3_REGION",
    )

    # Host Data Mount
    data_root: str = Field(
        default="D:/docker_data/cpc_lakehouse",
        validation_alias="DATA_ROOT",
    )

    # Simulation
    speed_factor: int = Field(
        default=60,
        validation_alias="SPEED_FACTOR",
    )
    sim_start_date: str = Field(
        default="2026-01-01",
        validation_alias="SIM_START_DATE",
    )

    # Alerting
    telegram_bot_token: str | None = Field(
        default=None,
        validation_alias="TELEGRAM_BOT_TOKEN",
    )
    telegram_chat_id: str | None = Field(
        default=None,
        validation_alias="TELEGRAM_CHAT_ID",
    )
    discord_webhook_url: str | None = Field(
        default=None,
        validation_alias="DISCORD_WEBHOOK_URL",
    )
    alert_dry_run: bool = Field(
        default=True,
        validation_alias="ALERT_DRY_RUN",
    )

    # Pipeline YAML settings (nested)
    pipeline: PipelineYamlConfig = Field(default_factory=PipelineYamlConfig)

    @property
    def s3_endpoint_host_port(self) -> str:
        """Strip scheme (http:// or https://) and trailing slashes for DuckDB / dbt-duckdb.

        Example:
            'http://minio:9000' -> 'minio:9000'
            'https://s3.amazonaws.com' -> 's3.amazonaws.com'
        """
        clean_url = re.sub(r"^https?://", "", self.s3_endpoint.strip())
        return clean_url.rstrip("/")

    @property
    def s3_use_ssl(self) -> bool:
        """Return True if s3_endpoint uses SSL (https://), otherwise False."""
        return self.s3_endpoint.strip().lower().startswith("https://")


def _find_project_root() -> Path:
    """Find repository root by looking for pyproject.toml or .git."""
    curr = Path(__file__).resolve().parent
    for p in [curr, *curr.parents]:
        if (p / "pyproject.toml").exists() or (p / ".git").exists():
            return p
    return Path.cwd()


def load(
    env_file: str | Path | None = None,
    yaml_path: str | Path | None = None,
) -> AppConfig:
    """Load application configuration from .env and config/pipeline.yaml.

    Args:
        env_file: Optional explicit path to .env file.
        yaml_path: Optional explicit path to pipeline.yaml file.

    Returns:
        AppConfig populated instance.
    """
    root = _find_project_root()

    if env_file is None:
        default_env = root / ".env"
        env_file = default_env if default_env.exists() else None

    # Load YAML if present
    yaml_config = PipelineYamlConfig()
    target_yaml = Path(yaml_path) if yaml_path else (root / "config" / "pipeline.yaml")
    if target_yaml.exists():
        with open(target_yaml, encoding="utf-8") as f:
            data: dict[str, Any] = yaml.safe_load(f) or {}
            yaml_config = PipelineYamlConfig(**data)

    config_kwargs: dict[str, Any] = {"pipeline": yaml_config}
    if env_file:
        config_kwargs["_env_file"] = str(env_file)

    return AppConfig(**config_kwargs)
