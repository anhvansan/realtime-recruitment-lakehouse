"""Unit tests for configuration loading and S3 endpoint parsing."""

from pathlib import Path

from src.common.config import AppConfig, load


def test_default_config_loading():
    """Verify default configuration loads with correct values and types."""
    cfg = load()
    assert cfg.s3_endpoint == "http://minio:9000"
    assert cfg.s3_endpoint_host_port == "minio:9000"
    assert cfg.s3_use_ssl is False
    assert cfg.s3_bucket == "lakehouse"
    assert cfg.speed_factor == 60
    assert cfg.pipeline.kafka.raw_topic == "cpc_events_raw"
    assert cfg.pipeline.streaming.watermark_interval == "10 minutes"


def test_s3_endpoint_host_port_variations():
    """Verify s3_endpoint_host_port correctly strips schemes and trailing slashes."""
    # Standard HTTP with port
    c1 = AppConfig(S3_ENDPOINT="http://minio:9000")
    assert c1.s3_endpoint_host_port == "minio:9000"
    assert c1.s3_use_ssl is False

    # HTTP with trailing slash
    c2 = AppConfig(S3_ENDPOINT="http://minio:9000/")
    assert c2.s3_endpoint_host_port == "minio:9000"
    assert c2.s3_use_ssl is False

    # HTTPS endpoint (e.g. AWS S3)
    c3 = AppConfig(S3_ENDPOINT="https://s3.amazonaws.com")
    assert c3.s3_endpoint_host_port == "s3.amazonaws.com"
    assert c3.s3_use_ssl is True

    # Bare host:port (no scheme provided)
    c4 = AppConfig(S3_ENDPOINT="localhost:9000")
    assert c4.s3_endpoint_host_port == "localhost:9000"
    assert c4.s3_use_ssl is False


def test_custom_env_file_loading(tmp_path: Path):
    """Verify custom .env file correctly overrides environment variables."""
    custom_env = tmp_path / ".env.test"
    custom_env.write_text(
        "S3_ENDPOINT=http://127.0.0.1:9000\n"
        "SPEED_FACTOR=120\n"
        "KAFKA_BOOTSTRAP_EXTERNAL=127.0.0.1:19092\n",
        encoding="utf-8",
    )
    cfg = load(env_file=custom_env)
    assert cfg.s3_endpoint == "http://127.0.0.1:9000"
    assert cfg.s3_endpoint_host_port == "127.0.0.1:9000"
    assert cfg.speed_factor == 120
    assert cfg.kafka_bootstrap_external == "127.0.0.1:19092"
