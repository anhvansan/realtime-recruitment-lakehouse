"""Host unit tests for Task 3.6 — Unified Q1 + Q2 streaming runner CLI."""

from __future__ import annotations

from src.streaming.run_all import _parse_args


def test_run_all_parse_args_defaults() -> None:
    """Verify default CLI options for continuous 1-JVM streaming execution."""
    args = _parse_args([])
    assert args.available_now is False
    assert args.starting_offsets == "earliest"
    assert args.no_kafka_dlq is False
    assert args.log_level == "WARN"


def test_run_all_parse_args_available_now() -> None:
    """Verify explicit CLI flags for drain mode and logging."""
    args = _parse_args(
        ["--available-now", "--starting-offsets", "latest", "--no-kafka-dlq", "--log-level", "INFO"]
    )
    assert args.available_now is True
    assert args.starting_offsets == "latest"
    assert args.no_kafka_dlq is True
    assert args.log_level == "INFO"
