"""Host unit tests for Task 3.8 — Safe Delta Maintenance & Conflict Retry."""

from __future__ import annotations

from datetime import date

import pytest

from src.streaming.maintenance import (
    build_closed_partitions_where,
    is_delta_conflict_error,
    run_with_conflict_retry,
    validate_vacuum_retention,
)


def test_is_delta_conflict_error() -> None:
    """Detect Delta OCC exception markers while ignoring unrelated errors."""

    class ConcurrentAppendException(RuntimeError):
        pass

    class ConcurrentDeleteReadException(RuntimeError):
        pass

    assert is_delta_conflict_error(ConcurrentAppendException("conflict on commit 5")) is True
    assert is_delta_conflict_error(ConcurrentDeleteReadException("deleted file")) is True
    assert is_delta_conflict_error(ValueError("bad schema")) is False


def test_run_with_conflict_retry_succeeds_after_transient_conflicts() -> None:
    """Retry transient Delta OCC errors with exponential backoff and succeed."""
    sleep_calls: list[float] = []
    attempts = {"count": 0}

    class ConcurrentAppendException(RuntimeError):
        pass

    def _flaky_op() -> str:
        attempts["count"] += 1
        if attempts["count"] < 3:
            raise ConcurrentAppendException("transient MERGE vs OPTIMIZE collision")
        return "compacted"

    result = run_with_conflict_retry(
        _flaky_op,
        max_retries=3,
        base_backoff_sec=0.5,
        sleep_fn=sleep_calls.append,
    )
    assert result == "compacted"
    assert attempts["count"] == 3
    assert sleep_calls == [0.5, 1.0]


def test_run_with_conflict_retry_raises_non_conflict_immediately() -> None:
    """Non-OCC exceptions must fail fast without backoff retries."""
    sleep_calls: list[float] = []

    def _bad_op() -> None:
        raise ValueError("invalid predicate")

    with pytest.raises(ValueError, match="invalid predicate"):
        run_with_conflict_retry(_bad_op, max_retries=3, sleep_fn=sleep_calls.append)
    assert sleep_calls == []


def test_run_with_conflict_retry_exhausted_raises() -> None:
    """Re-raise Delta conflict once max_retries is exhausted."""
    sleep_calls: list[float] = []

    class ConcurrentAppendException(RuntimeError):
        pass

    def _always_conflict() -> None:
        raise ConcurrentAppendException("still colliding")

    with pytest.raises(ConcurrentAppendException):
        run_with_conflict_retry(
            _always_conflict,
            max_retries=2,
            base_backoff_sec=0.25,
            sleep_fn=sleep_calls.append,
        )
    assert sleep_calls == [0.25, 0.5]


def test_validate_vacuum_retention_blocks_unsafe_values() -> None:
    """Never allow vacuum(0) or < 1h retention that crashes concurrent readers."""
    assert validate_vacuum_retention(168.0) == 168.0
    assert validate_vacuum_retention(1.0) == 1.0
    for unsafe in (0.0, 0.5, -1.0):
        with pytest.raises(ValueError, match="Refusing unsafe VACUUM retention"):
            validate_vacuum_retention(unsafe)


def test_build_closed_partitions_where() -> None:
    """Generate partition pruning predicate strictly older than current_sim_date."""
    assert build_closed_partitions_where("2026-04-06") == "event_date < '2026-04-06'"
    assert build_closed_partitions_where(date(2026, 4, 6)) == "event_date < '2026-04-06'"
    with pytest.raises(ValueError):
        build_closed_partitions_where("   ")
