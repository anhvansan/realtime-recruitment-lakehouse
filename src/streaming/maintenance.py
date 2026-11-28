"""Safe Delta Lake compaction (OPTIMIZE) and retention maintenance (VACUUM) (Task 3.8).

Concurrency & safety guarantees:
  1. When streaming Q2 MERGE is active on the current simulated partition, use
     ``--closed-only`` so OPTIMIZE only compacts historical partitions
     (``event_date < current_sim_date``), preventing ConcurrentModification conflicts.
  2. Retries transient Delta OCC conflicts (``ConcurrentAppendException``,
     ``ConcurrentDeleteReadException``, etc.) with exponential backoff.
  3. Enforces a safety floor on ``VACUUM`` retention (default 168h; refuses < 1h) so
     active DuckDB / Spark readers never fail with FileNotFoundException on deleted
     Parquet files.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from collections.abc import Callable
from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING, Any, TypeVar

# Ensure project root is importable when invoked directly inside container
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.common.config import AppConfig, load  # noqa: E402
from src.streaming.spark_session import build_spark  # noqa: E402

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

_T = TypeVar("_T")

DEFAULT_VACUUM_RETENTION_HOURS = 168.0
MIN_SAFE_VACUUM_RETENTION_HOURS = 1.0

_DELTA_CONFLICT_MARKERS: tuple[str, ...] = (
    "ConcurrentAppendException",
    "ConcurrentDeleteReadException",
    "ConcurrentDeleteDeleteException",
    "ConcurrentTransactionException",
    "MetadataChangedException",
    "ProtocolChangedException",
)


def is_delta_conflict_error(exc: BaseException) -> bool:
    """Return True if an exception represents a retryable Delta OCC conflict."""
    msg = f"{type(exc).__name__}: {exc}"
    return any(marker in msg for marker in _DELTA_CONFLICT_MARKERS)


def build_closed_partitions_where(current_sim_date: str | date) -> str:
    """Build partition filter targeting only closed dates (< current_sim_date)."""
    date_str = (
        current_sim_date.isoformat()
        if isinstance(current_sim_date, date)
        else str(current_sim_date).strip()
    )
    if not date_str:
        raise ValueError("current_sim_date must not be empty")
    return f"event_date < '{date_str}'"


def validate_vacuum_retention(retention_hours: float) -> float:
    """Enforce safe minimum VACUUM retention (>= 1 hour) to protect active readers."""
    if retention_hours < MIN_SAFE_VACUUM_RETENTION_HOURS:
        raise ValueError(
            f"Refusing unsafe VACUUM retention {retention_hours}h "
            f"(minimum allowed is {MIN_SAFE_VACUUM_RETENTION_HOURS}h; default 168h)."
        )
    return float(retention_hours)


def run_with_conflict_retry(
    fn: Callable[[], _T],
    *,
    max_retries: int = 3,
    base_backoff_sec: float = 1.0,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> _T:
    """Execute ``fn`` and retry Delta OCC conflicts with exponential backoff."""
    attempt = 0
    while True:
        try:
            return fn()
        except Exception as exc:  # noqa: BLE001
            if not is_delta_conflict_error(exc) or attempt >= max_retries:
                raise
            wait_sec = base_backoff_sec * (2**attempt)
            attempt += 1
            logger.warning(
                "Delta OCC conflict detected (%s); retrying attempt %d/%d after %.2fs",
                type(exc).__name__,
                attempt,
                max_retries,
                wait_sec,
            )
            sleep_fn(wait_sec)


def get_table_stats(spark: SparkSession, table_path: str) -> dict[str, Any]:
    """Read file count and byte size metrics via ``DESCRIBE DETAIL`` / DeltaTable.detail()."""
    from delta.tables import DeltaTable  # type: ignore[import-not-found]

    if not DeltaTable.isDeltaTable(spark, table_path):
        return {
            "exists": False,
            "num_files": 0,
            "size_in_bytes": 0,
            "avg_file_size_bytes": 0,
        }

    detail = DeltaTable.forPath(spark, table_path).detail().collect()[0]
    num_files = int(detail["numFiles"] or 0)
    size_bytes = int(detail["sizeInBytes"] or 0)
    avg_bytes = round(size_bytes / num_files) if num_files > 0 else 0

    return {
        "exists": True,
        "num_files": num_files,
        "size_in_bytes": size_bytes,
        "avg_file_size_bytes": avg_bytes,
        "min_reader_version": int(detail["minReaderVersion"] or 1),
        "min_writer_version": int(detail["minWriterVersion"] or 2),
    }


def resolve_closed_partitions_where(spark: SparkSession, table_path: str) -> str | None:
    """Compute ``event_date < MAX(event_date)`` so active simulated date is skipped."""
    from delta.tables import DeltaTable  # type: ignore[import-not-found]
    from pyspark.sql import functions as F

    if not DeltaTable.isDeltaTable(spark, table_path):
        return None
    row = spark.read.format("delta").load(table_path).agg(F.max("event_date")).collect()[0]
    max_date = row[0]
    if max_date is None:
        return None
    return build_closed_partitions_where(max_date)


def compact_table(
    spark: SparkSession,
    table_path: str,
    *,
    where_clause: str | None = None,
    max_retries: int = 3,
    base_backoff_sec: float = 1.0,
) -> dict[str, Any]:
    """Run Delta ``OPTIMIZE`` compaction with optional partition filter and OCC retry."""
    from delta.tables import DeltaTable  # type: ignore[import-not-found]

    before = get_table_stats(spark, table_path)
    if not before["exists"]:
        return {
            "table_path": table_path,
            "where_clause": where_clause,
            "skipped": True,
            "reason": "Delta table does not exist",
            "before": before,
            "after": before,
        }

    def _optimize() -> None:
        optimizer = DeltaTable.forPath(spark, table_path).optimize()
        if where_clause:
            optimizer = optimizer.where(where_clause)
        optimizer.executeCompaction()

    t0 = time.perf_counter()
    run_with_conflict_retry(
        _optimize,
        max_retries=max_retries,
        base_backoff_sec=base_backoff_sec,
    )
    elapsed_sec = round(time.perf_counter() - t0, 2)
    after = get_table_stats(spark, table_path)

    return {
        "table_path": table_path,
        "where_clause": where_clause,
        "skipped": False,
        "elapsed_sec": elapsed_sec,
        "before": before,
        "after": after,
    }


def vacuum_table(
    spark: SparkSession,
    table_path: str,
    *,
    retention_hours: float = DEFAULT_VACUUM_RETENTION_HOURS,
    max_retries: int = 3,
    base_backoff_sec: float = 1.0,
) -> dict[str, Any]:
    """Run Delta ``VACUUM`` safely with minimum retention enforcement (>= 1h)."""
    from delta.tables import DeltaTable  # type: ignore[import-not-found]

    safe_hours = validate_vacuum_retention(retention_hours)
    stats = get_table_stats(spark, table_path)
    if not stats["exists"]:
        return {
            "table_path": table_path,
            "retention_hours": safe_hours,
            "skipped": True,
            "reason": "Delta table does not exist",
        }

    def _vacuum() -> None:
        DeltaTable.forPath(spark, table_path).vacuum(safe_hours)

    t0 = time.perf_counter()
    run_with_conflict_retry(
        _vacuum,
        max_retries=max_retries,
        base_backoff_sec=base_backoff_sec,
    )
    return {
        "table_path": table_path,
        "retention_hours": safe_hours,
        "skipped": False,
        "elapsed_sec": round(time.perf_counter() - t0, 2),
    }


def _resolve_targets(cfg: AppConfig, table_arg: str) -> list[tuple[str, str]]:
    mapping = {
        "bronze": cfg.s3a_uri(cfg.pipeline.storage.bronze_path),
        "silver": cfg.s3a_uri(cfg.pipeline.storage.silver_path),
    }
    if table_arg == "all":
        return list(mapping.items())
    return [(table_arg, mapping[table_arg])]


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run Delta Lake compaction (OPTIMIZE) and safe VACUUM."
    )
    parser.add_argument(
        "--table",
        choices=("bronze", "silver", "all"),
        default="all",
        help="Target Delta table(s) to maintain (default: all).",
    )
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument(
        "--closed-only",
        action="store_true",
        help="Only compact partitions strictly older than MAX(event_date) (safe during live MERGE).",
    )
    mode_group.add_argument(
        "--full",
        action="store_true",
        help="Compact all partitions including active simulated date (default).",
    )
    parser.add_argument(
        "--vacuum",
        action="store_true",
        help="Also run safe VACUUM after compaction.",
    )
    parser.add_argument(
        "--retention-hours",
        type=float,
        default=DEFAULT_VACUUM_RETENTION_HOURS,
        help="VACUUM retention hours (default: 168.0, minimum safe: 1.0).",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="WARN",
        help="Spark driver log level (default: WARN).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint for Delta compaction & safe maintenance."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    args = _parse_args(argv)
    if args.vacuum:
        validate_vacuum_retention(args.retention_hours)

    cfg = load()
    spark = build_spark("delta_maintenance", cfg=cfg, log_level=args.log_level)

    report: dict[str, Any] = {
        "mode": "closed_only" if args.closed_only else "full",
        "compaction": {},
        "vacuum": {},
    }

    try:
        for name, uri in _resolve_targets(cfg, args.table):
            where_clause = resolve_closed_partitions_where(spark, uri) if args.closed_only else None
            report["compaction"][name] = compact_table(spark, uri, where_clause=where_clause)
            if args.vacuum:
                report["vacuum"][name] = vacuum_table(
                    spark,
                    uri,
                    retention_hours=args.retention_hours,
                )
    finally:
        spark.stop()

    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
