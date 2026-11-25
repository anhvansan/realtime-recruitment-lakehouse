#!/usr/bin/env python3
"""Replay Producer v1 (clean stream, --no-faults) for Redpanda topic cpc_events_raw.

Merges the 1,875 clean CPC click events from tracking.csv and 102 recruitment funnel
events from application.csv via DuckDB, sorts by original timestamp ascending into a
temporary Parquet file, streams rows in bounded Arrow chunks, maps simulation time
with absolute deadline scheduling, and produces idempotent JSON messages keyed by
campaign_id.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import signal
import sys
import threading
import time
from collections.abc import Iterator
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import duckdb
import pyarrow.parquet as pq
from confluent_kafka import KafkaError, KafkaException, Message, Producer

# Ensure project root is importable when invoked directly as a script
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.common.config import AppConfig, load  # noqa: E402
from src.common.schemas.cpc_event import CpcEventV1, validate_event  # noqa: E402
from src.producer.time_mapper import TimeMapper, ensure_utc, parse_sim_start  # noqa: E402


class DeliveryStats:
    """Thread-safe delivery metrics updated from confluent-kafka on_delivery callbacks."""

    def __init__(self) -> None:
        self.produced: int = 0
        self.acked: int = 0
        self.delivery_errors: int = 0
        self.buffer_retries: int = 0
        self.max_behind_ms: float = 0.0

    def on_delivery(self, err: KafkaError | None, _msg: Message) -> None:
        """Handle broker delivery acknowledgement or error callback."""
        if err is not None:
            self.delivery_errors += 1
            print(f"[DELIVERY ERROR] {err}", file=sys.stderr, flush=True)
        else:
            self.acked += 1


def _build_stream_sql(raw_dir: Path, single_file: Path | None) -> str:
    """Build DuckDB SQL query producing canonical clean event candidates sorted by time."""
    if single_file is not None:
        resolved = single_file.as_posix()
        name = single_file.name.lower()
        if name == "application.csv":
            tracking_path = (raw_dir / "tracking.csv").as_posix()
            job_path = (raw_dir / "job.csv").as_posix()
            campaign_path = (raw_dir / "campaign.csv").as_posix()
            return _application_only_sql(resolved, tracking_path, job_path, campaign_path)
        if name == "tracking.csv":
            return _tracking_only_sql(resolved)
        raise ValueError(
            f"Unsupported single --file {single_file}; expected tracking.csv or application.csv"
        )

    tracking_path = (raw_dir / "tracking.csv").as_posix()
    app_path = (raw_dir / "application.csv").as_posix()
    job_path = (raw_dir / "job.csv").as_posix()
    campaign_path = (raw_dir / "campaign.csv").as_posix()

    return f"""
        WITH job_to_camp AS (
            SELECT job_id, MIN(campaign_id) AS campaign_id
            FROM read_csv_auto('{tracking_path}')
            WHERE job_id IS NOT NULL AND campaign_id IS NOT NULL
            GROUP BY job_id
        ),
        company_to_camp AS (
            SELECT company_id, MAX(id) AS campaign_id
            FROM read_csv_auto('{campaign_path}')
            WHERE company_id IS NOT NULL
            GROUP BY company_id
        ),
        job_to_pub AS (
            SELECT job_id, MIN(publisher_id) AS publisher_id
            FROM read_csv_auto('{tracking_path}')
            WHERE job_id IS NOT NULL AND publisher_id IS NOT NULL
            GROUP BY job_id
        ),
        camp_to_pub AS (
            SELECT campaign_id, MIN(publisher_id) AS publisher_id
            FROM read_csv_auto('{tracking_path}')
            WHERE campaign_id IS NOT NULL AND publisher_id IS NOT NULL
            GROUP BY campaign_id
        ),
        tracking_raw AS (
            SELECT
                ROW_NUMBER() OVER () AS source_row_id,
                *
            FROM read_csv_auto('{tracking_path}')
        ),
        tracking_numbered AS (
            SELECT
                source_row_id,
                'tracking.csv' AS source_file,
                ts AS orig_ts,
                'click' AS event_type,
                CAST(job_id AS VARCHAR) AS job_id,
                CAST(campaign_id AS VARCHAR) AS campaign_id,
                CASE WHEN group_id IS NOT NULL THEN CAST(group_id AS VARCHAR) ELSE NULL END AS group_id,
                CAST(publisher_id AS VARCHAR) AS publisher_id,
                CAST(ROUND(bid, 4) AS DECIMAL(18, 4)) AS bid_dec,
                CAST(ROUND(bid, 4) AS DECIMAL(18, 4)) AS spend_dec
            FROM tracking_raw
            WHERE campaign_id IS NOT NULL
              AND job_id IS NOT NULL
              AND publisher_id IS NOT NULL
              AND bid IS NOT NULL
              AND ts IS NOT NULL
        ),
        app_raw AS (
            SELECT
                ROW_NUMBER() OVER () AS source_row_id,
                *
            FROM read_csv_auto('{app_path}')
        ),
        app_numbered AS (
            SELECT
                a.source_row_id,
                'application.csv' AS source_file,
                a.created_date AS orig_ts,
                CASE
                    WHEN a.is_rejected = 1 THEN 'unqualified'
                    WHEN a.status = 2 OR a.is_match = 1 THEN 'qualified'
                    ELSE 'apply'
                END AS event_type,
                CAST(a.job_id AS VARCHAR) AS job_id,
                CAST(COALESCE(jtc.campaign_id, ctc.campaign_id) AS VARCHAR) AS campaign_id,
                CASE WHEN j.group_id IS NOT NULL THEN CAST(j.group_id AS VARCHAR) ELSE NULL END AS group_id,
                COALESCE(
                    CAST(a.publisher_id AS VARCHAR),
                    CAST(jtp.publisher_id AS VARCHAR),
                    CAST(ctp.publisher_id AS VARCHAR),
                    'unknown'
                ) AS publisher_id,
                CAST(0.0000 AS DECIMAL(18, 4)) AS bid_dec,
                CAST(0.0000 AS DECIMAL(18, 4)) AS spend_dec
            FROM app_raw a
            LEFT JOIN job_to_camp jtc ON a.job_id = jtc.job_id
            LEFT JOIN read_csv_auto('{job_path}') j ON a.job_id = j.id
            LEFT JOIN company_to_camp ctc ON j.company_id = ctc.company_id
            LEFT JOIN job_to_pub jtp ON a.job_id = jtp.job_id
            LEFT JOIN camp_to_pub ctp ON COALESCE(jtc.campaign_id, ctc.campaign_id) = ctp.campaign_id
            WHERE a.created_date IS NOT NULL
              AND a.job_id IS NOT NULL
              AND COALESCE(jtc.campaign_id, ctc.campaign_id) IS NOT NULL
        ),
        merged AS (
            SELECT * FROM tracking_numbered
            UNION ALL
            SELECT * FROM app_numbered
        )
        SELECT * FROM merged
        ORDER BY orig_ts ASC, source_file ASC, source_row_id ASC
    """


def _tracking_only_sql(tracking_path: str) -> str:
    return f"""
        WITH raw_rows AS (
            SELECT
                ROW_NUMBER() OVER () AS source_row_id,
                *
            FROM read_csv_auto('{tracking_path}')
        ),
        numbered AS (
            SELECT
                source_row_id,
                'tracking.csv' AS source_file,
                ts AS orig_ts,
                'click' AS event_type,
                CAST(job_id AS VARCHAR) AS job_id,
                CAST(campaign_id AS VARCHAR) AS campaign_id,
                CASE WHEN group_id IS NOT NULL THEN CAST(group_id AS VARCHAR) ELSE NULL END AS group_id,
                CAST(publisher_id AS VARCHAR) AS publisher_id,
                CAST(ROUND(bid, 4) AS DECIMAL(18, 4)) AS bid_dec,
                CAST(ROUND(bid, 4) AS DECIMAL(18, 4)) AS spend_dec
            FROM raw_rows
            WHERE campaign_id IS NOT NULL
              AND job_id IS NOT NULL
              AND publisher_id IS NOT NULL
              AND bid IS NOT NULL
              AND ts IS NOT NULL
        )
        SELECT * FROM numbered
        ORDER BY orig_ts ASC, source_row_id ASC
    """


def _application_only_sql(
    app_path: str, tracking_path: str, job_path: str, campaign_path: str
) -> str:
    return f"""
        WITH job_to_camp AS (
            SELECT job_id, MIN(campaign_id) AS campaign_id
            FROM read_csv_auto('{tracking_path}')
            WHERE job_id IS NOT NULL AND campaign_id IS NOT NULL
            GROUP BY job_id
        ),
        company_to_camp AS (
            SELECT company_id, MAX(id) AS campaign_id
            FROM read_csv_auto('{campaign_path}')
            WHERE company_id IS NOT NULL
            GROUP BY company_id
        ),
        job_to_pub AS (
            SELECT job_id, MIN(publisher_id) AS publisher_id
            FROM read_csv_auto('{tracking_path}')
            WHERE job_id IS NOT NULL AND publisher_id IS NOT NULL
            GROUP BY job_id
        ),
        camp_to_pub AS (
            SELECT campaign_id, MIN(publisher_id) AS publisher_id
            FROM read_csv_auto('{tracking_path}')
            WHERE campaign_id IS NOT NULL AND publisher_id IS NOT NULL
            GROUP BY campaign_id
        ),
        raw_apps AS (
            SELECT
                ROW_NUMBER() OVER () AS source_row_id,
                *
            FROM read_csv_auto('{app_path}')
        ),
        numbered AS (
            SELECT
                a.source_row_id,
                'application.csv' AS source_file,
                a.created_date AS orig_ts,
                CASE
                    WHEN a.is_rejected = 1 THEN 'unqualified'
                    WHEN a.status = 2 OR a.is_match = 1 THEN 'qualified'
                    ELSE 'apply'
                END AS event_type,
                CAST(a.job_id AS VARCHAR) AS job_id,
                CAST(COALESCE(jtc.campaign_id, ctc.campaign_id) AS VARCHAR) AS campaign_id,
                CASE WHEN j.group_id IS NOT NULL THEN CAST(j.group_id AS VARCHAR) ELSE NULL END AS group_id,
                COALESCE(
                    CAST(a.publisher_id AS VARCHAR),
                    CAST(jtp.publisher_id AS VARCHAR),
                    CAST(ctp.publisher_id AS VARCHAR),
                    'unknown'
                ) AS publisher_id,
                CAST(0.0000 AS DECIMAL(18, 4)) AS bid_dec,
                CAST(0.0000 AS DECIMAL(18, 4)) AS spend_dec
            FROM raw_apps a
            LEFT JOIN job_to_camp jtc ON a.job_id = jtc.job_id
            LEFT JOIN read_csv_auto('{job_path}') j ON a.job_id = j.id
            LEFT JOIN company_to_camp ctc ON j.company_id = ctc.company_id
            LEFT JOIN job_to_pub jtp ON a.job_id = jtp.job_id
            LEFT JOIN camp_to_pub ctp ON COALESCE(jtc.campaign_id, ctc.campaign_id) = ctp.campaign_id
            WHERE a.created_date IS NOT NULL
              AND a.job_id IS NOT NULL
              AND COALESCE(jtc.campaign_id, ctc.campaign_id) IS NOT NULL
        )
        SELECT * FROM numbered
        ORDER BY orig_ts ASC, source_row_id ASC
    """


def prepare_sorted_parquet(
    raw_dir: Path,
    out_parquet: Path,
    single_file: Path | None = None,
) -> tuple[int, datetime]:
    """Merge, enrich, sort chronologically via DuckDB and export to temporary Parquet."""
    out_parquet.parent.mkdir(parents=True, exist_ok=True)
    sql = _build_stream_sql(raw_dir, single_file)
    safe_out = out_parquet.as_posix().replace("'", "''")

    con = duckdb.connect(":memory:")
    try:
        con.execute(f"COPY ({sql}) TO '{safe_out}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        row = con.execute(
            f"SELECT COUNT(*), MIN(orig_ts) FROM read_parquet('{safe_out}')"
        ).fetchone()
        count = int(row[0])
        min_ts = ensure_utc(row[1]) if row[1] is not None else datetime.now(UTC)
        return count, min_ts
    finally:
        con.close()


def iter_parquet_chunks(
    parquet_path: Path,
    chunk_size: int = 256,
    start_row: int = 0,
    limit: int | None = None,
) -> Iterator[dict[str, Any]]:
    """Stream sorted event rows in bounded PyArrow batches without full-table RAM load."""
    pf = pq.ParquetFile(parquet_path)
    seen = 0
    yielded = 0

    for batch in pf.iter_batches(batch_size=chunk_size):
        batch_len = batch.num_rows
        if seen + batch_len <= start_row:
            seen += batch_len
            continue

        for row in batch.to_pylist():
            if seen < start_row:
                seen += 1
                continue
            if limit is not None and yielded >= limit:
                return
            yield row
            seen += 1
            yielded += 1


def compute_event_id(source_file: str, source_row_id: int) -> str:
    """Compute deterministic SHA-1 event_id so replays produce identical IDs."""
    return hashlib.sha1(f"{source_file}:{source_row_id}".encode()).hexdigest()


def build_event_payload(
    row: dict[str, Any],
    mapper: TimeMapper,
    produced_at: datetime | None = None,
) -> dict[str, Any]:
    """Convert a sorted stream row into a canonical cpc_event.v1 wire dict."""
    source_file = str(row["source_file"])
    source_row_id = int(row["source_row_id"])
    event_type = str(row["event_type"])

    bid_val = Decimal(str(row["bid_dec"])) if row.get("bid_dec") is not None else Decimal("0.0000")
    spend_val = bid_val if event_type == "click" else Decimal("0.0000")

    sim_event_time = mapper.map_sim_time(row["orig_ts"])
    emit_time = ensure_utc(produced_at or datetime.now(UTC))
    publisher_id = str(row.get("publisher_id") or "").strip() or "unknown"

    model = CpcEventV1(
        schema_version="v1",
        event_id=compute_event_id(source_file, source_row_id),
        event_time=sim_event_time,
        produced_at=emit_time,
        event_type=event_type,  # type: ignore[arg-type]
        job_id=str(row["job_id"]),
        campaign_id=str(row["campaign_id"]),
        group_id=str(row["group_id"]) if row.get("group_id") is not None else None,
        publisher_id=publisher_id,
        bid=bid_val if event_type == "click" else Decimal("0.0000"),
        spend=spend_val,
        source_row_id=source_row_id,
    )
    return model.to_wire_dict()


def build_producer(bootstrap_servers: str) -> Producer:
    """Create a confluent-kafka Producer configured for safe idempotent delivery."""
    return Producer(
        {
            "bootstrap.servers": bootstrap_servers,
            "broker.address.family": "v4",
            "enable.idempotence": True,
            "acks": "all",
            "compression.type": "lz4",
            "linger.ms": 20,
            "queue.buffering.max.messages": 100000,
        }
    )


def _produce_with_backpressure(
    producer: Producer,
    topic: str,
    key: str,
    value: str,
    stats: DeliveryStats,
) -> None:
    """Send message to Kafka with BufferError handling via poll + retry."""
    while True:
        try:
            producer.produce(
                topic=topic,
                key=key.encode("utf-8"),
                value=value.encode("utf-8"),
                on_delivery=stats.on_delivery,
            )
            stats.produced += 1
            return
        except BufferError:
            stats.buffer_retries += 1
            producer.poll(0.1)


def _parse_args(argv: list[str] | None, cfg: AppConfig) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Replay clean CPC + application events into Redpanda cpc_events_raw."
    )
    parser.add_argument(
        "--file",
        type=Path,
        default=None,
        help="Optional single raw CSV file (tracking.csv or application.csv). Default: both merged.",
    )
    parser.add_argument(
        "--raw-dir",
        type=Path,
        default=Path("data/raw"),
        help="Directory holding raw CSV datasets (default: data/raw).",
    )
    parser.add_argument(
        "--parquet-out",
        type=Path,
        default=Path("data/tmp/replay_sorted.parquet"),
        help="Temporary sorted Parquet cache path.",
    )
    parser.add_argument(
        "--speed-factor",
        type=float,
        default=float(cfg.speed_factor),
        help="Simulation clock multiplier (e.g., 60 => 1 real min = 1 sim hour).",
    )
    parser.add_argument(
        "--max-rate",
        type=float,
        default=None,
        help="Optional maximum emission rate cap (messages/sec).",
    )
    parser.add_argument(
        "--max-sleep-sec",
        type=float,
        default=None,
        help="Optional cap on quiet inter-event wait (seconds) while preserving sim_time.",
    )
    parser.add_argument(
        "--start-row",
        type=int,
        default=0,
        help="0-based row offset in the sorted stream (useful for resume or Lab L2).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional maximum number of events to emit.",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=256,
        help="PyArrow batch size when streaming temporary Parquet file.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Deterministic seed (reserved for fault injection in Task 2.6).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate contract and print sample events without connecting to Redpanda.",
    )
    parser.add_argument(
        "--no-faults",
        action="store_true",
        help="Emit clean stream with zero fault injection (Task 2.5 baseline).",
    )
    return parser.parse_args(argv)


def run_replay(args: argparse.Namespace, cfg: AppConfig) -> int:
    """Execute full DuckDB sort -> chunk stream -> deadline-paced Kafka replay."""
    if args.start_row < 0:
        raise ValueError("--start-row must be >= 0")
    if args.max_rate is not None and args.max_rate <= 0:
        raise ValueError("--max-rate must be > 0")
    if args.max_sleep_sec is not None and args.max_sleep_sec < 0:
        raise ValueError("--max-sleep-sec must be >= 0")

    t0_prep = time.perf_counter()
    total_available, stream_orig_t0 = prepare_sorted_parquet(
        raw_dir=args.raw_dir,
        out_parquet=args.parquet_out,
        single_file=args.file,
    )
    prep_ms = (time.perf_counter() - t0_prep) * 1000.0

    # Anchor orig_t0 on the first row actually emitted so --start-row skips dead history
    first_iter = iter_parquet_chunks(
        args.parquet_out, chunk_size=1, start_row=args.start_row, limit=1
    )
    first_row = next(first_iter, None)
    if first_row is None:
        print(f"⚠️  Không có sự kiện nào từ start_row={args.start_row} (tổng={total_available}).")
        return 0
    effective_orig_t0 = ensure_utc(first_row["orig_ts"])

    sim_start = parse_sim_start(cfg.sim_start_date)
    t_start = time.monotonic()
    mapper = TimeMapper(
        orig_t0=effective_orig_t0,
        sim_start=sim_start,
        t_start_monotonic=t_start,
        speed_factor=args.speed_factor,
    )

    topic = cfg.pipeline.kafka.raw_topic
    bootstrap = cfg.kafka_bootstrap_external

    print("=" * 84)
    print("📨 CPC REPLAY PRODUCER v1 (Task 2.5)")
    print(f"   Sorted Parquet : {args.parquet_out} ({total_available:,} rows, prep {prep_ms:.1f}ms)")
    print(
        f"   Start/Limit    : start_row={args.start_row}, limit={args.limit or 'ALL'}, "
        f"speed_factor={args.speed_factor}x, max_rate={args.max_rate or 'unlimited'}"
    )
    print(f"   Orig t0 -> Sim : {effective_orig_t0.isoformat()} -> {sim_start.isoformat()}")
    print(
        f"   Mode           : {'DRY-RUN' if args.dry_run else f'LIVE -> {bootstrap} ({topic})'} | "
        f"no_faults={args.no_faults} | seed={args.seed}"
    )
    print("=" * 84)

    stop_event = threading.Event()
    prev_sigint = signal.getsignal(signal.SIGINT)

    def _handle_sigint(_sig: int, _frame: Any) -> None:
        if not stop_event.is_set():
            print("\n🛑 Nhận SIGINT (Ctrl+C) — đang dừng sạch và flush buffer...", flush=True)
            stop_event.set()

    signal.signal(signal.SIGINT, _handle_sigint)

    producer = None if args.dry_run else build_producer(bootstrap)
    stats = DeliveryStats()
    last_log_ts = t_start
    last_emit_ts: float | None = None
    min_interval_sec = (1.0 / args.max_rate) if args.max_rate else 0.0
    prev_event_time: datetime | None = None
    non_monotonic_count = 0
    contract_failures = 0
    wall_shift_sec = 0.0

    try:
        for row in iter_parquet_chunks(
            args.parquet_out,
            chunk_size=args.chunk_size,
            start_row=args.start_row,
            limit=args.limit,
        ):
            if stop_event.is_set():
                break

            orig_ts = ensure_utc(row["orig_ts"])
            deadline = mapper.target_wall_time(orig_ts) - wall_shift_sec
            if min_interval_sec > 0 and last_emit_ts is not None:
                deadline = max(deadline, last_emit_ts + min_interval_sec)

            now = time.monotonic()
            wait_sec = deadline - now
            if wait_sec > 0 and args.max_sleep_sec is not None and wait_sec > args.max_sleep_sec:
                wall_shift_sec += wait_sec - args.max_sleep_sec
                wait_sec = args.max_sleep_sec
                deadline = now + wait_sec

            if wait_sec > 0:
                # Wait in up to 5s slices so progress logs and SIGINT stay responsive
                while wait_sec > 0 and not stop_event.is_set():
                    slice_sec = min(wait_sec, 5.0)
                    stop_event.wait(timeout=slice_sec)
                    if producer is not None:
                        producer.poll(0)
                    now = time.monotonic()
                    wait_sec = deadline - now
                    if now - last_log_ts >= 5.0 and not stop_event.is_set() and wait_sec > 0:
                        elapsed = now - t_start
                        rate = stats.produced / elapsed if elapsed > 0 else 0.0
                        print(
                            f"[PROGRESS] sent={stats.produced} | acked={stats.acked} | "
                            f"rate={rate:.1f} msg/s | waiting next event in {wait_sec:.1f}s",
                            flush=True,
                        )
                        last_log_ts = now

                if stop_event.is_set():
                    break

            behind_ms = max(0.0, (time.monotonic() - deadline) * 1000.0)
            if behind_ms > stats.max_behind_ms:
                stats.max_behind_ms = behind_ms

            sim_event_time = mapper.map_sim_time(orig_ts)
            payload = build_event_payload(row, mapper)
            raw_json = json.dumps(payload, separators=(",", ":"))

            if prev_event_time is not None and sim_event_time < prev_event_time:
                non_monotonic_count += 1
            prev_event_time = sim_event_time

            # Local parity safeguard in --dry-run (or --no-faults)
            if args.dry_run:
                res = validate_event(raw_json)
                if not res.is_valid:
                    contract_failures += 1
                    print(f"[CONTRACT ERROR] row={payload['source_row_id']}: {res.errors}")
                stats.produced += 1
                stats.acked += 1
                if stats.produced <= 3:
                    print(f"[DRY-RUN SAMPLE #{stats.produced}] key={payload['campaign_id']} {raw_json}")
            else:
                assert producer is not None
                _produce_with_backpressure(
                    producer=producer,
                    topic=topic,
                    key=payload["campaign_id"],
                    value=raw_json,
                    stats=stats,
                )
                producer.poll(0)

            last_emit_ts = time.monotonic()
            if last_emit_ts - last_log_ts >= 5.0:
                elapsed = last_emit_ts - t_start
                rate = stats.produced / elapsed if elapsed > 0 else 0.0
                print(
                    f"[PROGRESS] sent={stats.produced} | acked={stats.acked} | "
                    f"rate={rate:.1f} msg/s | behind_schedule_ms={behind_ms:.1f}",
                    flush=True,
                )
                last_log_ts = last_emit_ts
    except KafkaException as exc:
        print(f"❌ Kafka error: {exc}", file=sys.stderr)
        return 1
    finally:
        signal.signal(signal.SIGINT, prev_sigint)
        if producer is not None:
            remaining = producer.flush(timeout=10.0)
            if remaining > 0:
                print(f"⚠️  {remaining} messages still unacknowledged after flush timeout!")

    total_elapsed = time.monotonic() - t_start
    avg_rate = stats.produced / total_elapsed if total_elapsed > 0 else 0.0
    print("\n" + "=" * 84)
    print("📊 TỔNG KẾT REPLAY PRODUCER v1")
    print(f"   Trạng thái       : {'STOPPED BY SIGINT' if stop_event.is_set() else 'COMPLETED'}")
    print(f"   Produced / Acked : {stats.produced} / {stats.acked}")
    print(f"   Delivery errors  : {stats.delivery_errors} | Buffer retries: {stats.buffer_retries}")
    print(f"   Non-monotonic ts : {non_monotonic_count} | Dry-run contract fails: {contract_failures}")
    print(f"   Elapsed / Rate   : {total_elapsed:.2f}s / {avg_rate:.1f} msg/s")
    print(f"   Max behind ms    : {stats.max_behind_ms:.2f} ms")
    print("=" * 84)
    return 1 if (stats.delivery_errors > 0 or contract_failures > 0) else 0


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint."""
    cfg = load()
    args = _parse_args(argv, cfg)
    return run_replay(args, cfg)


if __name__ == "__main__":
    raise SystemExit(main())
