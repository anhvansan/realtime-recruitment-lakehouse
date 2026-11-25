#!/usr/bin/env python3
"""Partition Key Skew Analysis for Kafka/Redpanda topic cpc_events_raw (Task 2.3).

Evaluates candidate partition keys ('campaign_id', 'job_id', 'publisher_id') on the
exact Event Stream dataset selected in Task 2.1:
- 1,875 clean CPC click events from tracking.csv
- 102 funnel events from application.csv mapped to campaign_id per data_dictionary.md

Computes per-key cardinality, Top-1 key share, and 3-partition distribution using
both DuckDB `hash(key) % 3` and `zlib.crc32(key) % 3` (matching librdkafka).
"""

from __future__ import annotations

import sys
import zlib
from pathlib import Path
from typing import Any

import duckdb

# Ensure project root is importable when executed directly
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

COMBINED_STREAM_CTE = """
WITH clean_clicks AS (
    SELECT
        CAST(campaign_id AS VARCHAR) AS campaign_id,
        CAST(job_id AS VARCHAR) AS job_id,
        CAST(publisher_id AS VARCHAR) AS publisher_id,
        'tracking' AS source_file
    FROM read_csv_auto('data/raw/tracking.csv')
    WHERE campaign_id IS NOT NULL
      AND job_id IS NOT NULL
      AND publisher_id IS NOT NULL
      AND bid IS NOT NULL
),
job_to_camp AS (
    SELECT job_id, MIN(campaign_id) AS campaign_id
    FROM read_csv_auto('data/raw/tracking.csv')
    WHERE job_id IS NOT NULL AND campaign_id IS NOT NULL
    GROUP BY job_id
),
company_to_camp AS (
    SELECT company_id, MAX(id) AS campaign_id
    FROM read_csv_auto('data/raw/campaign.csv')
    WHERE company_id IS NOT NULL
    GROUP BY company_id
),
clean_apps AS (
    SELECT
        CAST(COALESCE(jtc.campaign_id, ctc.campaign_id, -1) AS VARCHAR) AS campaign_id,
        CAST(a.job_id AS VARCHAR) AS job_id,
        CAST(COALESCE(a.publisher_id, -1) AS VARCHAR) AS publisher_id,
        'application' AS source_file
    FROM read_csv_auto('data/raw/application.csv') a
    LEFT JOIN job_to_camp jtc ON a.job_id = jtc.job_id
    LEFT JOIN read_csv_auto('data/raw/job.csv') j ON a.job_id = j.id
    LEFT JOIN company_to_camp ctc ON j.company_id = ctc.company_id
),
combined_stream AS (
    SELECT * FROM clean_clicks
    UNION ALL
    SELECT * FROM clean_apps
)
"""

CANDIDATES: tuple[str, ...] = ("campaign_id", "job_id", "publisher_id")


def analyze_candidates(num_partitions: int = 3) -> list[dict[str, Any]]:
    """Run DuckDB + CRC32 partition distribution analysis on data/raw/."""
    con = duckdb.connect(":memory:")
    try:
        rows = con.execute(
            f"{COMBINED_STREAM_CTE} "
            "SELECT campaign_id, job_id, publisher_id, source_file FROM combined_stream"
        ).fetchall()
        total = len(rows)
        n_tracking = sum(1 for r in rows if r[3] == "tracking")
        n_apps = sum(1 for r in rows if r[3] == "application")
        null_pub_apps = con.execute(
            "SELECT COUNT(*) FROM read_csv_auto('data/raw/application.csv') "
            "WHERE publisher_id IS NULL"
        ).fetchone()[0]

        print("=" * 84)
        print(
            f"📊 PHÂN TÍCH LỆCH KHOÁ PARTITION (N={total:,}: "
            f"{n_tracking:,} tracking clicks + {n_apps:,} application events)"
        )
        print(f"   Lưu ý dữ liệu thật: application.csv có {null_pub_apps} dòng publisher_id NULL")
        print("=" * 84)

        results: list[dict[str, Any]] = []
        for idx, col in enumerate(CANDIDATES):
            distinct_cnt = con.execute(
                f"{COMBINED_STREAM_CTE} SELECT COUNT(DISTINCT {col}) FROM combined_stream"
            ).fetchone()[0]

            duck_parts_rows = con.execute(
                f"""
                {COMBINED_STREAM_CTE}
                SELECT
                    CAST(hash({col}) % {num_partitions} AS INT) AS p,
                    COUNT(*) AS cnt,
                    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct
                FROM combined_stream
                GROUP BY 1
                ORDER BY 1
                """
            ).fetchall()
            duck_map = {int(r[0]): (int(r[1]), float(r[2])) for r in duck_parts_rows}
            duck_pcts = [duck_map.get(p, (0, 0.0))[1] for p in range(num_partitions)]
            duck_cnts = [duck_map.get(p, (0, 0.0))[0] for p in range(num_partitions)]

            crc_cnts = [0] * num_partitions
            for r in rows:
                p = (zlib.crc32(str(r[idx]).encode("utf-8")) & 0xFFFFFFFF) % num_partitions
                crc_cnts[p] += 1
            crc_pcts = [round(c * 100.0 / total, 2) for c in crc_cnts]

            top_rows = con.execute(
                f"""
                {COMBINED_STREAM_CTE}
                SELECT
                    {col},
                    COUNT(*) AS cnt,
                    ROUND(COUNT(*) * 100.0 / {total}, 2) AS pct
                FROM combined_stream
                GROUP BY 1
                ORDER BY cnt DESC
                LIMIT 3
                """
            ).fetchall()
            top1_key, top1_cnt, top1_pct = top_rows[0][0], int(top_rows[0][1]), float(top_rows[0][2])

            entry = {
                "candidate": col,
                "distinct": int(distinct_cnt),
                "top1_key": str(top1_key),
                "top1_cnt": top1_cnt,
                "top1_pct": top1_pct,
                "duck_cnts": duck_cnts,
                "duck_pcts": duck_pcts,
                "duck_max_pct": max(duck_pcts),
                "crc_cnts": crc_cnts,
                "crc_pcts": crc_pcts,
                "crc_max_pct": max(crc_pcts),
                "top3": [(str(k), int(c), float(p)) for k, c, p in top_rows],
            }
            results.append(entry)

            print(f"\n🔑 Ứng viên: {col} (COUNT(DISTINCT) = {entry['distinct']})")
            print(
                f"   - Top-1 key          : {entry['top1_key']} "
                f"({entry['top1_cnt']}/{total} = {entry['top1_pct']:.2f}%)"
            )
            print(
                f"   - DuckDB hash % 3    : P0={duck_cnts[0]} ({duck_pcts[0]:.2f}%), "
                f"P1={duck_cnts[1]} ({duck_pcts[1]:.2f}%), "
                f"P2={duck_cnts[2]} ({duck_pcts[2]:.2f}%) | "
                f"Max Share = {entry['duck_max_pct']:.2f}%"
            )
            print(
                f"   - CRC32 % 3 (Kafka)  : P0={crc_cnts[0]} ({crc_pcts[0]:.2f}%), "
                f"P1={crc_cnts[1]} ({crc_pcts[1]:.2f}%), "
                f"P2={crc_cnts[2]} ({crc_pcts[2]:.2f}%) | "
                f"Max Share = {entry['crc_max_pct']:.2f}%"
            )
            print(f"   - Top-3 keys         : {entry['top3']}")

        print("\n" + "=" * 84)
        return results
    finally:
        con.close()


def main() -> int:
    """CLI entrypoint."""
    analyze_candidates(num_partitions=3)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
