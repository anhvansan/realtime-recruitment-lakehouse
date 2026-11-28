#!/usr/bin/env python3
"""Prepare deterministic dbt seed CSVs from selected raw dimension CSVs (Task 4.2).

Reads the 5 approved Group B dimension files from ``data/raw/``:
  - ``campaign.csv``        -> ``seed_campaign_budget.csv``
  - ``job.csv``             -> ``seed_job.csv``
  - ``master_publisher.csv``-> ``seed_publisher.csv`` (excludes PII/tokens; adds 'unknown' fallback)
  - ``company.csv``         -> ``seed_company.csv`` (excludes PII/logos)
  - ``group.csv``           -> ``seed_group.csv``

Enforces Rule R1 (real campaign.csv budgets, no random generation), Rule R6 (no PII/secrets),
Rule R7 (DECIMAL(18,4) formatting), and deterministic row ordering for reproducible runs.
"""

from __future__ import annotations

import argparse
import csv
import sys
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from pathlib import Path

_DEC_QUANT = Decimal("0.0001")
_OUTLIER_BUDGET_THRESHOLD = Decimal("100000000.0000")
_DAYS_PER_WEEK = Decimal("7")


def _clean_str(value: str | None) -> str:
    """Collapse internal newlines/tabs and strip surrounding whitespace."""
    if value is None:
        return ""
    return " ".join(value.split())


def _format_decimal_or_empty(value: str | None) -> str:
    """Format numeric string as DECIMAL(18,4) or return empty string when blank."""
    cleaned = _clean_str(value)
    if not cleaned:
        return ""
    try:
        dec = Decimal(cleaned).quantize(_DEC_QUANT, rounding=ROUND_HALF_UP)
    except InvalidOperation as exc:
        raise ValueError(f"Invalid decimal value {value!r}") from exc
    return f"{dec:.4f}"


def _sort_key_id(row: dict[str, str], id_col: str) -> tuple[int, int, str]:
    """Sort numeric IDs numerically first, followed by non-numeric fallback IDs."""
    raw = row[id_col]
    if raw.isdigit():
        return (0, int(raw), raw)
    return (1, 0, raw)


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        raise FileNotFoundError(f"Missing raw dimension CSV: {path}")
    with path.open("r", encoding="utf-8", newline="") as fh:
        return list(csv.DictReader(fh))


def _write_csv_rows(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=fieldnames,
            lineterminator="\n",
            quoting=csv.QUOTE_MINIMAL,
        )
        writer.writeheader()
        writer.writerows(rows)
    return len(rows)


def build_campaign_budget_rows(raw_dir: Path) -> list[dict[str, str]]:
    """Transform ``campaign.csv`` into normalized ``seed_campaign_budget`` rows."""
    out: list[dict[str, str]] = []
    for row in _read_csv_rows(raw_dir / "campaign.csv"):
        cid = _clean_str(row.get("id"))
        if not cid:
            continue
        raw_name = _clean_str(row.get("campaign_name"))
        campaign_name = raw_name if raw_name else f"Campaign {cid}"

        raw_pacing = _clean_str(row.get("pacing_unit")).lower()
        pacing_unit = "Weekly" if raw_pacing == "weekly" else "Daily"

        budget_total_str = _format_decimal_or_empty(row.get("budget_total"))
        if not budget_total_str:
            raise ValueError(f"campaign_id={cid} has missing budget_total")
        budget_total_dec = Decimal(budget_total_str)

        if pacing_unit == "Weekly":
            daily_dec = (budget_total_dec / _DAYS_PER_WEEK).quantize(
                _DEC_QUANT, rounding=ROUND_HALF_UP
            )
        else:
            daily_dec = budget_total_dec

        is_outlier = "true" if daily_dec >= _OUTLIER_BUDGET_THRESHOLD else "false"
        out.append(
            {
                "campaign_id": cid,
                "campaign_name": campaign_name,
                "company_id": _clean_str(row.get("company_id")),
                "pacing_unit": pacing_unit,
                "budget_total": f"{budget_total_dec:.4f}",
                "daily_budget": f"{daily_dec:.4f}",
                "is_active": _clean_str(row.get("is_active")),
                "is_outlier_budget": is_outlier,
                "budget_source": "campaign.csv",
            }
        )
    out.sort(key=lambda r: _sort_key_id(r, "campaign_id"))
    return out


def build_job_rows(raw_dir: Path) -> list[dict[str, str]]:
    """Transform ``job.csv`` into normalized ``seed_job`` rows."""
    out: list[dict[str, str]] = []
    for row in _read_csv_rows(raw_dir / "job.csv"):
        jid = _clean_str(row.get("id"))
        if not jid:
            continue
        raw_title = _clean_str(row.get("title"))
        out.append(
            {
                "job_id": jid,
                "job_title": raw_title if raw_title else f"Job {jid}",
                "company_id": _clean_str(row.get("company_id")),
                "group_id": _clean_str(row.get("group_id")),
                "campaign_id": _clean_str(row.get("campaign_id")),
                "work_schedule": _clean_str(row.get("work_schedule")),
                "role_location": _clean_str(row.get("role_location")),
                "job_budget": _format_decimal_or_empty(row.get("budget")),
                "status": _clean_str(row.get("status")),
                "is_active": _clean_str(row.get("is_active")),
            }
        )
    out.sort(key=lambda r: _sort_key_id(r, "job_id"))
    return out


def build_publisher_rows(raw_dir: Path) -> list[dict[str, str]]:
    """Transform ``master_publisher.csv`` excluding PII/tokens and adding ``'unknown'``."""
    out: list[dict[str, str]] = []
    seen_ids: set[str] = set()
    for row in _read_csv_rows(raw_dir / "master_publisher.csv"):
        pid = _clean_str(row.get("id"))
        if not pid:
            continue
        seen_ids.add(pid)
        raw_name = _clean_str(row.get("publisher_name"))
        out.append(
            {
                "publisher_id": pid,
                "publisher_name": raw_name if raw_name else f"Publisher {pid}",
                "publisher_type": _clean_str(row.get("publisher_type")),
                "min_bid": _format_decimal_or_empty(row.get("min_bid")),
                "max_bid": _format_decimal_or_empty(row.get("max_bid")),
                "cpc_increment": _clean_str(row.get("cpc_increment")),
                "is_active": _clean_str(row.get("is_active")),
            }
        )

    if "unknown" not in seen_ids:
        out.append(
            {
                "publisher_id": "unknown",
                "publisher_name": "Unknown Publisher",
                "publisher_type": "",
                "min_bid": "",
                "max_bid": "",
                "cpc_increment": "",
                "is_active": "0",
            }
        )

    out.sort(key=lambda r: _sort_key_id(r, "publisher_id"))
    return out


def build_company_rows(raw_dir: Path) -> list[dict[str, str]]:
    """Transform ``company.csv`` into normalized ``seed_company`` rows without PII."""
    out: list[dict[str, str]] = []
    for row in _read_csv_rows(raw_dir / "company.csv"):
        cid = _clean_str(row.get("id"))
        if not cid:
            continue
        raw_name = _clean_str(row.get("name"))
        out.append(
            {
                "company_id": cid,
                "company_name": raw_name if raw_name else f"Company {cid}",
                "billing_id": _clean_str(row.get("billing_id")),
                "customer_type": _clean_str(row.get("customer_type")),
                "status": _clean_str(row.get("status")),
                "is_active": _clean_str(row.get("is_active")),
            }
        )
    out.sort(key=lambda r: _sort_key_id(r, "company_id"))
    return out


def build_group_rows(raw_dir: Path) -> list[dict[str, str]]:
    """Transform ``group.csv`` into normalized ``seed_group`` rows."""
    out: list[dict[str, str]] = []
    for row in _read_csv_rows(raw_dir / "group.csv"):
        gid = _clean_str(row.get("id"))
        if not gid:
            continue
        raw_name = _clean_str(row.get("name"))
        out.append(
            {
                "group_id": gid,
                "group_name": raw_name if raw_name else f"Group {gid}",
                "budget_group": _format_decimal_or_empty(row.get("budget_group")),
                "company_id": _clean_str(row.get("company_id")),
                "campaign_id": _clean_str(row.get("campaign_id")),
                "status": _clean_str(row.get("status")),
                "is_active": _clean_str(row.get("is_active")),
            }
        )
    out.sort(key=lambda r: _sort_key_id(r, "group_id"))
    return out


def prepare_all_seeds(raw_dir: Path, out_dir: Path) -> dict[str, int]:
    """Generate all 5 normalized dbt seed CSVs deterministically and return row counts."""
    specs: list[tuple[str, list[str], list[dict[str, str]]]] = [
        (
            "seed_campaign_budget.csv",
            [
                "campaign_id",
                "campaign_name",
                "company_id",
                "pacing_unit",
                "budget_total",
                "daily_budget",
                "is_active",
                "is_outlier_budget",
                "budget_source",
            ],
            build_campaign_budget_rows(raw_dir),
        ),
        (
            "seed_job.csv",
            [
                "job_id",
                "job_title",
                "company_id",
                "group_id",
                "campaign_id",
                "work_schedule",
                "role_location",
                "job_budget",
                "status",
                "is_active",
            ],
            build_job_rows(raw_dir),
        ),
        (
            "seed_publisher.csv",
            [
                "publisher_id",
                "publisher_name",
                "publisher_type",
                "min_bid",
                "max_bid",
                "cpc_increment",
                "is_active",
            ],
            build_publisher_rows(raw_dir),
        ),
        (
            "seed_company.csv",
            [
                "company_id",
                "company_name",
                "billing_id",
                "customer_type",
                "status",
                "is_active",
            ],
            build_company_rows(raw_dir),
        ),
        (
            "seed_group.csv",
            [
                "group_id",
                "group_name",
                "budget_group",
                "company_id",
                "campaign_id",
                "status",
                "is_active",
            ],
            build_group_rows(raw_dir),
        ),
    ]

    counts: dict[str, int] = {}
    for filename, fieldnames, rows in specs:
        counts[filename] = _write_csv_rows(out_dir / filename, fieldnames, rows)
    return counts


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    project_root = Path(__file__).resolve().parent.parent
    parser = argparse.ArgumentParser(
        description="Normalize raw Group B CSVs into deterministic dbt_cpc/seeds/ files."
    )
    parser.add_argument(
        "--raw-dir",
        type=Path,
        default=project_root / "data" / "raw",
        help="Input directory containing raw CSVs (default: data/raw).",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=project_root / "dbt_cpc" / "seeds",
        help="Output directory for dbt seed CSVs (default: dbt_cpc/seeds).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint for deterministic dbt seed preparation."""
    args = _parse_args(argv)
    counts = prepare_all_seeds(args.raw_dir, args.out_dir)
    for name, count in counts.items():
        print(f"Wrote {args.out_dir / name} ({count} rows)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
