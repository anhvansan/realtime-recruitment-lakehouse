"""Unit tests for deterministic dbt seed preparation and dimension contracts (Task 4.2)."""

from __future__ import annotations

import csv
import hashlib
from decimal import Decimal
from pathlib import Path

import pytest

from scripts.prepare_dbt_seeds import prepare_all_seeds

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8", newline="\n") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


@pytest.fixture()
def raw_fixture_dir(tmp_path: Path) -> Path:
    """Create minimal self-contained Group B raw CSVs so tests pass in CI without data/raw/."""
    raw = tmp_path / "raw"
    raw.mkdir(parents=True, exist_ok=True)

    _write_csv(
        raw / "campaign.csv",
        ["id", "campaign_name", "budget_total", "pacing_unit", "company_id", "is_active"],
        [
            {
                "id": "1",
                "campaign_name": "Campaign 121",
                "budget_total": "1002930129",
                "pacing_unit": "Daily",
                "company_id": "1",
                "is_active": "1",
            },
            {
                "id": "4",
                "campaign_name": "Campaign draft",
                "budget_total": "1932570",
                "pacing_unit": "daily",
                "company_id": "1",
                "is_active": "1",
            },
            {
                "id": "15",
                "campaign_name": "",
                "budget_total": "100",
                "pacing_unit": "Daily",
                "company_id": "1",
                "is_active": "1",
            },
            {
                "id": "110",
                "campaign_name": "New Campaign 1111",
                "budget_total": "122222222",
                "pacing_unit": "Weekly",
                "company_id": "1",
                "is_active": "1",
            },
        ],
    )
    _write_csv(
        raw / "job.csv",
        [
            "id",
            "title",
            "company_id",
            "group_id",
            "campaign_id",
            "work_schedule",
            "role_location",
            "budget",
            "status",
            "is_active",
        ],
        [
            {
                "id": "2",
                "title": "PHP Developer",
                "company_id": "1",
                "group_id": "10",
                "campaign_id": "1",
                "work_schedule": "FULL_TIME",
                "role_location": "REMOTE",
                "budget": "50",
                "status": "1",
                "is_active": "1",
            },
            {
                "id": "3",
                "title": "Java\n  Developer",
                "company_id": "1",
                "group_id": "10",
                "campaign_id": "4",
                "work_schedule": "FULL_TIME",
                "role_location": "REMOTE",
                "budget": "",
                "status": "2",
                "is_active": "0",
            },
        ],
    )
    _write_csv(
        raw / "master_publisher.csv",
        [
            "id",
            "publisher_name",
            "email",
            "access_token",
            "publisher_type",
            "min_bid",
            "max_bid",
            "cpc_increment",
            "is_active",
        ],
        [
            {
                "id": "1",
                "publisher_name": "Facebook modify",
                "email": "fb@example.com",
                "access_token": "secret-token-1",
                "publisher_type": "0",
                "min_bid": "1",
                "max_bid": "5",
                "cpc_increment": "0",
                "is_active": "1",
            },
            {
                "id": "3",
                "publisher_name": "Talroo",
                "email": "talroo@example.com",
                "access_token": "secret-token-3",
                "publisher_type": "1",
                "min_bid": "1",
                "max_bid": "10",
                "cpc_increment": "0",
                "is_active": "1",
            },
        ],
    )
    _write_csv(
        raw / "company.csv",
        [
            "id",
            "name",
            "sign_deal_user",
            "logo",
            "billing_id",
            "customer_type",
            "status",
            "is_active",
        ],
        [
            {
                "id": "1",
                "name": "GOTORO",
                "sign_deal_user": "user@example.com",
                "logo": "https://example.com/logo.png",
                "billing_id": "",
                "customer_type": "1",
                "status": "1",
                "is_active": "1",
            }
        ],
    )
    _write_csv(
        raw / "group.csv",
        ["id", "name", "budget_group", "company_id", "campaign_id", "status", "is_active"],
        [
            {
                "id": "10",
                "name": "Group 10",
                "budget_group": "400",
                "company_id": "1",
                "campaign_id": "1",
                "status": "1",
                "is_active": "1",
            }
        ],
    )
    return raw


def test_prepare_all_seeds_is_deterministic(raw_fixture_dir: Path, tmp_path: Path) -> None:
    out_a = tmp_path / "run_a"
    out_b = tmp_path / "run_b"

    counts_a = prepare_all_seeds(raw_fixture_dir, out_a)
    counts_b = prepare_all_seeds(raw_fixture_dir, out_b)

    assert counts_a == counts_b
    assert counts_a == {
        "seed_campaign_budget.csv": 4,
        "seed_job.csv": 2,
        "seed_publisher.csv": 3,  # 2 publishers + 'unknown' fallback
        "seed_company.csv": 1,
        "seed_group.csv": 1,
    }

    for filename in counts_a:
        assert _sha256(out_a / filename) == _sha256(out_b / filename), (
            f"Non-deterministic output for {filename}"
        )


def test_seed_campaign_budget_real_values_outlier_and_weekly_conversion(
    raw_fixture_dir: Path,
    tmp_path: Path,
) -> None:
    out_dir = tmp_path / "seeds"
    prepare_all_seeds(raw_fixture_dir, out_dir)
    with (out_dir / "seed_campaign_budget.csv").open("r", encoding="utf-8", newline="") as fh:
        rows = {r["campaign_id"]: r for r in csv.DictReader(fh)}

    # Campaign 1 outlier (budget ~1B)
    c1 = rows["1"]
    assert c1["daily_budget"] == "1002930129.0000"
    assert c1["pacing_unit"] == "Daily"
    assert c1["is_outlier_budget"] == "true"
    assert c1["budget_source"] == "campaign.csv"

    # Campaign 4 had lowercase 'daily' in raw CSV -> normalized 'Daily'
    c4 = rows["4"]
    assert c4["pacing_unit"] == "Daily"
    assert c4["daily_budget"] == "1932570.0000"
    assert c4["is_outlier_budget"] == "false"

    # Blank campaign_name falls back to "Campaign <id>"
    assert rows["15"]["campaign_name"] == "Campaign 15"

    # Campaign 110 had 'Weekly' -> divided by 7 rounded to DECIMAL(18,4)
    c110 = rows["110"]
    assert c110["pacing_unit"] == "Weekly"
    expected_daily = (Decimal("122222222.0000") / Decimal("7")).quantize(Decimal("0.0001"))
    assert c110["daily_budget"] == f"{expected_daily:.4f}"

    for cid, row in rows.items():
        assert row["campaign_name"].strip() != "", f"Blank campaign_name for {cid}"
        assert Decimal(row["daily_budget"]) > Decimal("0")


def test_seed_publisher_and_company_exclude_pii_and_include_unknown_fallback(
    raw_fixture_dir: Path,
    tmp_path: Path,
) -> None:
    out_dir = tmp_path / "seeds"
    prepare_all_seeds(raw_fixture_dir, out_dir)

    with (out_dir / "seed_publisher.csv").open("r", encoding="utf-8", newline="") as fh:
        pub_reader = csv.DictReader(fh)
        pub_fields = pub_reader.fieldnames or []
        pub_rows = {r["publisher_id"]: r for r in pub_reader}

    assert "email" not in pub_fields
    assert "access_token" not in pub_fields
    assert "unknown" in pub_rows
    assert pub_rows["unknown"]["publisher_name"] == "Unknown Publisher"
    assert pub_rows["1"]["publisher_name"] == "Facebook modify"
    assert pub_rows["3"]["publisher_name"] == "Talroo"

    with (out_dir / "seed_company.csv").open("r", encoding="utf-8", newline="") as fh:
        comp_reader = csv.DictReader(fh)
        comp_fields = comp_reader.fieldnames or []

    assert "sign_deal_user" not in comp_fields
    assert "logo" not in comp_fields


@pytest.mark.skipif(
    not (RAW_DIR / "campaign.csv").exists(),
    reason="data/raw/ is gitignored and absent in CI",
)
def test_prepare_all_seeds_on_local_raw_csvs_when_present(tmp_path: Path) -> None:
    counts = prepare_all_seeds(RAW_DIR, tmp_path / "local_seeds")
    assert counts == {
        "seed_campaign_budget.csv": 193,
        "seed_job.csv": 1868,
        "seed_publisher.csv": 30,
        "seed_company.csv": 105,
        "seed_group.csv": 43,
    }
