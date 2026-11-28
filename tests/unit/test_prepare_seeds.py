"""Unit tests for deterministic dbt seed preparation and dimension contracts (Task 4.2)."""

from __future__ import annotations

import csv
import hashlib
from decimal import Decimal
from pathlib import Path

from scripts.prepare_dbt_seeds import prepare_all_seeds

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_prepare_all_seeds_is_deterministic_on_real_raw_csvs(tmp_path: Path) -> None:
    out_a = tmp_path / "run_a"
    out_b = tmp_path / "run_b"

    counts_a = prepare_all_seeds(RAW_DIR, out_a)
    counts_b = prepare_all_seeds(RAW_DIR, out_b)

    assert counts_a == counts_b
    assert counts_a == {
        "seed_campaign_budget.csv": 193,
        "seed_job.csv": 1868,
        "seed_publisher.csv": 30,  # 29 real publishers + 'unknown' fallback
        "seed_company.csv": 105,
        "seed_group.csv": 43,
    }

    for filename in counts_a:
        assert _sha256(out_a / filename) == _sha256(out_b / filename), (
            f"Non-deterministic output for {filename}"
        )


def test_seed_campaign_budget_real_values_outlier_and_weekly_conversion(tmp_path: Path) -> None:
    prepare_all_seeds(RAW_DIR, tmp_path)
    with (tmp_path / "seed_campaign_budget.csv").open("r", encoding="utf-8", newline="") as fh:
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

    # Campaign 110 had 'Weekly' -> divided by 7 rounded to DECIMAL(18,4)
    c110 = rows["110"]
    assert c110["pacing_unit"] == "Weekly"
    expected_daily = (Decimal("122222222.0000") / Decimal("7")).quantize(Decimal("0.0001"))
    assert c110["daily_budget"] == f"{expected_daily:.4f}"

    # Every row has non-empty campaign_name and positive DECIMAL(18,4) daily_budget
    for cid, row in rows.items():
        assert row["campaign_name"].strip() != "", f"Blank campaign_name for {cid}"
        assert Decimal(row["daily_budget"]) > Decimal("0")


def test_seed_publisher_and_company_exclude_pii_and_include_unknown_fallback(
    tmp_path: Path,
) -> None:
    prepare_all_seeds(RAW_DIR, tmp_path)

    with (tmp_path / "seed_publisher.csv").open("r", encoding="utf-8", newline="") as fh:
        pub_reader = csv.DictReader(fh)
        pub_fields = pub_reader.fieldnames or []
        pub_rows = {r["publisher_id"]: r for r in pub_reader}

    assert "email" not in pub_fields
    assert "access_token" not in pub_fields
    assert "unknown" in pub_rows
    assert pub_rows["unknown"]["publisher_name"] == "Unknown Publisher"
    assert pub_rows["1"]["publisher_name"] == "Facebook modify"
    assert pub_rows["3"]["publisher_name"] == "Talroo"

    with (tmp_path / "seed_company.csv").open("r", encoding="utf-8", newline="") as fh:
        comp_reader = csv.DictReader(fh)
        comp_fields = comp_reader.fieldnames or []

    assert "sign_deal_user" not in comp_fields
    assert "logo" not in comp_fields
