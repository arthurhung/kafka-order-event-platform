import subprocess
import sys
from pathlib import Path

import pytest

from data_platform.scaffolding import (
    ScaffoldError,
    ScaffoldRequest,
    normalize_model_name,
    scaffold_model,
)


@pytest.mark.parametrize(
    ("name", "layer", "expected"),
    [
        ("orders", "staging", "stg_orders"),
        ("order_state", "intermediate", "int_order_state"),
        ("daily_customer_orders", "marts", "mart_daily_customer_orders"),
        ("mart_daily_sales", "marts", "mart_daily_sales"),
    ],
)
def test_normalize_model_name(name, layer, expected):
    assert normalize_model_name(name, layer) == expected


def test_conflicting_prefix_is_rejected():
    with pytest.raises(ScaffoldError, match="conflicts"):
        normalize_model_name("stg_daily_sales", "marts")


def test_scaffold_generates_explicit_draft_without_guessing(tmp_path: Path):
    result = scaffold_model(
        ScaffoldRequest(
            name="daily_customer_orders",
            layer="marts",
            owner="data-platform",
            domain="commerce",
            grain="one row per event_date and user_id",
        ),
        project_dir=tmp_path / "dbt",
    )

    assert result.model_name == "mart_daily_customer_orders"
    assert result.status == "draft"
    assert "BLOCKING_TODO" in result.sql_path.read_text()
    yaml = result.yaml_path.read_text()
    assert "scaffold_status: draft" in yaml
    assert "owner: data-platform" in yaml
    assert "one row per event_date and user_id" in yaml
    assert "select *" not in result.sql_path.read_text().lower()


@pytest.mark.parametrize("existing_suffix", [".sql", ".yml"])
def test_scaffold_refuses_any_existing_target_without_changes(tmp_path: Path, existing_suffix: str):
    model_dir = tmp_path / "dbt" / "models" / "marts"
    model_dir.mkdir(parents=True)
    existing = model_dir / f"mart_daily_customer_orders{existing_suffix}"
    existing.write_text("user-owned\n")
    request = ScaffoldRequest(
        name="daily_customer_orders",
        layer="marts",
        owner="data-platform",
        domain="commerce",
        grain="one row per event_date and user_id",
    )

    with pytest.raises(ScaffoldError, match="refusing to overwrite"):
        scaffold_model(request, project_dir=tmp_path / "dbt")

    assert existing.read_text() == "user-owned\n"
    sibling_suffix = ".yml" if existing_suffix == ".sql" else ".sql"
    assert not (model_dir / f"mart_daily_customer_orders{sibling_suffix}").exists()


def test_scaffold_cli_reports_normalized_name(tmp_path: Path):
    command = [
        sys.executable,
        "scripts/data_platform/scaffold_dbt_model.py",
        "--name",
        "daily_customer_orders",
        "--layer",
        "marts",
        "--owner",
        "data-platform",
        "--domain",
        "commerce",
        "--grain",
        "one row per event_date and user_id",
        "--project-dir",
        str(tmp_path / "dbt"),
    ]
    completed = subprocess.run(  # noqa: S603 - test uses the current interpreter and fixed script
        command, check=False, capture_output=True, text=True
    )

    assert completed.returncode == 0
    assert '"model_name": "mart_daily_customer_orders"' in completed.stdout
    assert '"status": "draft"' in completed.stdout
