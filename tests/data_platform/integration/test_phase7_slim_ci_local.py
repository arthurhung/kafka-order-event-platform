import json
import os
import shutil
import subprocess
from pathlib import Path
from uuid import uuid4

import pytest

from data_platform.slim_ci import CommandResult, cleanup_ci_schemas, select_modified_models
from streaming_platform.config import get_settings


def _run(command: list[str], *, cwd: Path, environment: dict[str, str]) -> str:
    subprocess_environment = {
        key: value for key, value in environment.items() if not key.startswith("COV_CORE_")
    }
    completed = subprocess.run(  # noqa: S603 - test supplies fixed dbt commands
        command,
        cwd=cwd,
        env=subprocess_environment,
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
    return completed.stdout


def _dbt(
    action: list[str], project: Path, profiles: Path, target: Path, extra: list[str] | None = None
) -> list[str]:
    command = [
        "dbt",
        *action,
        "--project-dir",
        str(project),
        "--profiles-dir",
        str(profiles),
        "--target",
        "ci",
    ]
    if action != ["deps"]:
        command.extend(("--target-path", str(target)))
    command.extend(extra or [])
    return command


@pytest.mark.data_platform
def test_modified_staging_selects_downstream_and_builds_with_defer(tmp_path: Path):
    run_id = f"pytest_{uuid4().hex[:8]}"
    base_schema = f"analytics_ci_base_{run_id}"
    current_schema = f"analytics_ci_current_{run_id}"
    base_project = tmp_path / "base" / "dbt"
    current_project = tmp_path / "current" / "dbt"
    profiles = tmp_path / "profiles"
    ignore = shutil.ignore_patterns("target", "logs", "dbt_packages", "profiles.yml", ".user.yml")
    shutil.copytree("dbt", base_project, ignore=ignore)
    shutil.copytree("dbt", current_project, ignore=ignore)
    profiles.mkdir()
    shutil.copy2("dbt/profiles.yml", profiles / "profiles.yml")
    base_target = tmp_path / "state" / "base"
    current_target = tmp_path / "state" / "current"
    settings = get_settings()
    environment = os.environ.copy()
    environment.update(
        {
            "POSTGRES_HOST": settings.POSTGRES_HOST,
            "POSTGRES_PORT": str(settings.POSTGRES_PORT),
            "POSTGRES_DB": settings.POSTGRES_DB,
            "POSTGRES_USER": settings.POSTGRES_USER,
            "POSTGRES_PASSWORD": settings.POSTGRES_PASSWORD.get_secret_value(),
        }
    )

    try:
        base_environment = environment | {"DBT_TARGET": "ci", "DBT_TARGET_SCHEMA": base_schema}
        _run(
            _dbt(["deps"], base_project, profiles, base_target),
            cwd=tmp_path,
            environment=base_environment,
        )
        _run(
            _dbt(["build"], base_project, profiles, base_target),
            cwd=tmp_path,
            environment=base_environment,
        )

        staging_sql = current_project / "models" / "staging" / "stg_order_events.sql"
        staging_sql.write_text(staging_sql.read_text() + "\n-- Phase 7 state selection scenario\n")
        current_environment = environment | {
            "DBT_TARGET": "ci",
            "DBT_TARGET_SCHEMA": current_schema,
        }
        _run(
            _dbt(["parse"], current_project, profiles, current_target),
            cwd=tmp_path,
            environment=current_environment,
        )
        evidence: list[CommandResult] = []
        selected = select_modified_models(
            project_dir=current_project,
            profiles_dir=profiles,
            target_path=current_target,
            state_path=base_target,
            repo_root=tmp_path,
            environment=current_environment,
            evidence=evidence,
        )
        expected = {
            "stg_order_events",
            "int_order_event_sequence",
            "int_order_latest_state",
            "fct_order_events",
            "fct_orders",
            "mart_daily_sales",
        }
        assert expected <= set(selected)
        assert "stg_log_metrics_minute" not in selected
        assert "mart_service_health" not in selected
        build_command = _dbt(
            ["build"],
            current_project,
            profiles,
            current_target,
            ["--select", "state:modified+", "--defer", "--state", str(base_target)],
        )
        _run(build_command, cwd=tmp_path, environment=current_environment)

        evidence_path = Path(
            os.environ.get("PHASE7_SELECTION_EVIDENCE", tmp_path / "selection.json")
        )
        evidence_path.parent.mkdir(parents=True, exist_ok=True)
        evidence_path.write_text(
            json.dumps(
                {
                    "report_type": "phase7_modified_staging_selection",
                    "schema_version": 1,
                    "mode": "state_modified_plus",
                    "modified_model": "stg_order_events",
                    "selected_models": selected,
                    "defer": True,
                    "state_manifest": str(base_target / "manifest.json"),
                    "status": "passed",
                },
                indent=2,
                sort_keys=True,
            )
            + "\n"
        )
    finally:
        cleanup_ci_schemas(environment, (base_schema, current_schema))
