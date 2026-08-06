import json
from pathlib import Path

import pytest

from data_platform.slim_ci import SlimCIError, _selected_models, existing_artifacts


def test_selected_models_reads_only_model_json_lines():
    output = "\n".join(
        [
            "16:00:00 dbt log line",
            json.dumps({"name": "stg_orders", "resource_type": "model"}),
            json.dumps({"name": "source_test", "resource_type": "test"}),
            json.dumps({"name": "mart_orders", "resource_type": "model"}),
        ]
    )

    assert _selected_models(output) == ("mart_orders", "stg_orders")


def test_existing_artifacts_lists_only_files_that_exist(tmp_path: Path):
    base = tmp_path / "base"
    current = tmp_path / "current"
    base.mkdir()
    current.mkdir()
    (base / "manifest.json").write_text("{}")
    (current / "run_results.json").write_text("{}")

    assert existing_artifacts(tmp_path) == (
        str(base / "manifest.json"),
        str(current / "run_results.json"),
    )


def test_unsafe_run_id_is_rejected(tmp_path: Path):
    from data_platform.slim_ci import run_slim_ci

    with pytest.raises(SlimCIError, match="run_id"):
        run_slim_ci(
            repo_root=tmp_path,
            base_ref=None,
            run_id="../unsafe",
            state_root=tmp_path,
            summary_path=tmp_path / "summary.json",
            convention_report_path=tmp_path / "conventions.json",
            contract_report_path=tmp_path / "contracts.json",
        )
