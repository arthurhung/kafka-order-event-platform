import json
from pathlib import Path
from uuid import uuid4

import pytest

from data_platform.phase8a_validation import run_phase8a_validation


@pytest.mark.data_platform
def test_phase8a_aggregate_uses_fresh_run_specific_artifacts():
    run_id = f"pytest_{uuid4().hex[:10]}"
    result = run_phase8a_validation(repo_root=Path("."), run_id=run_id)
    assert result.status == "passed"
    assert run_id in result.target_directory
    assert run_id in result.report_directory
    summary = json.loads((Path(result.report_directory) / "phase8a-ci-summary.json").read_text())
    assert summary["status"] == "passed"
    assert summary["phase_identifier"] == "phase_8a"
    assert summary["phase_status"] == {
        "phase_8a": "local_implementation_complete",
        "phase_8b": "not_started",
        "phase_8c": "not_started",
    }
    assert summary["gcp_credentials_accessed"] is False
    assert summary["bigquery_runtime_execution"] is False
