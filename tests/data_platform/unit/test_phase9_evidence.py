import json
from pathlib import Path

import pytest

from data_platform.metadata_index import MetadataBuilder, write_build
from data_platform.phase9_evidence import write_json, write_phase9_evidence
from tests.data_platform.unit.test_metadata_index import artifact_fixture


def _reports(root: Path, *, smoke_status: str = "passed") -> None:
    artifact_fixture(root)
    output = root / "reports/metadata"
    write_build(MetadataBuilder(root).build(), output)
    write_json(
        output / "metadata-validation-report.json",
        {"status": "complete", "report_type": "phase9_metadata_validation"},
    )
    write_json(
        output / "mcp-smoke-report.json",
        {"status": smoke_status, "report_type": "phase9_mcp_smoke", "tool_count": 10},
    )
    (output / "mcp-audit.jsonl").write_text(
        json.dumps({"status": "ok", "sanitized_arguments": {}}) + "\n"
    )


def test_phase9_summary_records_restricted_runtime_boundary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _reports(tmp_path)
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    summary_path, security_path = write_phase9_evidence(tmp_path)
    summary = json.loads(summary_path.read_text())
    security = json.loads(security_path.read_text())
    assert summary["status"] == "passed"
    assert summary["execution_environment"] == "github_actions"
    assert summary["mcp_transport"] == "stdio"
    assert summary["mcp_adapter"] == "restricted_local_adapter"
    assert summary["official_mcp_sdk_runtime"] == "not_available"
    assert summary["official_mcp_sdk_install_status"] == "not_installed"
    assert summary["cloud_execution_status"] == "not_executed"
    assert summary["gcp_credentials_accessed"] is False
    assert security["public_listener"] is False
    assert security["checks"]["shell_execution"] == "not_provided"


def test_phase9_summary_rejects_failed_child(tmp_path: Path) -> None:
    _reports(tmp_path, smoke_status="failed")
    with pytest.raises(ValueError, match="failed child validation"):
        write_phase9_evidence(tmp_path)
