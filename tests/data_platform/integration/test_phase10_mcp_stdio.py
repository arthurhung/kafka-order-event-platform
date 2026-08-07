import shutil
import subprocess
import sys
from pathlib import Path
from unittest.mock import Mock

import pytest

from data_platform.mcp_client import (
    READ_ONLY_TOOL_NAMES,
    McpClientError,
    RestrictedStdioMcpClient,
)


def _metadata_source_root(root: Path, tmp_path: Path) -> Path:
    if (root / "dbt/target/manifest.json").is_file():
        return root
    slim_manifests = sorted(
        (root / "dbt/target/phase7-ci").glob("*/current/manifest.json")
    )
    assert slim_manifests, "a dbt manifest is required for the Phase 9 transport test"
    isolated_root = tmp_path / "metadata-source"
    shutil.copytree(slim_manifests[-1].parent, isolated_root / "dbt/target")
    return isolated_root


def test_incident_client_uses_real_phase9_stdio_transport(tmp_path: Path) -> None:
    root = Path.cwd()
    metadata_source = _metadata_source_root(root, tmp_path)
    subprocess.run(  # noqa: S603 - fixed repository Python entrypoint
        [
            sys.executable,
            "scripts/data_platform/build_metadata_index.py",
            "--repository-root",
            str(metadata_source),
            "--output-dir",
            str(root / "reports/metadata"),
        ],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    )
    assert (root / "reports/metadata/metadata-index.json").is_file()
    with RestrictedStdioMcpClient(root) as client:
        assert len(client.tool_names) == 10
        result = client.call("search_data_assets", {"query": "orders"})
    assert result["status"] == "ok"
    assert result["data"]["matches"]


def test_client_rejects_duplicate_or_malformed_tool_surface(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    process = Mock()
    process.stdin = Mock()
    process.wait.return_value = 0
    monkeypatch.setattr("data_platform.mcp_client.subprocess.Popen", Mock(return_value=process))
    responses = iter(
        (
            {"serverInfo": {"name": "retail-data-platform-discovery"}},
            {
                "tools": [
                    *({"name": name} for name in sorted(READ_ONLY_TOOL_NAMES)),
                    {"name": sorted(READ_ONLY_TOOL_NAMES)[0]},
                ]
            },
        )
    )
    monkeypatch.setattr(
        RestrictedStdioMcpClient,
        "_request",
        lambda self, method, params: next(responses),
    )
    with pytest.raises(McpClientError, match="malformed or duplicate"):
        with RestrictedStdioMcpClient(Path.cwd()):
            pass
    process.stdin.close.assert_called_once()


def test_client_kills_server_when_graceful_and_terminate_time_out() -> None:
    process = Mock()
    process.stdin = Mock()
    process.wait.side_effect = (
        subprocess.TimeoutExpired("server", 1),
        subprocess.TimeoutExpired("server", 1),
        0,
    )
    client = RestrictedStdioMcpClient(Path.cwd(), timeout_seconds=1)
    client._process = process
    client.__exit__(None, None, None)
    process.terminate.assert_called_once()
    process.kill.assert_called_once()
    assert client._process is None
