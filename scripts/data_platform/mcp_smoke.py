"""Exercise the real STDIO protocol surface without external services."""

import argparse
import json
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from data_platform.phase9_evidence import Phase9Boundary, execution_environment, write_json


def _exchange(process: subprocess.Popen[str], request: dict[str, Any]) -> dict[str, Any]:
    if process.stdin is None or process.stdout is None:
        raise RuntimeError("MCP child process was created without pipes")
    process.stdin.write(json.dumps(request) + "\n")
    process.stdin.flush()
    line = process.stdout.readline()
    if not line:
        raise RuntimeError("MCP server closed stdout before responding")
    response = json.loads(line)
    if not isinstance(response, dict):
        raise RuntimeError("MCP response is not an object")
    return response


def main() -> None:
    """Initialize, list tools, and query orders through a child STDIO server."""
    parser = argparse.ArgumentParser(description="Smoke test Phase 9 MCP over STDIO")
    parser.add_argument("--repository-root", type=Path, default=Path.cwd())
    parser.add_argument("--report", type=Path)
    arguments = parser.parse_args()
    command = [
        sys.executable,
        str(Path(__file__).with_name("run_mcp_server.py")),
        "--repository-root",
        str(arguments.repository_root),
    ]
    process = subprocess.Popen(  # noqa: S603 - fixed local Python script, no shell
        command,
        cwd=arguments.repository_root,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        initialized = _exchange(
            process,
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": "2025-11-25",
                    "capabilities": {},
                    "clientInfo": {"name": "phase9-smoke", "version": "1"},
                },
            },
        )
        listed = _exchange(process, {"jsonrpc": "2.0", "id": 2, "method": "tools/list"})
        called = _exchange(
            process,
            {
                "jsonrpc": "2.0",
                "id": 3,
                "method": "tools/call",
                "params": {"name": "search_data_assets", "arguments": {"query": "orders"}},
            },
        )
        tools = listed.get("result", {}).get("tools", [])
        status = called.get("result", {}).get("structuredContent", {}).get("status")
        if initialized.get("result", {}).get("serverInfo", {}).get("name") is None:
            raise RuntimeError("initialize response is incomplete")
        if len(tools) != 10 or status != "ok":
            raise RuntimeError("tool list or search response failed smoke assertions")
        report = {
            "schema_version": 1,
            "report_type": "phase9_mcp_smoke",
            "status": "passed",
            "evidence_level": "static_validation",
            "generated_at": datetime.now(UTC).isoformat(),
            **Phase9Boundary(execution_environment=execution_environment()).model_dump(mode="json"),
            "tool_count": len(tools),
            "tool_names": sorted(tool["name"] for tool in tools),
            "input_schemas": "validated",
            "output_schemas": "validated",
            "read_only_annotations": "validated",
            "search_status": status,
        }
        if arguments.report is not None:
            write_json(arguments.report, report)
        print(json.dumps(report, indent=2, sort_keys=True))
    finally:
        if process.stdin is not None:
            process.stdin.close()
        process.wait(timeout=10)
        if process.returncode != 0:
            error_output = process.stderr.read()[:500] if process.stderr is not None else ""
            raise RuntimeError(f"MCP server exited {process.returncode}: {error_output}")


if __name__ == "__main__":
    main()
