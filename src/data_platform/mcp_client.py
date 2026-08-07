"""Typed client for the repository's restricted local STDIO MCP adapter."""

from __future__ import annotations

import json
import selectors
import subprocess
import sys
from pathlib import Path
from typing import Any


class McpClientError(RuntimeError):
    """Raised when the local MCP transport or protocol fails."""


READ_ONLY_TOOL_NAMES = frozenset(
    {
        "search_data_assets",
        "get_model_schema",
        "get_model_owner",
        "get_lineage",
        "get_upstream_lineage",
        "get_downstream_impact",
        "get_quality_status",
        "get_recent_pipeline_failures",
        "get_consumer_lag",
        "get_cost_estimate",
    }
)


class RestrictedStdioMcpClient:
    """Call only named Phase 9 tools over the existing local STDIO transport."""

    def __init__(self, repository_root: Path, *, timeout_seconds: float = 10.0) -> None:
        """Configure the fixed repository server command and bounded timeout."""
        root = repository_root.resolve()
        server = root / "scripts/data_platform/run_mcp_server.py"
        if not server.is_file():
            raise McpClientError("Phase 9 STDIO server entrypoint is unavailable")
        if timeout_seconds <= 0 or timeout_seconds > 60:
            raise McpClientError("timeout must be greater than zero and at most 60 seconds")
        self.repository_root = root
        self.timeout_seconds = timeout_seconds
        self.command = (sys.executable, str(server), "--repository-root", str(root))
        self._next_id = 1
        self._process: subprocess.Popen[str] | None = None
        self.tool_names: frozenset[str] = frozenset()

    def __enter__(self) -> RestrictedStdioMcpClient:
        """Start and initialize the child STDIO server."""
        self._process = subprocess.Popen(  # noqa: S603 - fixed local Python entrypoint
            self.command,
            cwd=self.repository_root,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        try:
            initialized = self._request(
                "initialize",
                {
                    "protocolVersion": "2025-11-25",
                    "capabilities": {},
                    "clientInfo": {"name": "phase10-agent", "version": "1"},
                },
            )
            if initialized.get("serverInfo", {}).get("name") != ("retail-data-platform-discovery"):
                raise McpClientError("unexpected MCP server identity")
            listing = self._request("tools/list", {})
            tools = listing.get("tools")
            if not isinstance(tools, list):
                raise McpClientError("MCP tool list is malformed")
            if len(tools) != len(READ_ONLY_TOOL_NAMES) or any(
                not isinstance(tool, dict) or not isinstance(tool.get("name"), str)
                for tool in tools
            ):
                raise McpClientError("MCP tool list contains malformed or duplicate entries")
            discovered = frozenset(str(tool["name"]) for tool in tools if isinstance(tool, dict))
            if discovered != READ_ONLY_TOOL_NAMES:
                raise McpClientError(
                    "MCP tool surface differs from the Phase 10 read-only allowlist"
                )
            self.tool_names = discovered
        except Exception:
            self.__exit__(None, None, None)
            raise
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        """Close stdin and stop the child within the configured timeout."""
        process = self._process
        if process is None:
            return
        if process.stdin is not None:
            process.stdin.close()
        try:
            process.wait(timeout=self.timeout_seconds)
        except subprocess.TimeoutExpired:
            process.terminate()
            try:
                process.wait(timeout=self.timeout_seconds)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=self.timeout_seconds)
        finally:
            self._process = None

    def call(self, tool_name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        """Call one discovered read-only tool and return its structured response."""
        if tool_name not in self.tool_names:
            raise McpClientError(f"tool is not exposed by Phase 9: {tool_name}")
        result = self._request("tools/call", {"name": tool_name, "arguments": arguments})
        structured = result.get("structuredContent")
        if not isinstance(structured, dict):
            raise McpClientError(f"tool returned no structured content: {tool_name}")
        return structured

    def _request(self, method: str, params: dict[str, Any]) -> dict[str, Any]:
        process = self._process
        if process is None or process.stdin is None or process.stdout is None:
            raise McpClientError("MCP client is not running")
        request_id = self._next_id
        self._next_id += 1
        process.stdin.write(
            json.dumps({"jsonrpc": "2.0", "id": request_id, "method": method, "params": params})
            + "\n"
        )
        process.stdin.flush()
        selector = selectors.DefaultSelector()
        try:
            selector.register(process.stdout, selectors.EVENT_READ)
            if not selector.select(timeout=self.timeout_seconds):
                raise McpClientError("MCP response exceeded the configured transport timeout")
            line = process.stdout.readline()
        finally:
            selector.close()
        if not line:
            error = process.stderr.read()[:300] if process.stderr is not None else ""
            raise McpClientError(f"MCP server closed before responding: {error}")
        try:
            response = json.loads(line)
        except json.JSONDecodeError as error:
            raise McpClientError("MCP response was not JSON") from error
        if not isinstance(response, dict) or response.get("id") != request_id:
            raise McpClientError("MCP response did not match the request")
        if "error" in response:
            raise McpClientError("MCP protocol returned an error")
        result = response.get("result")
        if not isinstance(result, dict):
            raise McpClientError("MCP result is malformed")
        return result
