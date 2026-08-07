"""Minimal local STDIO MCP adapter for the read-only metadata service."""

import json
import sys
from pathlib import Path
from typing import Any, TextIO

from data_platform.metadata_models import ToolResponse
from data_platform.metadata_security import bounded_json, redact
from data_platform.metadata_service import INPUT_MODELS, MetadataService

SERVER_NAME = "retail-data-platform-discovery"
SERVER_VERSION = "1.0.0"
PROTOCOL_VERSION = "2025-11-25"
TOOL_DESCRIPTIONS = {
    "search_data_assets": "Search bounded dbt asset metadata by name, description, or column.",
    "get_model_schema": "Return documented schema and contract metadata; never returns rows.",
    "get_model_owner": "Return owner, domain, product, maturity, and SLO metadata.",
    "get_lineage": "Traverse bounded upstream/downstream dbt lineage with cycle protection.",
    "get_upstream_lineage": "Return bounded upstream lineage for one model.",
    "get_downstream_impact": "Return bounded downstream assets and published marts.",
    "get_quality_status": "Return latest indexed quality, freshness, and contract status.",
    "get_recent_pipeline_failures": "Return sanitized local failures and cloud availability.",
    "get_consumer_lag": "Read a fixed consumer-lag report without changing offsets.",
    "get_cost_estimate": "Return explicitly classified simulated or observed cost evidence.",
}


class StdioMcpServer:
    """Handle the small MCP request surface required by Phase 9."""

    def __init__(self, service: MetadataService) -> None:
        """Create the adapter around an already-loaded metadata service."""
        self.service = service

    def serve(self, source: TextIO = sys.stdin, sink: TextIO = sys.stdout) -> None:
        """Process newline-delimited JSON-RPC until stdin closes."""
        for line in source:
            try:
                message = json.loads(line)
                response = self.handle(message)
            except json.JSONDecodeError, TypeError, ValueError:
                response = self._error(None, -32700, "invalid JSON-RPC request")
            if response is not None:
                sink.write(bounded_json(response) + "\n")
                sink.flush()

    def handle(self, message: object) -> dict[str, Any] | None:
        """Return one protocol response; notifications intentionally return nothing."""
        if not isinstance(message, dict) or message.get("jsonrpc") != "2.0":
            return self._error(None, -32600, "invalid request")
        request_id = message.get("id")
        method = message.get("method")
        params = message.get("params", {})
        if request_id is None:
            return None
        if method == "initialize":
            requested = params.get("protocolVersion") if isinstance(params, dict) else None
            return self._result(
                request_id,
                {
                    "protocolVersion": requested
                    if isinstance(requested, str)
                    else PROTOCOL_VERSION,
                    "capabilities": {"tools": {"listChanged": False}},
                    "serverInfo": {"name": SERVER_NAME, "version": SERVER_VERSION},
                    "instructions": (
                        "Read-only metadata discovery. Never executes SQL or shell, reads rows, "
                        "writes files through tools, mutates schemas, reruns pipelines, "
                        "or resets offsets."
                    ),
                },
            )
        if method == "ping":
            return self._result(request_id, {})
        if method == "tools/list":
            return self._result(request_id, {"tools": self._tools()})
        if method == "tools/call":
            if not isinstance(params, dict) or not isinstance(params.get("name"), str):
                return self._error(request_id, -32602, "invalid tool call parameters")
            arguments = params.get("arguments", {})
            if not isinstance(arguments, dict):
                return self._error(request_id, -32602, "tool arguments must be an object")
            payload = self.service.call(params["name"], arguments)
            return self._result(
                request_id,
                {
                    "content": [{"type": "text", "text": bounded_json(payload)}],
                    "structuredContent": payload,
                    "isError": payload["status"] in {"error", "invalid_request"},
                },
            )
        return self._error(request_id, -32601, "method not found")

    @staticmethod
    def _tools() -> list[dict[str, Any]]:
        output_schema = ToolResponse.model_json_schema()
        return [
            {
                "name": name,
                "description": TOOL_DESCRIPTIONS[name],
                "inputSchema": model.model_json_schema(),
                "outputSchema": output_schema,
                "annotations": {
                    "readOnlyHint": True,
                    "destructiveHint": False,
                    "idempotentHint": True,
                    "openWorldHint": False,
                },
            }
            for name, model in sorted(INPUT_MODELS.items())
        ]

    @staticmethod
    def _result(request_id: int | str | None, result: dict[str, Any]) -> dict[str, Any]:
        return {"jsonrpc": "2.0", "id": request_id, "result": redact(result)}

    @staticmethod
    def _error(request_id: int | str | None, code: int, message: str) -> dict[str, Any]:
        return {"jsonrpc": "2.0", "id": request_id, "error": {"code": code, "message": message}}


def run_stdio(root: Path, *, timeout_seconds: float = 5.0) -> None:
    """Load the fixed index and run the localhost-equivalent STDIO transport."""
    service = MetadataService.from_output(root, timeout_seconds=timeout_seconds)
    StdioMcpServer(service).serve()
