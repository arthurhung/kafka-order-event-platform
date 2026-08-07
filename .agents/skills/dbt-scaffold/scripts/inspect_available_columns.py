"""Inspect one upstream schema through the Phase 9 STDIO server."""

import argparse
import json
from pathlib import Path

from data_platform.mcp_client import RestrictedStdioMcpClient
from data_platform.skill_scaffold import inspect_available_columns


def main() -> None:
    """Print verified columns and evidence IDs as JSON."""
    parser = argparse.ArgumentParser()
    parser.add_argument("upstream_model")
    parser.add_argument("--repository-root", type=Path, default=Path.cwd())
    args = parser.parse_args()
    with RestrictedStdioMcpClient(args.repository_root) as client:
        columns, evidence = inspect_available_columns(client, args.upstream_model)
    print(json.dumps({"columns": columns, "evidence": evidence}, sort_keys=True))


if __name__ == "__main__":
    main()
