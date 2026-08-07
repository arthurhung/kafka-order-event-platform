"""Run the local read-only Phase 9 MCP server over STDIO."""

import argparse
from pathlib import Path

from data_platform.mcp_server import run_stdio


def main() -> None:
    """Run STDIO only; no public network listener is created."""
    parser = argparse.ArgumentParser(description="Run the Phase 9 metadata MCP server")
    parser.add_argument("--repository-root", type=Path, default=Path.cwd())
    parser.add_argument("--timeout-seconds", type=float, default=5.0)
    arguments = parser.parse_args()
    if arguments.timeout_seconds <= 0 or arguments.timeout_seconds > 60:
        parser.error("timeout must be greater than zero and at most 60 seconds")
    run_stdio(arguments.repository_root, timeout_seconds=arguments.timeout_seconds)


if __name__ == "__main__":
    main()
