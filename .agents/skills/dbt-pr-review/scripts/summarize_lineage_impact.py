"""Query downstream lineage through Phase 9 restricted STDIO."""

import argparse
import json
from pathlib import Path

from data_platform.mcp_client import RestrictedStdioMcpClient


def main() -> None:
    """Print one bounded downstream-impact response."""
    parser = argparse.ArgumentParser()
    parser.add_argument("model_name")
    parser.add_argument("--repository-root", type=Path, default=Path.cwd())
    args = parser.parse_args()
    with RestrictedStdioMcpClient(args.repository_root) as client:
        response = client.call(
            "get_downstream_impact", {"model_name": args.model_name, "max_depth": 5}
        )
    print(json.dumps(response, sort_keys=True))


if __name__ == "__main__":
    main()
