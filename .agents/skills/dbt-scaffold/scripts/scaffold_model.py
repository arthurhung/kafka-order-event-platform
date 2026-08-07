"""Generate one verified model using the repository workflow."""

import argparse
import json
from pathlib import Path

from data_platform.mcp_client import RestrictedStdioMcpClient
from data_platform.skill_scaffold import (
    DbtScaffoldRequest,
    scaffold_verified_model,
    write_scaffold_report,
)


def main() -> None:
    """Validate a request file and atomically scaffold the requested model."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--request", type=Path, required=True)
    parser.add_argument("--project-dir", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--repository-root", type=Path, default=Path.cwd())
    args = parser.parse_args()
    request = DbtScaffoldRequest.model_validate_json(args.request.read_text(encoding="utf-8"))
    template_dir = Path(__file__).resolve().parents[1] / "templates"
    with RestrictedStdioMcpClient(args.repository_root) as client:
        report = scaffold_verified_model(
            request, client=client, project_dir=args.project_dir, template_dir=template_dir
        )
    write_scaffold_report(report, args.report)
    print(json.dumps(report.model_dump(mode="json"), sort_keys=True))


if __name__ == "__main__":
    main()
