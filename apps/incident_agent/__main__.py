"""Run read-only MCP-backed incident diagnosis."""

import argparse
from pathlib import Path

from data_platform.incidents import IncidentAlert, IncidentDiagnoser, write_incident_report
from data_platform.mcp_client import RestrictedStdioMcpClient


def main() -> None:
    """Diagnose one alert and write JSON/Markdown reports."""
    parser = argparse.ArgumentParser(description="Read-only evidence-based incident diagnosis")
    parser.add_argument("--alert", type=Path, required=True)
    parser.add_argument("--repository-root", type=Path, default=Path.cwd())
    args = parser.parse_args()
    repository_root = args.repository_root.resolve()
    alert = IncidentAlert.model_validate_json(args.alert.read_text(encoding="utf-8"))
    with RestrictedStdioMcpClient(repository_root) as client:
        report = IncidentDiagnoser(client).diagnose(alert)
    json_path, markdown_path = write_incident_report(report, repository_root / "reports/incidents")
    print(f"incident_report_json={json_path.relative_to(repository_root)}")
    print(f"incident_report_markdown={markdown_path.relative_to(repository_root)}")


if __name__ == "__main__":
    main()
