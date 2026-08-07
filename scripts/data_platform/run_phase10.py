"""Deterministic smoke and demo entrypoints for Phase 10."""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Any

from data_platform.dbt_review import DbtReviewRequest, review_dbt_changes, write_review_report
from data_platform.incidents import (
    FixtureEvidenceClient,
    IncidentAlert,
    IncidentDiagnoser,
    write_incident_report,
)
from data_platform.mcp_client import RestrictedStdioMcpClient
from data_platform.skill_scaffold import (
    DbtScaffoldRequest,
    ScaffoldCommandResult,
    scaffold_verified_model,
    write_scaffold_report,
)


def _write(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _validate_scaffold_with_dbt(
    root: Path,
    request: DbtScaffoldRequest,
    template_dir: Path,
) -> list[ScaffoldCommandResult]:
    """Parse and compile generated files inside a temporary project copy."""
    executable = shutil.which("dbt")
    if executable is None:
        raise RuntimeError("dbt executable is unavailable")
    results: list[ScaffoldCommandResult] = []
    with tempfile.TemporaryDirectory(prefix="phase10-scaffold-dbt-") as temporary:
        project = Path(temporary) / "dbt"
        shutil.copytree(
            root / "dbt",
            project,
            ignore=shutil.ignore_patterns("target", "logs", "dbt_packages"),
        )
        with RestrictedStdioMcpClient(root) as client:
            scaffold_verified_model(
                request,
                client=client,
                project_dir=project,
                template_dir=template_dir,
            )
        target = Path(temporary) / "target"
        for action in ("parse", "compile", "build"):
            command = [
                executable,
                action,
                "--project-dir",
                str(project),
                "--profiles-dir",
                str(project),
                "--target",
                "local",
                "--target-path",
                str(target),
            ]
            if action == "build":
                command.extend(("--select", request.model_name))
            completed = subprocess.run(  # noqa: S603 - fixed dbt executable and arguments
                command,
                check=False,
                capture_output=True,
                text=True,
            )
            results.append(
                ScaffoldCommandResult(
                    command=f"dbt {action} in temporary project",
                    exit_code=completed.returncode,
                    result="passed" if completed.returncode == 0 else "failed",
                )
            )
            if completed.returncode:
                output = (completed.stdout + completed.stderr)[-1000:]
                raise RuntimeError(f"temporary dbt {action} failed: {output}")
    return results


def scaffold_smoke(root: Path) -> dict[str, Any]:
    """Generate twice in temporary projects through real STDIO and verify cleanup."""
    request_path = root / "tests/data_platform/fixtures/phase10/scaffold_request.json"
    request = DbtScaffoldRequest.model_validate_json(request_path.read_text(encoding="utf-8"))
    template_dir = root / ".agents/skills/dbt-scaffold/templates"
    rendered: list[dict[str, str]] = []
    with RestrictedStdioMcpClient(root) as client:
        for _ in range(2):
            with tempfile.TemporaryDirectory(prefix="phase10-scaffold-smoke-") as temporary:
                project = Path(temporary) / "dbt"
                report = scaffold_verified_model(
                    request,
                    client=client,
                    project_dir=project,
                    template_dir=template_dir,
                )
                contents = {
                    relative: (project / relative).read_text(encoding="utf-8")
                    for relative in report.generated_files
                }
                rendered.append(contents)
                if not all((project / relative).is_file() for relative in report.generated_files):
                    raise RuntimeError("scaffold smoke did not create every expected file")
    if rendered[0] != rendered[1]:
        raise RuntimeError("identical scaffold inputs produced different output")
    dbt_results = _validate_scaffold_with_dbt(root, request, template_dir)
    report = report.model_copy(
        update={
            "validation_commands": [
                ScaffoldCommandResult(
                    command="Phase 9 get_model_schema over STDIO",
                    exit_code=0,
                    result="passed",
                ),
                ScaffoldCommandResult(
                    command="atomic scaffold generation in temporary output",
                    exit_code=0,
                    result="passed",
                ),
                ScaffoldCommandResult(
                    command="deterministic duplicate-input comparison and cleanup",
                    exit_code=0,
                    result="passed",
                ),
                *dbt_results,
            ]
        }
    )
    output = root / "reports/skills/dbt-scaffold-smoke.json"
    write_scaffold_report(report, output)
    payload = report.model_dump(mode="json") | {
        "cleanup_status": "passed",
        "deterministic_comparison": "passed",
        "mcp_transport": "stdio",
        "mcp_protocol_steps": ["initialize", "tools/list", "tools/call"],
        "environment": "local_temporary_output",
        "run_id": "phase10-scaffold-smoke",
    }
    _write(output, payload)
    return payload


def review_smoke(root: Path) -> dict[str, Any]:
    """Run normal, warning/degraded, and blocking deterministic fixtures."""
    directory = root / "tests/data_platform/fixtures/phase10"
    expected = {
        "review_normal.json": "passed",
        "review_warning.json": "degraded",
        "review_blocking.json": "blocked",
    }
    results: dict[str, Any] = {}
    for name, status in expected.items():
        request = DbtReviewRequest.model_validate_json((directory / name).read_text())
        first = review_dbt_changes(request)
        second = review_dbt_changes(request)
        if first.model_dump() != second.model_dump() or first.status != status:
            raise RuntimeError(f"review fixture failed deterministic expectation: {name}")
        results[name] = first.model_dump(mode="json")
        if name == "review_blocking.json":
            write_review_report(first, root / "reports/skills/dbt-pr-review-findings.json")
    payload = {
        "report_type": "dbt_pr_review_smoke",
        "schema_version": 1,
        "status": "passed",
        "evidence_level": "static_validation",
        "deterministic_status": "passed",
        "generated_at": "2026-08-07T12:00:00Z",
        "environment": "local_fixture",
        "run_id": "phase10-review-smoke",
        "inputs": sorted(expected),
        "warnings": [],
        "errors": [],
        "scenarios": results,
    }
    _write(root / "reports/skills/dbt-pr-review-smoke.json", payload)
    return payload


def incident_smoke(root: Path) -> dict[str, Any]:
    """Run all four required deterministic incident scenarios."""
    fixture_dir = root / ".agents/skills/incident-diagnosis/fixtures"
    expected = {
        "freshness_failure.json": "completed",
        "lag_failure.json": "completed",
        "quality_failure.json": "completed",
        "insufficient_evidence.json": "degraded",
    }
    scenarios: dict[str, Any] = {}
    for name, status in expected.items():
        fixture = json.loads((fixture_dir / name).read_text(encoding="utf-8"))
        alert = IncidentAlert.model_validate(fixture["alert"])
        client = FixtureEvidenceClient(fixture["responses"])
        first = IncidentDiagnoser(client).diagnose(alert)
        second = IncidentDiagnoser(client).diagnose(alert)
        if first.model_dump() != second.model_dump() or first.status != status:
            raise RuntimeError(f"incident fixture failed deterministic expectation: {name}")
        write_incident_report(first, root / "reports/incidents")
        scenarios[name] = {
            "incident_id": first.incident_id,
            "status": first.status,
            "most_likely_cause": first.most_likely_cause.model_dump(mode="json"),
            "state_history": first.state_history,
        }
    payload = {
        "report_type": "incident_diagnosis_smoke",
        "schema_version": 1,
        "status": "passed",
        "evidence_level": "simulated",
        "deterministic_status": "passed",
        "generated_at": "2026-08-07T11:00:00Z",
        "environment": "local_fixture",
        "run_id": "phase10-incident-smoke",
        "inputs": sorted(expected),
        "warnings": ["Fixture scenarios are simulated, not observed incidents."],
        "errors": [],
        "scenarios": scenarios,
    }
    _write(root / "reports/skills/incident-diagnosis-smoke.json", payload)
    return payload


def incident_demo(root: Path) -> dict[str, Any]:
    """Run a real Phase 9 STDIO-backed diagnosis without production mutation."""
    alert = IncidentAlert.model_validate(
        {
            "incident_id": "INC-DEMO-001",
            "alert_type": "freshness",
            "asset": "mart_daily_sales",
            "observed_at": "2026-08-07T12:30:00Z",
            "severity": "medium",
            "message": "Controlled local Phase 10 incident demonstration.",
        }
    )
    with RestrictedStdioMcpClient(root) as client:
        report = IncidentDiagnoser(client).diagnose(alert)
        tool_count = len(client.tool_names)
    write_incident_report(report, root / "reports/incidents")
    payload = {
        "report_type": "phase10_incident_demo",
        "schema_version": 1,
        "status": "passed",
        "diagnosis_status": report.status,
        "evidence_level": "static_validation",
        "mcp_transport": "stdio",
        "mcp_adapter": "restricted_local_adapter",
        "protocol_steps": ["initialize", "tools/list", "tools/call"],
        "tool_count": tool_count,
        "mutation_executed": False,
        "incident_id": report.incident_id,
        "generated_at": report.generated_at.isoformat(),
        "environment": "local_repository",
        "run_id": "phase10-incident-demo",
        "warnings": [
            "This validates local STDIO evidence consumption, not production observability."
        ],
        "errors": [],
    }
    _write(root / "reports/skills/incident-demo-summary.json", payload)
    return payload


def phase10_summary(root: Path) -> dict[str, Any]:
    """Write an allowlisted local/CI summary from current Phase 10 reports."""
    expected = [
        "dbt-scaffold-smoke.json",
        "dbt-pr-review-smoke.json",
        "incident-diagnosis-smoke.json",
        "incident-demo-summary.json",
    ]
    missing = [name for name in expected if not (root / "reports/skills" / name).is_file()]
    metadata_index = root / "reports/metadata/metadata-index.json"
    generated_at = None
    if metadata_index.is_file():
        generated_at = json.loads(metadata_index.read_text(encoding="utf-8")).get("generated_at")
    payload = {
        "report_type": "phase10_ci_summary",
        "schema_version": 1,
        "status": "failed" if missing else "passed",
        "evidence_level": "static_validation",
        "mcp_transport": "stdio",
        "mutation_tools_exposed": False,
        "cloud_execution": "not_available",
        "phase8b": "not_started_optional",
        "phase8c": "not_started_optional_deferred",
        "artifacts": expected,
        "missing": missing,
        "generated_at": generated_at,
        "environment": "local_or_github_actions",
        "run_id": "phase10-current-run",
        "warnings": [],
        "errors": [] if not missing else ["Required Phase 10 child artifact is missing."],
    }
    _write(root / "reports/skills/phase10-ci-summary.json", payload)
    return payload


def main() -> None:
    """Dispatch one fixed Phase 10 smoke or demo operation."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "operation",
        choices=("scaffold-smoke", "review-smoke", "incident-smoke", "incident-demo", "summary"),
    )
    parser.add_argument("--repository-root", type=Path, default=Path.cwd())
    args = parser.parse_args()
    root = args.repository_root.resolve()
    handlers = {
        "scaffold-smoke": scaffold_smoke,
        "review-smoke": review_smoke,
        "incident-smoke": incident_smoke,
        "incident-demo": incident_demo,
        "summary": phase10_summary,
    }
    payload = handlers[args.operation](root)
    print(json.dumps(payload, sort_keys=True))
    raise SystemExit(0 if payload["status"] in {"passed", "completed"} else 1)


if __name__ == "__main__":
    main()
