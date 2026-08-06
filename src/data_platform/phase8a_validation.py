"""Run the complete local-only Phase 8A validation gate."""

from __future__ import annotations

import os
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from data_platform.bigquery_compatibility import (
    extract_bigquery_policies,
    extract_published_sql,
    validate_compatibility_manifest,
    write_compatibility_report,
)
from data_platform.bigquery_cost_policy import (
    evaluate_cost_policy,
    load_cost_policy,
    write_cost_report,
)
from data_platform.bigquery_dry_run import DryRunProvider, run_provider, write_dry_run_report
from data_platform.bigquery_policy_diff import (
    PolicyDiffReport,
    compare_cost_thresholds,
    compare_policy_manifests,
    write_policy_diff_report,
)
from data_platform.bigquery_sql_policy import (
    SQLFinding,
    SQLPolicyReport,
    analyze_sql,
    validate_sql_directory,
    write_sql_policy_report,
)
from data_platform.phase8a_common import load_json_object, report_header, write_json
from data_platform.phase8a_orchestration import quality_gate, validate_orchestration_contract

_SAFE_RUN_ID = re.compile(r"^[a-z0-9_]+$")


class Phase8AValidationError(RuntimeError):
    """Raised when local orchestration input or dbt parsing fails."""


@dataclass(frozen=True, slots=True)
class Phase8AResult:
    """Paths and status from one isolated Phase 8A run."""

    status: str
    run_id: str
    target_directory: str
    report_directory: str
    reports: tuple[str, ...]
    git_sha: str | None

    def to_dict(self) -> dict[str, Any]:
        """Return the aggregate summary."""
        return {
            "status": self.status,
            "run_id": self.run_id,
            "target_directory": self.target_directory,
            "report_directory": self.report_directory,
            "reports": list(self.reports),
            "git_sha": self.git_sha,
        }


def _git_sha(repo_root: Path) -> str | None:
    completed = subprocess.run(
        ["git", "rev-parse", "HEAD"],  # noqa: S607 - repository requires Git on PATH
        cwd=repo_root,
        check=False,
        capture_output=True,
        text=True,
    )
    return completed.stdout.strip() if completed.returncode == 0 else None


def _fresh_manifest(repo_root: Path, target_path: Path, target: str) -> Path:
    project = repo_root / "dbt"
    command = [
        "dbt",
        "parse",
        "--project-dir",
        str(project),
        "--profiles-dir",
        str(project),
        "--target",
        target,
        "--target-path",
        str(target_path),
        "--log-path",
        str(target_path / "logs"),
    ]
    environment = {
        key: value for key, value in os.environ.items() if not key.startswith("COV_CORE_")
    }
    completed = subprocess.run(  # noqa: S603 - fixed dbt command with validated paths
        command,
        cwd=repo_root,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode:
        tail = (completed.stdout + completed.stderr)[-2000:]
        for key in ("POSTGRES_PASSWORD", "DATABASE_URL", "DBT_ENV_SECRET_PASSWORD"):
            secret = environment.get(key)
            if secret:
                tail = tail.replace(secret, "[REDACTED]")
        raise Phase8AValidationError(f"fresh dbt parse failed ({completed.returncode}): {tail}")
    manifest = target_path / "manifest.json"
    if not manifest.is_file():
        raise Phase8AValidationError("fresh dbt parse did not produce manifest.json")
    return manifest


def _partition_report(compatibility: dict[str, Any]) -> dict[str, Any]:
    value = dict(compatibility)
    value["report_type"] = "partition_cluster_policy"
    value["policy_findings"] = [
        item
        for item in compatibility["policy_findings"]
        if item["rule"].startswith(("partition", "cluster", "scan", "incremental", "merge", "late"))
        or item["rule"] == "model-checked"
    ]
    return value


def run_phase8a_validation(
    *,
    repo_root: Path,
    run_id: str,
    target: str = "local",
    previous_manifest: Path | None = None,
    previous_cost_policy: Path | None = None,
) -> Phase8AResult:
    """Run fresh static checks, one simulated fixture, and the local quality gate."""
    if not _SAFE_RUN_ID.fullmatch(run_id):
        raise Phase8AValidationError(
            "run_id must contain only lowercase letters, digits, underscores"
        )
    repo_root = repo_root.resolve()
    target_path = repo_root / "dbt" / "target" / "phase8a" / run_id
    report_path = repo_root / "reports" / "data-quality" / "phase8a" / run_id
    if target_path.exists() or report_path.exists():
        raise Phase8AValidationError("run-specific target or report directory already exists")
    target_path.mkdir(parents=True)
    report_path.mkdir(parents=True)
    sha = _git_sha(repo_root)
    manifest = _fresh_manifest(repo_root, target_path, target)

    compatibility = validate_compatibility_manifest(manifest, git_sha=sha)
    compatibility_path = report_path / "bigquery-compatibility-report.json"
    write_compatibility_report(compatibility, compatibility_path)
    partition_path = report_path / "partition-cluster-policy-report.json"
    write_json(partition_path, _partition_report(compatibility.to_dict()))

    policies = extract_bigquery_policies(manifest)
    query_report = validate_sql_directory(
        repo_root / "tests" / "data_platform" / "fixtures" / "bigquery_sql",
        policies,
        git_sha=sha,
    )
    model_findings: list[SQLFinding] = []
    for model, sql in extract_published_sql(manifest).items():
        model_findings.extend(
            analyze_sql(
                model,
                sql,
                partition_field=None,
                require_partition_filter=False,
                monetary=False,
            )
        )
    all_sql_findings = tuple(
        sorted(
            (*query_report.findings, *model_findings),
            key=lambda item: (item.model, item.severity, item.rule),
        )
    )
    sql_report = SQLPolicyReport(
        "blocked" if any(item.severity == "error" for item in all_sql_findings) else "passed",
        query_report.models_checked,
        all_sql_findings,
        sha,
    )
    sql_path = report_path / "bigquery-sql-policy-report.json"
    write_sql_policy_report(sql_report, sql_path)

    fixture_path = (
        repo_root
        / "tests"
        / "data_platform"
        / "fixtures"
        / "bigquery_dry_run"
        / "below_warning.json"
    )
    dry_run = run_provider(DryRunProvider.LOCAL_FIXTURE, fixture_path, git_sha=sha)
    dry_run_path = report_path / "local-dry-run-fixture-report.json"
    write_dry_run_report(dry_run, dry_run_path)
    cost = evaluate_cost_policy(
        load_cost_policy(repo_root / "config" / "data_platform" / "bigquery_cost_policy.json"),
        load_json_object(fixture_path),
    )
    cost_path = report_path / "cost-policy-report.json"
    write_cost_report(cost, cost_path, git_sha=sha)

    policy_diff = compare_policy_manifests(previous_manifest, manifest, git_sha=sha)
    cost_policy_path = repo_root / "config" / "data_platform" / "bigquery_cost_policy.json"
    if previous_cost_policy is not None and previous_cost_policy.is_file():
        cost_findings = compare_cost_thresholds(
            load_json_object(previous_cost_policy), load_json_object(cost_policy_path)
        )
        combined = tuple(
            sorted(
                (*policy_diff.findings, *cost_findings),
                key=lambda item: (item.model, item.change_type),
            )
        )
        policy_diff = PolicyDiffReport(
            "blocked" if any(item.severity == "blocking" for item in combined) else "passed",
            combined,
            sha,
        )
    policy_diff_path = report_path / "bigquery-policy-diff-report.json"
    write_policy_diff_report(policy_diff, policy_diff_path)
    orchestration = validate_orchestration_contract(
        repo_root / "config" / "data_platform" / "phase8a_dag_contract.json",
        git_sha=sha,
    )
    orchestration_path = report_path / "phase8a-orchestration-report.json"
    write_json(orchestration_path, orchestration)

    diff_status = (
        "passed" if policy_diff.status == "previous_state_unavailable" else policy_diff.status
    )
    gate = quality_gate(
        [
            compatibility.status,
            sql_report.status,
            cost.decision,
            diff_status,
            orchestration["status"],
        ]
    )
    report_files = tuple(
        str(path)
        for path in (
            compatibility_path,
            partition_path,
            sql_path,
            cost_path,
            dry_run_path,
            policy_diff_path,
            orchestration_path,
        )
    )
    summary = report_header("phase8a_ci_summary", git_sha=sha, evidence_level="static_validation")
    summary.update(
        {
            "status": gate,
            "run_id": run_id,
            "models_checked": list(compatibility.models_checked),
            "errors": [] if gate == "passed" else ["One or more Phase 8A policy gates blocked."],
            "warnings": [
                "No BigQuery query or dry run was executed.",
                "Fixture estimates are not BigQuery optimizer results.",
            ],
            "policy_findings": [],
            "reports": list(report_files),
            "phase_status": {
                "phase_8a": "local_implementation_complete" if gate == "passed" else "in_progress",
                "phase_8b": "not_started",
                "phase_8c": "not_started",
            },
            "billing_used": False,
            "gcp_credentials_accessed": False,
            "bigquery_runtime_execution": False,
        }
    )
    summary_path = report_path / "phase8a-ci-summary.json"
    write_json(summary_path, summary)
    return Phase8AResult(
        gate,
        run_id,
        str(target_path),
        str(report_path),
        (*report_files, str(summary_path)),
        sha,
    )
