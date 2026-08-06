"""Validate the pure-Python Phase 8A orchestration contract."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Literal

from data_platform.phase8a_common import load_json_object, report_header

_TASK_ORDER = (
    "validate_environment",
    "dbt_parse",
    "dbt_compile",
    "validate_conventions",
    "validate_bigquery_policy",
    "evaluate_fixture_cost",
    "quality_gate",
    "publish_local_reports",
)


class OrchestrationInputError(ValueError):
    """Raised when a local DAG contract is malformed."""


@dataclass(frozen=True, slots=True)
class OrchestrationFinding:
    """One local orchestration-contract finding."""

    severity: Literal["error", "info"]
    rule: str
    message: str


def validate_orchestration_contract(path: Path, *, git_sha: str | None = None) -> dict[str, Any]:
    """Validate task order, bounded retries/timeouts, and the no-cloud boundary."""
    try:
        contract = load_json_object(path)
    except ValueError as error:
        raise OrchestrationInputError(str(error)) from error
    findings: list[OrchestrationFinding] = []
    tasks = contract.get("tasks")
    if not isinstance(tasks, list):
        raise OrchestrationInputError("orchestration tasks must be a list")
    ids = tuple(task.get("id") for task in tasks if isinstance(task, dict))
    if ids != _TASK_ORDER:
        findings.append(
            OrchestrationFinding(
                "error", "task-order", "task order does not match Phase 8A contract"
            )
        )
    for task in tasks:
        if not isinstance(task, dict):
            findings.append(OrchestrationFinding("error", "task-shape", "task must be an object"))
            continue
        retries = task.get("retries")
        timeout = task.get("timeout_seconds")
        if not isinstance(retries, int) or isinstance(retries, bool) or not 0 <= retries <= 3:
            findings.append(
                OrchestrationFinding(
                    "error", "retries", f"task {task.get('id')} retries are not bounded"
                )
            )
        if not isinstance(timeout, int) or isinstance(timeout, bool) or not 1 <= timeout <= 600:
            findings.append(
                OrchestrationFinding(
                    "error", "timeout", f"task {task.get('id')} timeout is not bounded"
                )
            )
    if contract.get("credentials_required") is not False:
        findings.append(
            OrchestrationFinding("error", "credentials", "Phase 8A must not require credentials")
        )
    if contract.get("cloud_tasks") != []:
        findings.append(
            OrchestrationFinding(
                "error", "cloud-tasks", "Phase 8A orchestration must not contain cloud tasks"
            )
        )
    findings.append(
        OrchestrationFinding(
            "info",
            "boundary",
            "pure-Python local orchestration contract validated; no Airflow runtime claim",
        )
    )
    blocked = any(item.severity == "error" for item in findings)
    value = report_header(
        "phase8a_orchestration", git_sha=git_sha, evidence_level="static_validation"
    )
    value.update(
        {
            "status": "blocked" if blocked else "passed",
            "models_checked": [],
            "task_order": list(ids),
            "policy_findings": [asdict(item) for item in findings],
            "errors": [item.message for item in findings if item.severity == "error"],
            "warnings": [
                "Airflow runtime is not available in the verified Python 3.14.6 environment; "
                "only the pure-Python orchestration contract was validated."
            ],
            "airflow_runtime": "not_available",
            "airflow_import_status": "not_available",
        }
    )
    return value


def quality_gate(statuses: list[str]) -> Literal["passed", "blocked"]:
    """Prevent success publication when any upstream result is not passed/warn."""
    return "passed" if all(value in {"passed", "pass", "warn"} for value in statuses) else "blocked"
