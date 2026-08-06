"""Local-only provider abstraction for Phase 8A dry-run fixtures."""

from __future__ import annotations

from enum import StrEnum
from pathlib import Path
from typing import Any

from data_platform.phase8a_common import load_json_object, report_header, write_json


class DryRunInputError(ValueError):
    """Raised when a local fixture is missing or malformed."""


class DryRunProvider(StrEnum):
    """Providers known across Phase 8 maturity levels."""

    LOCAL_FIXTURE = "local_fixture"
    BIGQUERY_SANDBOX = "bigquery_sandbox"
    BIGQUERY_CLOUD = "bigquery_cloud"


def unavailable_provider_report(
    provider: DryRunProvider, *, git_sha: str | None = None
) -> dict[str, Any]:
    """Return explicit not-available evidence without fixture fallback."""
    value = report_header(
        "bigquery_dry_run",
        git_sha=git_sha,
        evidence_level="not_available",
        provider=provider.value,
        cloud_execution_status="not_available",
    )
    value.update(
        {
            "status": "not_available",
            "estimated_bytes": None,
            "total_bytes_billed": None,
            "observed_job_id": None,
            "errors": [],
            "warnings": ["Provider is unavailable in Phase 8A; no fallback was performed."],
            "models_checked": [],
            "policy_findings": [],
        }
    )
    return value


def load_local_fixture(path: Path, *, git_sha: str | None = None) -> dict[str, Any]:
    """Validate and normalize a deterministic local fixture."""
    try:
        fixture = load_json_object(path)
    except ValueError as error:
        raise DryRunInputError(str(error)) from error
    required = {"schema_version", "fixture_name", "query_id", "model", "valid"}
    missing = sorted(required - fixture.keys())
    if missing or fixture.get("schema_version") != 1:
        raise DryRunInputError(f"fixture schema is invalid; missing={missing}")
    estimate = fixture.get("total_bytes_processed")
    if estimate is not None and (
        not isinstance(estimate, int) or isinstance(estimate, bool) or estimate < 0
    ):
        raise DryRunInputError("fixture estimate must be a non-negative integer or null")
    if "job_id" in fixture or "observed_job_id" in fixture:
        raise DryRunInputError("local fixture must not contain a job ID")
    value = report_header(
        "bigquery_dry_run",
        git_sha=git_sha,
        evidence_level="simulated",
        provider=DryRunProvider.LOCAL_FIXTURE.value,
    )
    value.update(fixture)
    value.update(
        {
            "status": "fixture_valid" if fixture["valid"] else "fixture_invalid",
            "estimated_bytes": estimate,
            "estimation_method": "fixture_estimated",
            "observed_job_id": None,
            "source_fixture": str(path),
            "errors": [] if fixture["valid"] else ["Fixture represents a sanitized invalid query."],
            "warnings": ["Fixture estimates are not BigQuery optimizer results."],
            "models_checked": [fixture["model"]],
            "policy_findings": [],
        }
    )
    return value


def run_provider(
    provider: DryRunProvider, fixture_path: Path | None, *, git_sha: str | None = None
) -> dict[str, Any]:
    """Run one provider without silently substituting another provider."""
    if provider is not DryRunProvider.LOCAL_FIXTURE:
        return unavailable_provider_report(provider, git_sha=git_sha)
    if fixture_path is None:
        raise DryRunInputError("local_fixture provider requires --fixture")
    return load_local_fixture(fixture_path, git_sha=git_sha)


def write_dry_run_report(report: dict[str, Any], path: Path) -> None:
    """Write provider evidence."""
    write_json(path, report)
