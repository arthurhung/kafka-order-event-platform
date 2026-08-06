"""Evaluate deterministic local BigQuery cost-policy fixtures."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

from data_platform.phase8a_common import load_json_object, report_header, write_json

Decision = Literal["pass", "warn", "block", "invalid"]


class CostPolicyInputError(ValueError):
    """Raised when cost policy or fixture input is malformed."""


@dataclass(frozen=True, slots=True)
class CostFinding:
    """One cost-policy result."""

    severity: Literal["error", "warning", "info"]
    rule: str
    message: str


@dataclass(frozen=True, slots=True)
class CostPolicyResult:
    """Deterministic policy decision for one simulated estimate."""

    model: str
    decision: Decision
    estimated_bytes: int | None
    warning_threshold_bytes: int
    blocking_threshold_bytes: int
    cost_class: str
    findings: tuple[CostFinding, ...]
    waiver_applied: bool = False

    @property
    def blocked(self) -> bool:
        """Return whether the result must fail a gate."""
        return self.decision in ("block", "invalid")

    def to_dict(self, *, git_sha: str | None = None) -> dict[str, Any]:
        """Return a simulated machine-readable cost report."""
        value = report_header(
            "bigquery_cost_policy",
            git_sha=git_sha,
            evidence_level="simulated",
            provider="local_fixture",
        )
        value.update(
            {
                "status": self.decision,
                "estimation_method": "fixture_estimated",
                "model": self.model,
                "models_checked": [self.model],
                "estimated_bytes": self.estimated_bytes,
                "warning_threshold_bytes": self.warning_threshold_bytes,
                "blocking_threshold_bytes": self.blocking_threshold_bytes,
                "cost_class": self.cost_class,
                "waiver_applied": self.waiver_applied,
                "observed_job_id": None,
                "policy_findings": [asdict(item) for item in self.findings],
                "errors": [item.message for item in self.findings if item.severity == "error"],
                "warnings": [item.message for item in self.findings if item.severity == "warning"],
            }
        )
        return value


def load_cost_policy(path: Path) -> dict[str, Any]:
    """Load and validate the single-source cost configuration."""
    try:
        policy = load_json_object(path)
    except ValueError as error:
        raise CostPolicyInputError(str(error)) from error
    if policy.get("schema_version") != 1 or not isinstance(policy.get("default"), dict):
        raise CostPolicyInputError("cost policy requires schema_version=1 and default")
    models = policy.get("models")
    if not isinstance(models, dict):
        raise CostPolicyInputError("cost policy models must be an object")
    for name, value in {"default": policy["default"], **models}.items():
        if not isinstance(value, dict):
            raise CostPolicyInputError(f"cost policy {name} must be an object")
        warning = value.get("warning_threshold_bytes")
        blocking = value.get("blocking_threshold_bytes")
        if (
            not isinstance(warning, int)
            or isinstance(warning, bool)
            or not isinstance(blocking, int)
            or isinstance(blocking, bool)
            or warning <= 0
            or blocking <= warning
        ):
            raise CostPolicyInputError(f"cost policy {name} thresholds are invalid")
        if not str(value.get("cost_class", "")).strip():
            raise CostPolicyInputError(f"cost policy {name} requires cost_class")
    return policy


def _model_policy(policy: dict[str, Any], model: str) -> dict[str, Any]:
    models = policy["models"]
    value = models.get(model, policy["default"])
    if not isinstance(value, dict):
        raise CostPolicyInputError(f"model policy is invalid: {model}")
    return value


def _parse_waiver(waiver: object, now: datetime) -> tuple[int | None, list[CostFinding]]:
    if waiver is None:
        return None, []
    findings: list[CostFinding] = []
    if not isinstance(waiver, dict):
        return None, [CostFinding("error", "waiver", "waiver must be an object")]
    for field in ("owner", "reason"):
        if not str(waiver.get(field, "")).strip():
            findings.append(CostFinding("error", "waiver", f"waiver requires {field}"))
    expiry = waiver.get("expires_at")
    parsed: datetime | None = None
    if isinstance(expiry, str) and expiry.endswith("Z"):
        try:
            parsed = datetime.fromisoformat(expiry.replace("Z", "+00:00"))
        except ValueError:
            parsed = None
    if parsed is None or parsed.tzinfo is None or parsed.utcoffset() != UTC.utcoffset(parsed):
        findings.append(
            CostFinding("error", "waiver", "waiver expires_at must be timezone-aware UTC")
        )
    elif parsed <= now:
        findings.append(CostFinding("error", "waiver-expired", "waiver is expired"))
    approved = waiver.get("approved_threshold_bytes")
    if not isinstance(approved, int) or isinstance(approved, bool) or approved <= 0:
        findings.append(
            CostFinding("error", "waiver", "waiver requires a positive approved threshold")
        )
        approved = None
    return approved, findings


def evaluate_cost_policy(
    policy: dict[str, Any], fixture: dict[str, Any], *, now: datetime | None = None
) -> CostPolicyResult:
    """Evaluate one local fixture without treating it as cloud evidence."""
    model = fixture.get("model")
    if not isinstance(model, str) or not model:
        raise CostPolicyInputError("fixture requires model")
    model_policy = _model_policy(policy, model)
    warning = model_policy["warning_threshold_bytes"]
    blocking = model_policy["blocking_threshold_bytes"]
    findings: list[CostFinding] = []
    estimate = fixture.get("total_bytes_processed")
    if estimate is not None and (
        not isinstance(estimate, int) or isinstance(estimate, bool) or estimate < 0
    ):
        raise CostPolicyInputError("total_bytes_processed must be a non-negative integer or null")
    if fixture.get("valid") is not True:
        findings.append(
            CostFinding("error", "fixture-validity", "fixture represents an invalid query")
        )
    if estimate is None:
        findings.append(
            CostFinding(
                "error", "missing-estimate", "missing estimate remains null and cannot pass"
            )
        )
    if model != "fct_orders" and fixture.get("partition_filter_present") is not True:
        findings.append(
            CostFinding("error", "partition-filter", "required partition predicate is missing")
        )
    requested_window = fixture.get("requested_scan_window_days")
    maximum_window = fixture.get("maximum_scan_window_days")
    if (
        not isinstance(requested_window, int)
        or isinstance(requested_window, bool)
        or requested_window <= 0
    ):
        findings.append(
            CostFinding("error", "scan-window", "requested scan window must be positive")
        )
    if (
        not isinstance(maximum_window, int)
        or isinstance(maximum_window, bool)
        or maximum_window <= 0
    ):
        findings.append(CostFinding("error", "scan-window", "maximum scan window must be positive"))
    elif isinstance(requested_window, int) and requested_window > maximum_window:
        findings.append(
            CostFinding("error", "scan-window", "requested scan window exceeds model policy")
        )
    approved, waiver_findings = _parse_waiver(fixture.get("waiver"), now or datetime.now(UTC))
    findings.extend(waiver_findings)
    waiver_applied = False
    if estimate is not None:
        if approved is not None and not waiver_findings and estimate <= approved:
            waiver_applied = estimate >= blocking
            if waiver_applied:
                findings.append(
                    CostFinding("info", "waiver-applied", "valid bounded waiver applied")
                )
        elif estimate >= blocking:
            findings.append(
                CostFinding(
                    "error", "blocking-threshold", "estimate meets or exceeds blocking threshold"
                )
            )
        elif estimate >= warning:
            findings.append(
                CostFinding(
                    "warning", "warning-threshold", "estimate meets or exceeds warning threshold"
                )
            )
        if approved is not None and estimate > approved:
            findings.append(
                CostFinding(
                    "error", "waiver-threshold", "estimate exceeds approved waiver threshold"
                )
            )
    errors = any(item.severity == "error" for item in findings)
    warnings = any(item.severity == "warning" for item in findings)
    decision: Decision = "block" if errors else "warn" if warnings else "pass"
    if fixture.get("valid") is not True or estimate is None:
        decision = "invalid"
    return CostPolicyResult(
        model=model,
        decision=decision,
        estimated_bytes=estimate,
        warning_threshold_bytes=warning,
        blocking_threshold_bytes=blocking,
        cost_class=str(model_policy["cost_class"]),
        findings=tuple(findings),
        waiver_applied=waiver_applied,
    )


def write_cost_report(result: CostPolicyResult, path: Path, *, git_sha: str | None = None) -> None:
    """Write the simulated cost report."""
    write_json(path, result.to_dict(git_sha=git_sha))
