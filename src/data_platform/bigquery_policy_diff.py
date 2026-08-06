"""Compare Phase 8A policies without changing Phase 7 contract semantics."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

from data_platform.phase8a_common import load_json_object, report_header, write_json

Severity = Literal["blocking", "manual_review", "info"]
Classification = Literal["breaking", "potentially_breaking", "non_breaking"]


class PolicyDiffInputError(ValueError):
    """Raised when policy-diff inputs are malformed."""


@dataclass(frozen=True, slots=True)
class PolicyDiffFinding:
    """One compatibility-policy change."""

    model: str
    change_type: str
    severity: Severity
    classification: Classification
    previous_value: object
    current_value: object
    evidence: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class PolicyDiffReport:
    """Companion report; independent from the Phase 7 contract report."""

    status: Literal["passed", "blocked", "previous_state_unavailable"]
    findings: tuple[PolicyDiffFinding, ...]
    git_sha: str | None = None

    @property
    def blocking_count(self) -> int:
        """Return the number of blocking changes."""
        return sum(item.severity == "blocking" for item in self.findings)

    def to_dict(self) -> dict[str, Any]:
        """Return a machine-readable report."""
        value = report_header(
            "bigquery_policy_diff", git_sha=self.git_sha, evidence_level="static_validation"
        )
        value.update(
            {
                "status": self.status,
                "models_checked": sorted({item.model for item in self.findings}),
                "blocking_count": self.blocking_count,
                "policy_findings": [asdict(item) for item in self.findings],
                "errors": [
                    item.change_type for item in self.findings if item.severity == "blocking"
                ],
                "warnings": [
                    item.change_type for item in self.findings if item.severity == "manual_review"
                ],
            }
        )
        return value


def _policies(manifest: dict[str, Any]) -> dict[str, dict[str, Any]]:
    nodes = manifest.get("nodes")
    if not isinstance(nodes, dict):
        raise PolicyDiffInputError("manifest requires object-valued nodes")
    result: dict[str, dict[str, Any]] = {}
    for node in nodes.values():
        if not isinstance(node, dict) or node.get("resource_type") != "model":
            continue
        if not str(node.get("original_file_path", "")).startswith("models/marts/"):
            continue
        meta_value = node.get("meta")
        meta: dict[str, Any] = meta_value if isinstance(meta_value, dict) else {}
        bigquery = meta.get("bigquery")
        if isinstance(bigquery, dict):
            result[str(node.get("name"))] = bigquery
    return result


def _finding(
    model: str,
    change_type: str,
    severity: Severity,
    classification: Classification,
    previous: object,
    current: object,
) -> PolicyDiffFinding:
    return PolicyDiffFinding(
        model,
        change_type,
        severity,
        classification,
        previous,
        current,
        (f"previous={previous!r}", f"current={current!r}"),
    )


def compare_policy_values(
    previous: dict[str, dict[str, Any]], current: dict[str, dict[str, Any]]
) -> tuple[PolicyDiffFinding, ...]:
    """Compare normalized model policies."""
    findings: list[PolicyDiffFinding] = []
    for model in sorted(previous.keys() & current.keys()):
        old = previous[model]
        new = current[model]
        old_partition = old.get("partition_by")
        new_partition = new.get("partition_by")
        for key, change_type in (
            ("field", "partition_field_changed"),
            ("data_type", "partition_type_changed"),
            ("granularity", "partition_granularity_changed"),
        ):
            old_value = old_partition.get(key) if isinstance(old_partition, dict) else None
            new_value = new_partition.get(key) if isinstance(new_partition, dict) else None
            if old_value != new_value:
                findings.append(
                    _finding(model, change_type, "blocking", "breaking", old_value, new_value)
                )
        if (
            old.get("require_partition_filter") is True
            and new.get("require_partition_filter") is False
        ):
            findings.append(
                _finding(model, "partition_filter_disabled", "blocking", "breaking", True, False)
            )
        old_incremental_value = old.get("incremental")
        new_incremental_value = new.get("incremental")
        old_incremental: dict[str, Any] = (
            old_incremental_value if isinstance(old_incremental_value, dict) else {}
        )
        new_incremental: dict[str, Any] = (
            new_incremental_value if isinstance(new_incremental_value, dict) else {}
        )
        if old_incremental.get("merge_key") != new_incremental.get("merge_key"):
            findings.append(
                _finding(
                    model,
                    "merge_key_changed",
                    "blocking",
                    "breaking",
                    old_incremental.get("merge_key"),
                    new_incremental.get("merge_key"),
                )
            )
        old_late_value = old_incremental.get("late_arriving_data")
        new_late_value = new_incremental.get("late_arriving_data")
        old_late: dict[str, Any] = old_late_value if isinstance(old_late_value, dict) else {}
        new_late: dict[str, Any] = new_late_value if isinstance(new_late_value, dict) else {}
        if old_late != new_late:
            severity: Severity = (
                "blocking"
                if new_late.get("outside_window_action") != "manual_bounded_backfill"
                else "manual_review"
            )
            findings.append(
                _finding(
                    model,
                    "late_data_policy_changed",
                    severity,
                    "breaking" if severity == "blocking" else "potentially_breaking",
                    old_late,
                    new_late,
                )
            )
        if old.get("maximum_expected_scan_window_days") != new.get(
            "maximum_expected_scan_window_days"
        ):
            old_window = old.get("maximum_expected_scan_window_days")
            new_window = new.get("maximum_expected_scan_window_days")
            severity = (
                "manual_review"
                if isinstance(old_window, int)
                and isinstance(new_window, int)
                and new_window > old_window
                else "info"
            )
            findings.append(
                _finding(
                    model,
                    "scan_window_changed",
                    severity,
                    "potentially_breaking" if severity == "manual_review" else "non_breaking",
                    old_window,
                    new_window,
                )
            )
        if old.get("cluster_by") != new.get("cluster_by"):
            findings.append(
                _finding(
                    model,
                    "cluster_fields_changed",
                    "manual_review",
                    "potentially_breaking",
                    old.get("cluster_by"),
                    new.get("cluster_by"),
                )
            )
        if old.get("cost_class") != new.get("cost_class"):
            findings.append(
                _finding(
                    model,
                    "cost_class_changed",
                    "manual_review",
                    "potentially_breaking",
                    old.get("cost_class"),
                    new.get("cost_class"),
                )
            )
        if old.get("validation_evidence_level") != new.get("validation_evidence_level") and new.get(
            "validation_evidence_level"
        ) in {"sandbox_observed", "cloud_observed"}:
            findings.append(
                _finding(
                    model,
                    "evidence_level_escalated",
                    "blocking",
                    "breaking",
                    old.get("validation_evidence_level"),
                    new.get("validation_evidence_level"),
                )
            )
    return tuple(sorted(findings, key=lambda item: (item.model, item.change_type)))


def compare_policy_manifests(
    previous_path: Path | None, current_path: Path, *, git_sha: str | None = None
) -> PolicyDiffReport:
    """Compare manifests or explicitly record unavailable prior state."""
    if previous_path is None or not previous_path.is_file():
        return PolicyDiffReport("previous_state_unavailable", (), git_sha)
    try:
        previous = _policies(load_json_object(previous_path))
        current = _policies(load_json_object(current_path))
    except ValueError as error:
        raise PolicyDiffInputError(str(error)) from error
    findings = compare_policy_values(previous, current)
    return PolicyDiffReport(
        "blocked" if any(item.severity == "blocking" for item in findings) else "passed",
        findings,
        git_sha,
    )


def compare_cost_thresholds(
    previous: dict[str, Any],
    current: dict[str, Any],
    *,
    waivers: dict[str, Any] | None = None,
    now: datetime | None = None,
) -> tuple[PolicyDiffFinding, ...]:
    """Block threshold loosening; waivers belong to run input, not permanent policy."""
    findings: list[PolicyDiffFinding] = []
    old_models = previous.get("models", {})
    new_models = current.get("models", {})
    if not isinstance(old_models, dict) or not isinstance(new_models, dict):
        raise PolicyDiffInputError("cost policy models must be objects")
    for model in sorted(old_models.keys() & new_models.keys()):
        old = old_models[model]
        new = new_models[model]
        if not isinstance(old, dict) or not isinstance(new, dict):
            continue
        old_value = old.get("blocking_threshold_bytes")
        new_value = new.get("blocking_threshold_bytes")
        if isinstance(old_value, int) and isinstance(new_value, int) and new_value > old_value:
            waiver = (waivers or {}).get(model)
            valid_waiver = False
            if isinstance(waiver, dict):
                expiry = waiver.get("expires_at")
                parsed: datetime | None = None
                if isinstance(expiry, str) and expiry.endswith("Z"):
                    try:
                        parsed = datetime.fromisoformat(expiry.replace("Z", "+00:00"))
                    except ValueError:
                        parsed = None
                valid_waiver = bool(
                    str(waiver.get("owner", "")).strip()
                    and str(waiver.get("reason", "")).strip()
                    and parsed is not None
                    and parsed.tzinfo is not None
                    and parsed > (now or datetime.now(UTC))
                    and isinstance(waiver.get("approved_threshold_bytes"), int)
                    and waiver["approved_threshold_bytes"] >= new_value
                )
            findings.append(
                _finding(
                    model,
                    (
                        "blocking_threshold_loosened_with_waiver"
                        if valid_waiver
                        else "blocking_threshold_loosened"
                    ),
                    "info" if valid_waiver else "blocking",
                    "potentially_breaking" if valid_waiver else "breaking",
                    old_value,
                    new_value,
                )
            )
    return tuple(findings)


def write_policy_diff_report(report: PolicyDiffReport, path: Path) -> None:
    """Write the companion policy-diff report."""
    write_json(path, report.to_dict())
