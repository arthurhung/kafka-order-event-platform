"""Validate local BigQuery compatibility declarations from a fresh dbt manifest."""

from __future__ import annotations

from collections import Counter
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

from data_platform.phase8a_common import load_json_object, report_header, write_json

Severity = Literal["error", "warning", "info"]
_STATUSES = {"not_supported", "planned", "static_validated", "sandbox_validated", "cloud_validated"}
_FORBIDDEN_PHASE8A_STATUSES = {"sandbox_validated", "cloud_validated"}
_FORBIDDEN_EVIDENCE = {"sandbox_observed", "cloud_observed"}
_PARTITION_TYPES = {"date", "timestamp", "datetime"}
_GRANULARITIES = {"hour", "day", "month", "year"}
_STRATEGIES = {"merge", "insert_overwrite"}
_REQUIRED_MODELS = {"fct_order_events", "fct_orders", "mart_daily_sales", "mart_service_health"}


class CompatibilityInputError(ValueError):
    """Raised when a manifest cannot be validated deterministically."""


@dataclass(frozen=True, slots=True)
class CompatibilityFinding:
    """One compatibility-policy result."""

    severity: Severity
    rule: str
    model: str
    message: str


@dataclass(frozen=True, slots=True)
class CompatibilityReport:
    """Compatibility validation result for published dbt models."""

    status: Literal["passed", "blocked"]
    manifest_path: str
    models_checked: tuple[str, ...]
    effective_statuses: dict[str, str]
    findings: tuple[CompatibilityFinding, ...]
    git_sha: str | None = None

    @property
    def error_count(self) -> int:
        """Return the number of blocking findings."""
        return sum(item.severity == "error" for item in self.findings)

    def to_dict(self) -> dict[str, Any]:
        """Return the standard machine-readable representation."""
        counts = Counter(item.severity for item in self.findings)
        value = report_header(
            "bigquery_compatibility", git_sha=self.git_sha, evidence_level="static_validation"
        )
        levels: tuple[Severity, ...] = ("error", "warning", "info")
        value.update(
            {
                "status": self.status,
                "manifest_path": self.manifest_path,
                "models_checked": list(self.models_checked),
                "effective_statuses": self.effective_statuses,
                "finding_counts": {level: counts[level] for level in levels},
                "policy_findings": [asdict(item) for item in self.findings],
                "errors": [item.message for item in self.findings if item.severity == "error"],
                "warnings": [item.message for item in self.findings if item.severity == "warning"],
            }
        )
        return value


def _finding(severity: Severity, rule: str, model: str, message: str) -> CompatibilityFinding:
    return CompatibilityFinding(severity, rule, model, message)


def _published_nodes(manifest: dict[str, Any]) -> dict[str, dict[str, Any]]:
    nodes = manifest.get("nodes")
    if not isinstance(nodes, dict):
        raise CompatibilityInputError("manifest must contain an object-valued nodes field")
    return {
        str(node.get("name")): node
        for node in nodes.values()
        if isinstance(node, dict)
        and node.get("resource_type") == "model"
        and str(node.get("original_file_path", "")).startswith("models/marts/")
    }


def extract_bigquery_policies(manifest_path: Path) -> dict[str, dict[str, Any]]:
    """Return published model BigQuery policies from a manifest."""
    try:
        manifest = load_json_object(manifest_path)
    except ValueError as error:
        raise CompatibilityInputError(str(error)) from error
    policies: dict[str, dict[str, Any]] = {}
    for name, node in _published_nodes(manifest).items():
        meta_value = node.get("meta")
        meta: dict[str, Any] = meta_value if isinstance(meta_value, dict) else {}
        bigquery = meta.get("bigquery")
        if isinstance(bigquery, dict):
            policies[name] = bigquery
    return policies


def extract_published_sql(manifest_path: Path) -> dict[str, str]:
    """Return raw SQL for published models from an explicit manifest."""
    try:
        manifest = load_json_object(manifest_path)
    except ValueError as error:
        raise CompatibilityInputError(str(error)) from error
    return {
        name: str(node.get("raw_code", "")) for name, node in _published_nodes(manifest).items()
    }


def _utc_future(value: object, now: datetime) -> bool:
    if not isinstance(value, str) or not value.endswith("Z"):
        return False
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return False
    return (
        parsed.tzinfo is not None and parsed.utcoffset() == UTC.utcoffset(parsed) and parsed > now
    )


def _validate_model(name: str, node: dict[str, Any], now: datetime) -> list[CompatibilityFinding]:
    findings: list[CompatibilityFinding] = []
    columns_value = node.get("columns")
    columns: dict[str, Any] = columns_value if isinstance(columns_value, dict) else {}
    meta_value = node.get("meta")
    meta: dict[str, Any] = meta_value if isinstance(meta_value, dict) else {}
    warehouse_value = meta.get("warehouse_compatibility")
    warehouse: dict[str, Any] | None = (
        warehouse_value if isinstance(warehouse_value, dict) else None
    )
    bigquery_value = meta.get("bigquery")
    bigquery: dict[str, Any] | None = bigquery_value if isinstance(bigquery_value, dict) else None
    raw_code = str(node.get("raw_code", ""))
    if "BLOCKING_TODO" in raw_code or meta.get("scaffold_status") == "draft":
        findings.append(_finding("error", "blocking-todo", name, "model is an unfinished draft"))
    if warehouse is None or warehouse.get("postgres") != "supported":
        findings.append(
            _finding("error", "warehouse-metadata", name, "postgres must remain supported")
        )
    status = warehouse.get("bigquery") if warehouse is not None else None
    if status not in _STATUSES:
        findings.append(_finding("error", "compatibility-status", name, "invalid BigQuery status"))
    elif status in _FORBIDDEN_PHASE8A_STATUSES:
        findings.append(
            _finding(
                "error", "compatibility-status", name, f"{status} requires later-phase evidence"
            )
        )
    elif status != "planned":
        findings.append(
            _finding(
                "error",
                "compatibility-status",
                name,
                "source declaration must remain planned in Phase 8A",
            )
        )
    if bigquery is None:
        findings.append(
            _finding("error", "bigquery-metadata", name, "BigQuery policy metadata is missing")
        )
        return findings
    evidence = bigquery.get("validation_evidence_level")
    if evidence in _FORBIDDEN_EVIDENCE or evidence != "static_validation":
        findings.append(
            _finding("error", "evidence-level", name, "Phase 8A evidence must be static_validation")
        )
    cluster = bigquery.get("cluster_by")
    if (
        not isinstance(cluster, list)
        or len(cluster) > 4
        or not all(isinstance(item, str) for item in cluster)
    ):
        findings.append(
            _finding(
                "error", "cluster-fields", name, "cluster_by must contain zero to four field names"
            )
        )
    cluster_fields = (
        [item for item in cluster if isinstance(item, str)] if isinstance(cluster, list) else []
    )
    for field in cluster_fields:
        if field not in columns:
            findings.append(
                _finding(
                    "error",
                    "cluster-field-exists",
                    name,
                    f"cluster field {field!r} is absent from contract",
                )
            )
    partition = bigquery.get("partition_by")
    if partition is None:
        exemption = bigquery.get("partition_exemption")
        if not isinstance(exemption, dict):
            findings.append(
                _finding(
                    "error",
                    "partition-exemption",
                    name,
                    "non-partitioned model requires an exemption",
                )
            )
        else:
            for field in ("owner", "reason"):
                if not str(exemption.get(field, "")).strip():
                    findings.append(
                        _finding(
                            "error",
                            "partition-exemption",
                            name,
                            f"partition exemption requires {field}",
                        )
                    )
            if not _utc_future(exemption.get("review_at"), now):
                findings.append(
                    _finding(
                        "error",
                        "partition-exemption-review",
                        name,
                        "partition exemption review_at must be a future UTC timestamp",
                    )
                )
        if bigquery.get("require_partition_filter") is not False:
            findings.append(
                _finding(
                    "error",
                    "partition-filter",
                    name,
                    "non-partitioned model must set require_partition_filter=false",
                )
            )
    elif not isinstance(partition, dict):
        findings.append(
            _finding("error", "partition-policy", name, "partition_by must be an object or null")
        )
    else:
        partition_field = partition.get("field")
        if partition_field not in columns:
            findings.append(
                _finding(
                    "error",
                    "partition-field-exists",
                    name,
                    f"partition field {partition_field!r} is absent from contract",
                )
            )
        if partition.get("data_type") not in _PARTITION_TYPES:
            findings.append(
                _finding("error", "partition-type", name, "invalid partition data type")
            )
        if partition.get("granularity") not in _GRANULARITIES:
            findings.append(
                _finding("error", "partition-granularity", name, "invalid partition granularity")
            )
        if isinstance(partition_field, str) and partition_field in cluster_fields:
            findings.append(
                _finding(
                    "error",
                    "partition-cluster-overlap",
                    name,
                    "partition field must not also be clustered",
                )
            )
        if bigquery.get("require_partition_filter") is not True:
            findings.append(
                _finding(
                    "error",
                    "partition-filter",
                    name,
                    "partitioned published model must require a partition filter",
                )
            )
    incremental = bigquery.get("incremental")
    if not isinstance(incremental, dict) or incremental.get("expected_strategy") not in _STRATEGIES:
        findings.append(
            _finding(
                "error",
                "incremental-strategy",
                name,
                "expected incremental strategy is missing or invalid",
            )
        )
    else:
        keys = incremental.get("unique_key")
        if not isinstance(keys, list) or not keys:
            findings.append(
                _finding("error", "incremental-key", name, "unique_key must be a non-empty list")
            )
        unique_keys = (
            [item for item in keys if isinstance(item, str)] if isinstance(keys, list) else []
        )
        for key in unique_keys:
            if key not in columns:
                findings.append(
                    _finding(
                        "error",
                        "incremental-key",
                        name,
                        f"unique key {key!r} is absent from contract",
                    )
                )
        merge_key = incremental.get("merge_key")
        if incremental.get("expected_strategy") == "merge" and (
            not isinstance(merge_key, str) or merge_key not in columns
        ):
            findings.append(
                _finding(
                    "error", "merge-key", name, "merge strategy requires a contracted merge_key"
                )
            )
        if incremental.get("expected_strategy") == "insert_overwrite" and merge_key is not None:
            findings.append(
                _finding("error", "merge-key", name, "insert_overwrite must not claim a merge_key")
            )
        late = incremental.get("late_arriving_data")
        if (
            not isinstance(late, dict)
            or not isinstance(late.get("lookback_days"), int)
            or late.get("lookback_days", 0) <= 0
        ):
            findings.append(
                _finding(
                    "error", "late-data", name, "late-data lookback_days must be a positive integer"
                )
            )
        elif late.get("outside_window_action") != "manual_bounded_backfill":
            findings.append(
                _finding(
                    "error",
                    "late-data",
                    name,
                    "outside-window action must be manual_bounded_backfill",
                )
            )
    maximum = bigquery.get("maximum_expected_scan_window_days")
    if not isinstance(maximum, int) or isinstance(maximum, bool) or maximum <= 0:
        findings.append(
            _finding("error", "scan-window", name, "maximum scan window must be a positive integer")
        )
    if not str(bigquery.get("cost_class", "")).strip():
        findings.append(_finding("error", "cost-class", name, "cost_class is required"))
    findings.append(
        _finding(
            "info", "model-checked", name, "BigQuery source status planned; local policy evaluated"
        )
    )
    return findings


def validate_compatibility_manifest(
    manifest_path: Path, *, git_sha: str | None = None, now: datetime | None = None
) -> CompatibilityReport:
    """Validate all published model compatibility policies."""
    try:
        manifest = load_json_object(manifest_path)
    except ValueError as error:
        raise CompatibilityInputError(str(error)) from error
    nodes = _published_nodes(manifest)
    findings: list[CompatibilityFinding] = []
    for missing in sorted(_REQUIRED_MODELS - nodes.keys()):
        findings.append(
            _finding("error", "required-model", missing, "required published model is absent")
        )
    validation_time = now or datetime.now(UTC)
    for name, node in sorted(nodes.items()):
        findings.extend(_validate_model(name, node, validation_time))
    errors_by_model = {item.model for item in findings if item.severity == "error"}
    effective = {
        name: "planned" if name in errors_by_model else "static_validated" for name in sorted(nodes)
    }
    ordered = tuple(sorted(findings, key=lambda item: (item.model, item.severity, item.rule)))
    return CompatibilityReport(
        status="blocked" if errors_by_model else "passed",
        manifest_path=str(manifest_path),
        models_checked=tuple(sorted(nodes)),
        effective_statuses=effective,
        findings=ordered,
        git_sha=git_sha,
    )


def write_compatibility_report(report: CompatibilityReport, path: Path) -> None:
    """Write a compatibility report."""
    write_json(path, report.to_dict())
