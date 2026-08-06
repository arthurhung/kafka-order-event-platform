"""Deterministic dbt manifest and SQL convention validation."""

from __future__ import annotations

import json
import re
from collections import Counter
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Literal

Severity = Literal["error", "warning", "info"]

_LAYER_PREFIXES = {
    "staging": ("stg_",),
    "intermediate": ("int_",),
    "marts": ("fct_", "dim_", "mart_"),
}
_PROHIBITED_NAMES = {"final_table", "temp_data", "new_model", "test_model"}
_MATURITIES = {"experimental", "beta", "stable", "deprecated"}
_WILDCARD = re.compile(
    r"\bselect\s+(?:distinct\s+)?(?:[a-z_][a-z0-9_]*\.)?\*\s*(?:,|\bfrom\b)",
    flags=re.IGNORECASE | re.DOTALL,
)


class ConventionInputError(ValueError):
    """Raised when the manifest cannot be safely validated."""


@dataclass(frozen=True, slots=True)
class ConventionFinding:
    """One deterministic convention result."""

    severity: Severity
    rule: str
    model: str
    file: str
    message: str


@dataclass(frozen=True, slots=True)
class ConventionReport:
    """Machine-readable convention validation result."""

    status: Literal["passed", "failed"]
    manifest_path: str
    checked_models: int
    findings: tuple[ConventionFinding, ...]

    @property
    def error_count(self) -> int:
        """Return the number of blocking findings."""
        return sum(finding.severity == "error" for finding in self.findings)

    def to_dict(self) -> dict[str, object]:
        """Return a JSON-serializable representation."""
        counts = Counter(finding.severity for finding in self.findings)
        return {
            "report_type": "dbt_convention_validation",
            "schema_version": 1,
            "status": self.status,
            "manifest_path": self.manifest_path,
            "checked_models": self.checked_models,
            "finding_counts": {
                "error": counts["error"],
                "warning": counts["warning"],
                "info": counts["info"],
            },
            "findings": [asdict(finding) for finding in self.findings],
        }


def _load_manifest(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ConventionInputError(f"cannot read manifest {path}: {error}") from error
    if not isinstance(value, dict) or not isinstance(value.get("nodes"), dict):
        raise ConventionInputError("manifest must contain an object-valued nodes field")
    return value


def _model_nodes(manifest: dict[str, object]) -> list[dict[str, object]]:
    nodes = manifest["nodes"]
    if not isinstance(nodes, dict):
        raise ConventionInputError("manifest nodes must be an object")
    return [
        node
        for node in nodes.values()
        if isinstance(node, dict) and node.get("resource_type") == "model"
    ]


def _layer(node: dict[str, object]) -> str:
    path = str(node.get("original_file_path", ""))
    parts = Path(path).parts
    if len(parts) >= 2 and parts[0] == "models":
        return parts[1]
    return "unknown"


def _finding(
    severity: Severity,
    rule: str,
    node: dict[str, object],
    message: str,
) -> ConventionFinding:
    return ConventionFinding(
        severity=severity,
        rule=rule,
        model=str(node.get("name", "<unknown>")),
        file=str(node.get("original_file_path", "<unknown>")),
        message=message,
    )


def _validate_identity(node: dict[str, object], layer: str) -> list[ConventionFinding]:
    findings: list[ConventionFinding] = []
    name = str(node.get("name", ""))
    prefixes = _LAYER_PREFIXES.get(layer)
    if prefixes is None:
        findings.append(
            _finding("error", "model-layer", node, f"unsupported model layer {layer!r}")
        )
    elif not name.startswith(prefixes):
        findings.append(
            _finding(
                "error",
                "model-prefix",
                node,
                f"model in {layer!r} must use one of these prefixes: {prefixes}",
            )
        )
    if name in _PROHIBITED_NAMES:
        findings.append(_finding("error", "prohibited-model-name", node, name))
    return findings


def _validate_dependencies(node: dict[str, object], layer: str) -> list[ConventionFinding]:
    depends_on = node.get("depends_on", {})
    dependency_nodes = depends_on.get("nodes", []) if isinstance(depends_on, dict) else []
    sources = [item for item in dependency_nodes if str(item).startswith("source.")]
    if layer != "staging" and sources:
        return [
            _finding(
                "error",
                "direct-source-outside-staging",
                node,
                f"direct source dependencies are only allowed in staging: {sources}",
            )
        ]
    if layer == "staging" and len(sources) > 1:
        return [
            _finding(
                "error",
                "staging-multiple-sources",
                node,
                f"staging model directly depends on multiple sources: {sources}",
            )
        ]
    return []


def _validate_wildcards(node: dict[str, object], layer: str) -> list[ConventionFinding]:
    sql = str(node.get("raw_code", ""))
    if not _WILDCARD.search(sql):
        return []
    severity: Severity = "warning" if layer == "intermediate" else "error"
    return [
        _finding(
            severity,
            "select-star",
            node,
            f"wildcard projection is {severity} severity in {layer}",
        )
    ]


def _validate_published_model(node: dict[str, object]) -> list[ConventionFinding]:
    findings: list[ConventionFinding] = []
    description = str(node.get("description", "")).strip()
    meta = node.get("meta", {})
    meta = meta if isinstance(meta, dict) else {}
    contract = node.get("contract", {})
    contract = contract if isinstance(contract, dict) else {}
    columns = node.get("columns", {})
    columns = columns if isinstance(columns, dict) else {}

    if not description.lower().startswith("grain: one row per "):
        findings.append(
            _finding("error", "mart-grain", node, "mart description must begin with a grain")
        )
    for key in (
        "owner",
        "domain",
        "data_product",
        "maturity",
        "contains_pii",
        "sla",
        "contract_policy",
    ):
        if key not in meta:
            findings.append(
                _finding("error", "mart-metadata", node, f"missing metadata field {key}")
            )
    if meta.get("maturity") not in _MATURITIES:
        findings.append(_finding("error", "mart-maturity", node, "invalid or missing maturity"))
    if not isinstance(meta.get("contains_pii"), bool):
        findings.append(_finding("error", "mart-pii", node, "contains_pii must be boolean"))
    sla = meta.get("sla")
    if (
        not isinstance(sla, dict)
        or not isinstance(sla.get("freshness_minutes"), int)
        or not sla.get("availability")
    ):
        findings.append(
            _finding("error", "mart-slo", node, "SLO requires freshness_minutes and availability")
        )
    if contract.get("enforced") is not True:
        findings.append(
            _finding("error", "mart-contract", node, "published mart contract must be enforced")
        )
    if not columns:
        findings.append(
            _finding("error", "mart-columns", node, "published mart must declare columns")
        )
    for column_name, column in columns.items():
        column = column if isinstance(column, dict) else {}
        if not str(column.get("description", "")).strip():
            findings.append(
                _finding(
                    "error",
                    "column-description",
                    node,
                    f"column {column_name!r} is missing a description",
                )
            )
    return findings


def validate_manifest(path: Path) -> ConventionReport:
    """Validate dbt model conventions from a manifest artifact."""
    manifest = _load_manifest(path)
    models = _model_nodes(manifest)
    findings: list[ConventionFinding] = []
    names = Counter(str(node.get("name", "")) for node in models)
    for node in models:
        layer = _layer(node)
        findings.extend(_validate_identity(node, layer))
        findings.extend(_validate_dependencies(node, layer))
        findings.extend(_validate_wildcards(node, layer))
        raw_code = str(node.get("raw_code", ""))
        description = str(node.get("description", ""))
        meta = node.get("meta", {})
        meta = meta if isinstance(meta, dict) else {}
        if (
            "BLOCKING_TODO" in raw_code
            or "BLOCKING_TODO" in description
            or meta.get("scaffold_status") == "draft"
        ):
            findings.append(
                _finding("error", "blocking-todo", node, "scaffold draft is incomplete")
            )
        if layer == "marts":
            findings.extend(_validate_published_model(node))
        if names[str(node.get("name", ""))] > 1:
            findings.append(
                _finding("error", "duplicate-model-name", node, "model name is duplicated")
            )
    findings.append(
        ConventionFinding(
            severity="info",
            rule="models-checked",
            model="*",
            file=str(path),
            message=f"checked {len(models)} dbt models",
        )
    )
    ordered = tuple(sorted(findings, key=lambda item: (item.severity, item.model, item.rule)))
    status: Literal["passed", "failed"] = (
        "failed" if any(item.severity == "error" for item in ordered) else "passed"
    )
    return ConventionReport(
        status=status,
        manifest_path=str(path),
        checked_models=len(models),
        findings=ordered,
    )


def write_convention_report(report: ConventionReport, path: Path) -> None:
    """Write a convention report as stable, formatted JSON."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report.to_dict(), indent=2, sort_keys=True) + "\n", encoding="utf-8")
