"""Compare published dbt contracts using two manifest artifacts."""

from __future__ import annotations

import json
from collections import deque
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Literal

Severity = Literal["blocking", "manual_review", "info"]
Classification = Literal["breaking", "potentially_breaking", "non_breaking"]


class ContractInputError(ValueError):
    """Raised when contract input artifacts are missing or malformed."""


@dataclass(frozen=True, slots=True)
class ContractFinding:
    """One contract difference and its downstream impact."""

    model: str
    column: str | None
    change_type: str
    severity: Severity
    classification: Classification
    downstream_impact_paths: tuple[tuple[str, ...], ...]
    evidence: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ContractReport:
    """Machine-readable result of comparing two manifests."""

    status: Literal["passed", "blocked", "previous_state_unavailable"]
    previous_manifest: str | None
    current_manifest: str
    previous_git_sha: str | None
    current_git_sha: str | None
    findings: tuple[ContractFinding, ...]

    @property
    def blocking_count(self) -> int:
        """Return the number of blocking contract changes."""
        return sum(item.severity == "blocking" for item in self.findings)

    def to_dict(self) -> dict[str, object]:
        """Return a JSON-serializable report."""
        return {
            "report_type": "dbt_contract_comparison",
            "schema_version": 1,
            "status": self.status,
            "previous_manifest": self.previous_manifest,
            "current_manifest": self.current_manifest,
            "previous_git_sha": self.previous_git_sha,
            "current_git_sha": self.current_git_sha,
            "blocking_count": self.blocking_count,
            "findings": [asdict(item) for item in self.findings],
        }


def _load_manifest(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ContractInputError(f"cannot read manifest {path}: {error}") from error
    if not isinstance(value, dict) or not isinstance(value.get("nodes"), dict):
        raise ContractInputError("manifest must contain an object-valued nodes field")
    return value


def _published_models(manifest: dict[str, object]) -> dict[str, dict[str, object]]:
    nodes = manifest["nodes"]
    if not isinstance(nodes, dict):
        raise ContractInputError("manifest nodes must be an object")
    return {
        unique_id: node
        for unique_id, node in nodes.items()
        if isinstance(node, dict)
        and node.get("resource_type") == "model"
        and str(node.get("original_file_path", "")).startswith("models/marts/")
    }


def _required(column: dict[str, object]) -> bool:
    constraints = column.get("constraints", [])
    if isinstance(constraints, list) and any(
        isinstance(item, dict) and item.get("type") == "not_null" for item in constraints
    ):
        return True
    tests = column.get("data_tests", column.get("tests", []))
    return isinstance(tests, list) and "not_null" in tests


def _tests_for_model(manifest: dict[str, object], model_id: str) -> set[str]:
    nodes = manifest["nodes"]
    if not isinstance(nodes, dict):
        raise ContractInputError("manifest nodes must be an object")
    tests: set[str] = set()
    for node in nodes.values():
        if not isinstance(node, dict) or node.get("resource_type") != "test":
            continue
        depends_on = node.get("depends_on", {})
        dependencies = depends_on.get("nodes", []) if isinstance(depends_on, dict) else []
        if model_id not in dependencies:
            continue
        metadata = node.get("test_metadata", {})
        if not isinstance(metadata, dict):
            continue
        name = str(metadata.get("name", "unknown"))
        kwargs = metadata.get("kwargs", {})
        kwargs = kwargs if isinstance(kwargs, dict) else {}
        column = node.get("column_name") or kwargs.get("column_name")
        columns = kwargs.get("columns")
        suffix = str(column or columns or "model")
        tests.add(f"{name}:{suffix}")
    return tests


def _business_keys(manifest: dict[str, object], model_id: str) -> set[str]:
    return {
        value
        for value in _tests_for_model(manifest, model_id)
        if value.startswith("unique:") or value.startswith("unique_combination:")
    }


def _grain(node: dict[str, object]) -> str:
    return str(node.get("description", "")).split(".", maxsplit=1)[0].strip()


def _downstream_paths(
    manifest: dict[str, object], start_id: str, *, max_depth: int = 10
) -> tuple[tuple[str, ...], ...]:
    child_map = manifest.get("child_map", {})
    child_map = child_map if isinstance(child_map, dict) else {}
    nodes = manifest.get("nodes", {})
    nodes = nodes if isinstance(nodes, dict) else {}
    start = nodes.get(start_id, {})
    start_name = str(start.get("name", start_id)) if isinstance(start, dict) else start_id
    paths: list[tuple[str, ...]] = []
    queue: deque[tuple[str, tuple[str, ...]]] = deque([(start_id, (start_name,))])
    visited = {start_id}
    while queue:
        node_id, path = queue.popleft()
        if len(path) > max_depth:
            continue
        children = child_map.get(node_id, [])
        if not isinstance(children, list):
            continue
        for child_id in children:
            child = nodes.get(child_id, {})
            if not isinstance(child, dict) or child.get("resource_type") != "model":
                continue
            child_name = str(child.get("name", child_id))
            child_path = (*path, child_name)
            paths.append(child_path)
            if child_id not in visited:
                visited.add(child_id)
                queue.append((child_id, child_path))
    return tuple(sorted(paths))


def _finding(
    node: dict[str, object],
    model_id: str,
    manifest: dict[str, object],
    *,
    column: str | None,
    change_type: str,
    severity: Severity,
    classification: Classification,
    evidence: tuple[str, ...],
) -> ContractFinding:
    return ContractFinding(
        model=str(node.get("name", model_id)),
        column=column,
        change_type=change_type,
        severity=severity,
        classification=classification,
        downstream_impact_paths=_downstream_paths(manifest, model_id),
        evidence=evidence,
    )


def _compare_columns(
    model_id: str,
    previous_node: dict[str, object],
    current_node: dict[str, object],
    previous_manifest: dict[str, object],
) -> list[ContractFinding]:
    findings: list[ContractFinding] = []
    old_columns = previous_node.get("columns", {})
    new_columns = current_node.get("columns", {})
    old_columns = old_columns if isinstance(old_columns, dict) else {}
    new_columns = new_columns if isinstance(new_columns, dict) else {}
    removed = sorted(set(old_columns) - set(new_columns))
    added = sorted(set(new_columns) - set(old_columns))
    rename_pair = None
    if len(removed) == 1 and len(added) == 1:
        old_type = str(old_columns[removed[0]].get("data_type", "")).lower()
        new_type = str(new_columns[added[0]].get("data_type", "")).lower()
        if old_type and old_type == new_type:
            rename_pair = (removed[0], added[0])
            findings.append(
                _finding(
                    previous_node,
                    model_id,
                    previous_manifest,
                    column=f"{removed[0]} -> {added[0]}",
                    change_type="published_column_rename",
                    severity="blocking",
                    classification="breaking",
                    evidence=("one column removed and one same-typed column added",),
                )
            )
    for column_name in removed:
        if rename_pair and column_name == rename_pair[0]:
            continue
        findings.append(
            _finding(
                previous_node,
                model_id,
                previous_manifest,
                column=column_name,
                change_type="published_column_removed",
                severity="blocking",
                classification="breaking",
                evidence=("column exists in previous manifest only",),
            )
        )
    for column_name in added:
        if rename_pair and column_name == rename_pair[1]:
            continue
        current_column = new_columns[column_name]
        is_required = isinstance(current_column, dict) and _required(current_column)
        findings.append(
            _finding(
                current_node,
                model_id,
                previous_manifest,
                column=column_name,
                change_type="required_column_added" if is_required else "nullable_column_added",
                severity="manual_review" if is_required else "info",
                classification="potentially_breaking" if is_required else "non_breaking",
                evidence=("column exists in current manifest only",),
            )
        )
    for column_name in sorted(set(old_columns) & set(new_columns)):
        old_column = old_columns[column_name]
        new_column = new_columns[column_name]
        if not isinstance(old_column, dict) or not isinstance(new_column, dict):
            continue
        old_type = str(old_column.get("data_type", "")).lower()
        new_type = str(new_column.get("data_type", "")).lower()
        if old_type != new_type:
            findings.append(
                _finding(
                    previous_node,
                    model_id,
                    previous_manifest,
                    column=column_name,
                    change_type="incompatible_data_type_change",
                    severity="blocking",
                    classification="breaking",
                    evidence=(f"previous={old_type}", f"current={new_type}"),
                )
            )
        if not _required(old_column) and _required(new_column):
            findings.append(
                _finding(
                    previous_node,
                    model_id,
                    previous_manifest,
                    column=column_name,
                    change_type="nullable_to_required",
                    severity="blocking",
                    classification="breaking",
                    evidence=("current constraints/tests require non-null values",),
                )
            )
        old_description = str(old_column.get("description", "")).strip()
        new_description = str(new_column.get("description", "")).strip()
        metric_column = column_name.endswith(("_count", "_amount", "_rate", "_ms"))
        if metric_column and old_description != new_description:
            findings.append(
                _finding(
                    current_node,
                    model_id,
                    previous_manifest,
                    column=column_name,
                    change_type="metric_description_changed",
                    severity="manual_review",
                    classification="potentially_breaking",
                    evidence=("manifest text changed; business semantics require human review",),
                )
            )
    return findings


def compare_contracts(
    previous_path: Path | None,
    current_path: Path,
    *,
    previous_git_sha: str | None = None,
    current_git_sha: str | None = None,
) -> ContractReport:
    """Compare published contracts and classify deterministic changes."""
    current = _load_manifest(current_path)
    if previous_path is None or not previous_path.is_file():
        return ContractReport(
            status="previous_state_unavailable",
            previous_manifest=str(previous_path) if previous_path else None,
            current_manifest=str(current_path),
            previous_git_sha=previous_git_sha,
            current_git_sha=current_git_sha,
            findings=(),
        )
    previous = _load_manifest(previous_path)
    old_models = _published_models(previous)
    new_models = _published_models(current)
    findings: list[ContractFinding] = []
    for model_id, old_node in old_models.items():
        new_node = new_models.get(model_id)
        if new_node is None:
            findings.append(
                _finding(
                    old_node,
                    model_id,
                    previous,
                    column=None,
                    change_type="published_model_removed",
                    severity="blocking",
                    classification="breaking",
                    evidence=("published model exists in previous manifest only",),
                )
            )
            continue
        findings.extend(_compare_columns(model_id, old_node, new_node, previous))
        old_contract = old_node.get("contract", {})
        new_contract = new_node.get("contract", {})
        if (
            isinstance(old_contract, dict)
            and old_contract.get("enforced") is True
            and (not isinstance(new_contract, dict) or new_contract.get("enforced") is not True)
        ):
            findings.append(
                _finding(
                    old_node,
                    model_id,
                    previous,
                    column=None,
                    change_type="contract_removed",
                    severity="blocking",
                    classification="breaking",
                    evidence=("previous contract was enforced; current contract is not",),
                )
            )
        if _grain(old_node) != _grain(new_node):
            findings.append(
                _finding(
                    old_node,
                    model_id,
                    previous,
                    column=None,
                    change_type="grain_changed",
                    severity="blocking",
                    classification="breaking",
                    evidence=(f"previous={_grain(old_node)}", f"current={_grain(new_node)}"),
                )
            )
        old_keys = _business_keys(previous, model_id)
        new_keys = _business_keys(current, model_id)
        if old_keys != new_keys:
            findings.append(
                _finding(
                    old_node,
                    model_id,
                    previous,
                    column=None,
                    change_type="business_key_changed",
                    severity="blocking",
                    classification="breaking",
                    evidence=(f"previous={sorted(old_keys)}", f"current={sorted(new_keys)}"),
                )
            )
        removed_tests = sorted(
            _tests_for_model(previous, model_id) - _tests_for_model(current, model_id)
        )
        if removed_tests:
            findings.append(
                _finding(
                    old_node,
                    model_id,
                    previous,
                    column=None,
                    change_type="required_test_removed",
                    severity="blocking",
                    classification="breaking",
                    evidence=tuple(removed_tests),
                )
            )
        old_config = old_node.get("config", {})
        new_config = new_node.get("config", {})
        old_config = old_config if isinstance(old_config, dict) else {}
        new_config = new_config if isinstance(new_config, dict) else {}
        for key, change_type in (
            ("materialized", "materialization_changed"),
            ("unique_key", "incremental_key_changed"),
        ):
            if old_config.get(key) != new_config.get(key):
                findings.append(
                    _finding(
                        new_node,
                        model_id,
                        previous,
                        column=None,
                        change_type=change_type,
                        severity="manual_review",
                        classification="potentially_breaking",
                        evidence=(
                            f"previous={old_config.get(key)}",
                            f"current={new_config.get(key)}",
                        ),
                    )
                )
        old_meta = old_node.get("meta", {})
        new_meta = new_node.get("meta", {})
        old_meta = old_meta if isinstance(old_meta, dict) else {}
        new_meta = new_meta if isinstance(new_meta, dict) else {}
        for key, change_type in (("owner", "owner_changed"), ("sla", "slo_changed")):
            if old_meta.get(key) != new_meta.get(key):
                findings.append(
                    _finding(
                        new_node,
                        model_id,
                        previous,
                        column=None,
                        change_type=change_type,
                        severity="manual_review",
                        classification="potentially_breaking",
                        evidence=(f"previous={old_meta.get(key)}", f"current={new_meta.get(key)}"),
                    )
                )
    ordered = tuple(
        sorted(findings, key=lambda item: (item.model, item.change_type, item.column or ""))
    )
    status: Literal["passed", "blocked", "previous_state_unavailable"] = (
        "blocked" if any(item.severity == "blocking" for item in ordered) else "passed"
    )
    return ContractReport(
        status=status,
        previous_manifest=str(previous_path),
        current_manifest=str(current_path),
        previous_git_sha=previous_git_sha,
        current_git_sha=current_git_sha,
        findings=ordered,
    )


def write_contract_report(report: ContractReport, path: Path) -> None:
    """Write a stable JSON contract report."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report.to_dict(), indent=2, sort_keys=True) + "\n", encoding="utf-8")
