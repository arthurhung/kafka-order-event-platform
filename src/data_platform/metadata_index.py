"""Build deterministic Phase 9 metadata and lineage artifacts."""

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

from pydantic import ValidationError

from data_platform.metadata_models import (
    AssetMetadata,
    ColumnMetadata,
    EvidenceArtifact,
    IndexSummary,
    LineageEdge,
    LineageGraph,
    MetadataIndex,
)
from data_platform.metadata_security import ArtifactSecurityError, load_json, resolve_allowlisted

ARTIFACT_PATHS = {
    "manifest": "dbt/target/manifest.json",
    "catalog": "dbt/target/catalog.json",
    "run_results": "dbt/target/run_results.json",
    "freshness": "dbt/target/sources.json",
    "quality": "reports/data-quality/phase7-ci-summary.json",
    "contract": "reports/data-quality/phase7-contract-diff.json",
    "consumer_lag": "reports/consumer-lag.json",
}
REPORT_PATTERNS = {
    "phase8a": "reports/data-quality/phase8a/*/*.json",
    "benchmark": "reports/runs/benchmark-*.json",
}
REQUIRED_DBT = ("manifest", "catalog", "run_results", "freshness")


class MetadataBuildError(ValueError):
    """Raised when a mandatory artifact is present but invalid."""


@dataclass(frozen=True)
class BuildResult:
    """The three deterministic outputs from one build."""

    index: MetadataIndex
    graph: LineageGraph
    summary: IndexSummary


def _object_map(value: object, label: str) -> dict[str, dict[str, Any]]:
    if not isinstance(value, dict):
        raise MetadataBuildError(f"{label} must be a JSON object")
    if not all(isinstance(key, str) and isinstance(item, dict) for key, item in value.items()):
        raise MetadataBuildError(f"{label} contains invalid entries")
    return cast(dict[str, dict[str, Any]], value)


def _string_list_map(value: object, label: str) -> dict[str, list[str]]:
    if not isinstance(value, dict):
        raise MetadataBuildError(f"{label} must be a JSON object")
    if not all(
        isinstance(key, str)
        and isinstance(item, list)
        and all(isinstance(entry, str) for entry in item)
        for key, item in value.items()
    ):
        raise MetadataBuildError(f"{label} contains invalid entries")
    return cast(dict[str, list[str]], value)


class MetadataBuilder:
    """Read only fixed repository artifacts and normalize dbt metadata."""

    def __init__(self, root: Path) -> None:
        """Create a builder rooted at the repository under inspection."""
        self.root = root.resolve()

    def build(self) -> BuildResult:
        """Build a deterministic index, graph, and validation summary."""
        documents: dict[str, dict[str, Any]] = {}
        missing: list[str] = []
        warnings: list[str] = []
        allowlist = set(ARTIFACT_PATHS.values())
        for name, relative in ARTIFACT_PATHS.items():
            path = resolve_allowlisted(self.root, relative, allowlist)
            if not path.is_file():
                missing.append(name)
                continue
            try:
                documents[name] = load_json(path)
            except ArtifactSecurityError as error:
                if name in REQUIRED_DBT:
                    raise MetadataBuildError(f"invalid {name} artifact: {error}") from error
                warnings.append(f"Ignored invalid optional {name} artifact: {error}")

        manifest = documents.get("manifest")
        if manifest is None:
            raise MetadataBuildError("manifest artifact is required to build the metadata index")
        assets = self._assets(manifest, documents, warnings)
        evidence = self._evidence(documents, warnings)
        graph = self._graph(assets)
        generated_at = self._generated_at(manifest)
        index = MetadataIndex(
            status="degraded" if missing else "complete",
            generated_at=generated_at,
            assets=assets,
            evidence_artifacts=evidence,
            missing_artifacts=sorted(missing),
            warnings=sorted(warnings),
        )
        summary = IndexSummary(
            status=index.status,
            generated_at=generated_at,
            asset_count=len(assets),
            edge_count=len(graph.edges),
            missing_artifacts=index.missing_artifacts,
            warnings=index.warnings,
        )
        return BuildResult(index=index, graph=graph, summary=summary)

    def _assets(
        self,
        manifest: dict[str, Any],
        documents: dict[str, dict[str, Any]],
        warnings: list[str],
    ) -> list[AssetMetadata]:
        nodes = _object_map(manifest.get("nodes"), "manifest.nodes")
        sources = _object_map(manifest.get("sources", {}), "manifest.sources")
        catalog = documents.get("catalog", {})
        catalog_nodes = _object_map(catalog.get("nodes", {}), "catalog.nodes")
        catalog_sources = _object_map(catalog.get("sources", {}), "catalog.sources")
        result_status, test_names, last_run = self._quality(documents.get("run_results"), nodes)
        freshness = self._freshness(documents.get("freshness"))
        child_map = _string_list_map(manifest.get("child_map", {}), "manifest.child_map")
        asset_nodes = {
            **{key: value for key, value in nodes.items() if value.get("resource_type") == "model"},
            **sources,
        }
        assets: list[AssetMetadata] = []
        for asset_id, node in sorted(asset_nodes.items()):
            resource_type = str(node.get("resource_type", "unknown"))
            node_catalog = (catalog_sources if resource_type == "source" else catalog_nodes).get(
                asset_id, {}
            )
            columns = self._columns(node, node_catalog)
            depends = node.get("depends_on", {})
            upstream = depends.get("nodes", []) if isinstance(depends, dict) else []
            upstream_assets = sorted(item for item in upstream if item in asset_nodes)
            downstream_assets = sorted(
                item for item in child_map.get(asset_id, []) if item in asset_nodes
            )
            config = node.get("config", {}) if isinstance(node.get("config"), dict) else {}
            meta = config.get("meta", {}) if isinstance(config.get("meta"), dict) else {}
            description = str(node.get("description") or "")
            status_counts = result_status.get(asset_id, {})
            assets.append(
                AssetMetadata(
                    asset_id=asset_id,
                    name=str(node.get("name") or asset_id.rsplit(".", 1)[-1]),
                    resource_type=resource_type,
                    layer=self._layer(node),
                    owner=self._optional_string(meta.get("owner")),
                    domain=self._optional_string(meta.get("domain")),
                    data_product=self._optional_string(meta.get("data_product")),
                    description=description,
                    grain=self._grain(description),
                    maturity=self._optional_string(meta.get("maturity")),
                    materialization=self._optional_string(config.get("materialized")),
                    slo=cast(dict[str, Any], meta.get("sla", {}))
                    if isinstance(meta.get("sla", {}), dict)
                    else {},
                    columns=columns,
                    upstream=upstream_assets,
                    downstream=downstream_assets,
                    quality_status=self._overall_quality(status_counts),
                    quality_counts=status_counts,
                    freshness_status=freshness.get(asset_id),
                    contract_status="enforced"
                    if isinstance(config.get("contract"), dict)
                    and config["contract"].get("enforced") is True
                    else "not_enforced",
                    tests=sorted(test_names.get(asset_id, [])),
                    last_run_at=last_run,
                    source_artifacts=self._source_artifacts(asset_id, documents),
                )
            )
        if not catalog:
            warnings.append(
                "catalog is missing; declared manifest column types are used when available"
            )
        return assets

    @staticmethod
    def _columns(node: dict[str, Any], catalog: dict[str, Any]) -> list[ColumnMetadata]:
        declared = _object_map(node.get("columns", {}), "node.columns")
        observed = _object_map(catalog.get("columns", {}), "catalog.columns")
        result: list[ColumnMetadata] = []
        for name, column in declared.items():
            constraints = column.get("constraints", [])
            not_null = isinstance(constraints, list) and any(
                isinstance(item, dict) and item.get("type") == "not_null" for item in constraints
            )
            observed_column = observed.get(name, {})
            result.append(
                ColumnMetadata(
                    name=name,
                    data_type=cast(
                        str | None, observed_column.get("type") or column.get("data_type")
                    ),
                    nullable=not not_null,
                    description=str(column.get("description") or ""),
                )
            )
        return result

    @staticmethod
    def _quality(
        run_results: dict[str, Any] | None, nodes: dict[str, dict[str, Any]]
    ) -> tuple[dict[str, dict[str, int]], dict[str, list[str]], datetime | None]:
        if run_results is None:
            return {}, {}, None
        results = run_results.get("results")
        if not isinstance(results, list):
            raise MetadataBuildError("run_results.results must be a list")
        counts: dict[str, dict[str, int]] = {}
        names: dict[str, list[str]] = {}
        for item in results:
            if not isinstance(item, dict):
                raise MetadataBuildError("run_results contains a non-object result")
            test_id = str(item.get("unique_id") or "")
            test_node = nodes.get(test_id, {})
            depends = test_node.get("depends_on", {})
            parents = depends.get("nodes", []) if isinstance(depends, dict) else []
            for parent in parents:
                parent_counts = counts.setdefault(parent, {})
                status = str(item.get("status") or "unknown")
                parent_counts[status] = parent_counts.get(status, 0) + 1
                names.setdefault(parent, []).append(test_id)
        metadata = run_results.get("metadata", {})
        generated = metadata.get("generated_at") if isinstance(metadata, dict) else None
        return counts, names, _parse_datetime(generated)

    @staticmethod
    def _freshness(document: dict[str, Any] | None) -> dict[str, str]:
        if document is None:
            return {}
        results = document.get("results")
        if not isinstance(results, list):
            raise MetadataBuildError("freshness.results must be a list")
        return {
            str(item.get("unique_id")): str(item.get("status", "unknown"))
            for item in results
            if isinstance(item, dict) and item.get("unique_id")
        }

    @staticmethod
    def _overall_quality(counts: dict[str, int]) -> str:
        if not counts:
            return "unknown"
        if any(counts.get(name, 0) for name in ("error", "fail", "runtime error")):
            return "fail"
        if counts.get("warn", 0):
            return "warn"
        if counts.get("pass", 0) or counts.get("success", 0):
            return "pass"
        return "unknown"

    @staticmethod
    def _layer(node: dict[str, Any]) -> str:
        if node.get("resource_type") == "source":
            return "source"
        fqn = node.get("fqn", [])
        return str(fqn[1]) if isinstance(fqn, list) and len(fqn) > 1 else "unknown"

    @staticmethod
    def _grain(description: str) -> str | None:
        if description.lower().startswith("grain:"):
            return description.split(".", 1)[0].strip()
        return None

    @staticmethod
    def _optional_string(value: object) -> str | None:
        return str(value) if value is not None else None

    @staticmethod
    def _source_artifacts(asset_id: str, documents: dict[str, dict[str, Any]]) -> list[str]:
        result = ["dbt_manifest"]
        if "catalog" in documents:
            result.append("dbt_catalog")
        if "run_results" in documents:
            result.append("dbt_run_results")
        if asset_id.startswith("source.") and "freshness" in documents:
            result.append("dbt_source_freshness")
        if "quality" in documents:
            result.append("phase7_ci_summary")
        if "contract" in documents:
            result.append("contract_diff_report")
        return sorted(result)

    def _evidence(
        self, documents: dict[str, dict[str, Any]], warnings: list[str]
    ) -> list[EvidenceArtifact]:
        evidence: list[EvidenceArtifact] = []
        for name in ("quality", "contract", "consumer_lag"):
            document = documents.get(name)
            if document is not None:
                evidence.append(self._evidence_record(ARTIFACT_PATHS[name], document))
        for label, pattern in REPORT_PATTERNS.items():
            for path in sorted(self.root.glob(pattern)):
                resolved = path.resolve()
                if not resolved.is_file() or not resolved.is_relative_to(self.root):
                    warnings.append(f"Ignored unsafe {label} report path")
                    continue
                try:
                    document = load_json(resolved)
                except ArtifactSecurityError as error:
                    warnings.append(f"Ignored invalid {label} report: {error}")
                    continue
                evidence.append(
                    self._evidence_record(resolved.relative_to(self.root).as_posix(), document)
                )
        return sorted(evidence, key=lambda item: item.artifact_id)

    @staticmethod
    def _evidence_record(relative: str, document: dict[str, Any]) -> EvidenceArtifact:
        report_type = str(document.get("report_type") or Path(relative).stem)
        source_level = str(document.get("evidence_level") or "local_execution")
        observed = (
            document.get("generated_at")
            or document.get("observed_at")
            or document.get("finished_at")
        )
        return EvidenceArtifact(
            artifact_id=relative,
            report_type=report_type,
            status=str(document.get("status") or "unknown"),
            source_evidence_level=source_level,
            observed_at=_parse_datetime(observed),
        )

    @staticmethod
    def _graph(assets: list[AssetMetadata]) -> LineageGraph:
        edges = sorted(
            (
                LineageEdge(upstream=upstream, downstream=asset.asset_id)
                for asset in assets
                for upstream in asset.upstream
            ),
            key=lambda edge: (edge.upstream, edge.downstream),
        )
        return LineageGraph(nodes=sorted(asset.asset_id for asset in assets), edges=edges)

    @staticmethod
    def _generated_at(manifest: dict[str, Any]) -> datetime:
        metadata = manifest.get("metadata", {})
        generated = metadata.get("generated_at") if isinstance(metadata, dict) else None
        return _parse_datetime(generated) or datetime(1970, 1, 1, tzinfo=UTC)


def _parse_datetime(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)


def write_build(result: BuildResult, output_dir: Path) -> None:
    """Write stable JSON files without absolute paths or overwrite ambiguity."""
    output_dir.mkdir(parents=True, exist_ok=True)
    payloads = {
        "metadata-index.json": result.index.model_dump(mode="json"),
        "lineage-graph.json": result.graph.model_dump(mode="json"),
        "index-summary.json": result.summary.model_dump(mode="json"),
    }
    for name, payload in payloads.items():
        (output_dir / name).write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )


def validate_build(output_dir: Path) -> IndexSummary:
    """Validate all emitted schemas and cross-file lineage consistency."""
    try:
        index = MetadataIndex.model_validate_json((output_dir / "metadata-index.json").read_text())
        graph = LineageGraph.model_validate_json((output_dir / "lineage-graph.json").read_text())
        summary = IndexSummary.model_validate_json((output_dir / "index-summary.json").read_text())
    except (OSError, ValidationError) as error:
        raise MetadataBuildError("metadata output validation failed") from error
    asset_ids = {asset.asset_id for asset in index.assets}
    if set(graph.nodes) != asset_ids:
        raise MetadataBuildError("lineage nodes do not match metadata assets")
    if any(
        edge.upstream not in asset_ids or edge.downstream not in asset_ids for edge in graph.edges
    ):
        raise MetadataBuildError("lineage edge references an unknown asset")
    if summary.asset_count != len(index.assets) or summary.edge_count != len(graph.edges):
        raise MetadataBuildError("index summary counts are inconsistent")
    return summary
