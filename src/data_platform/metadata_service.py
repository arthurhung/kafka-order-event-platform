"""Read-only Phase 9 data discovery operations and audit handling."""

import json
import time
import uuid
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeoutError
from datetime import datetime
from pathlib import Path
from typing import Any, Literal, cast

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

from data_platform.metadata_models import AssetMetadata, MetadataIndex, ToolResponse, iso_now
from data_platform.metadata_security import (
    ArtifactSecurityError,
    bounded_json,
    load_json,
    redact,
)

MAX_SEARCH_LIMIT = 50
MAX_LINEAGE_DEPTH = 8
MAX_LINEAGE_NODES = 200
SQL_MARKERS = ("select ", "insert ", "update ", "delete ", "drop ", "alter ", ";")


class StrictInput(BaseModel):
    """Base for MCP inputs that reject undeclared fields."""

    model_config = ConfigDict(extra="forbid")


class SearchInput(StrictInput):
    """Validated search filters."""

    query: str = Field(min_length=1, max_length=100)
    resource_types: list[str] = Field(default_factory=list, max_length=10)
    owner: str | None = Field(default=None, max_length=100)
    domain: str | None = Field(default=None, max_length=100)
    limit: int = Field(default=20, ge=1, le=MAX_SEARCH_LIMIT)

    @field_validator("query")
    @classmethod
    def reject_sql(cls, value: str) -> str:
        """Reject SQL-shaped input; search accepts plain keywords only."""
        lowered = value.strip().lower()
        if any(marker in lowered for marker in SQL_MARKERS):
            raise ValueError("query must be a plain metadata keyword, not SQL")
        return value.strip()


class ModelInput(StrictInput):
    """Validated exact model lookup."""

    model_name: str = Field(min_length=1, max_length=150, pattern=r"^[A-Za-z0-9_.-]+$")


class LineageInput(ModelInput):
    """Validated bounded lineage request."""

    direction: Literal["upstream", "downstream", "both"] = "both"
    max_depth: int = Field(default=3, ge=1, le=MAX_LINEAGE_DEPTH)


class ImpactInput(ModelInput):
    """Validated bounded downstream request."""

    max_depth: int = Field(default=5, ge=1, le=MAX_LINEAGE_DEPTH)


class QualityInput(ModelInput):
    """Validated quality lookup."""

    include_history: bool = False


class FailureInput(StrictInput):
    """Validated pipeline failure lookup."""

    pipeline_name: str = Field(min_length=1, max_length=150, pattern=r"^[A-Za-z0-9_.-]+$")
    limit: int = Field(default=10, ge=1, le=50)


class LagInput(StrictInput):
    """Validated consumer lag lookup."""

    consumer_group: str = Field(min_length=1, max_length=150, pattern=r"^[A-Za-z0-9_.-]+$")


class CostInput(ModelInput):
    """Validated evidence-level cost lookup."""

    preferred_evidence_level: Literal[
        "best_available", "simulated", "sandbox_observed", "cloud_observed"
    ] = "best_available"


INPUT_MODELS: dict[str, type[StrictInput]] = {
    "search_data_assets": SearchInput,
    "get_model_schema": ModelInput,
    "get_model_owner": ModelInput,
    "get_lineage": LineageInput,
    "get_upstream_lineage": ImpactInput,
    "get_downstream_impact": ImpactInput,
    "get_quality_status": QualityInput,
    "get_recent_pipeline_failures": FailureInput,
    "get_consumer_lag": LagInput,
    "get_cost_estimate": CostInput,
}


class MetadataService:
    """Serve bounded read-only queries from normalized local artifacts."""

    def __init__(self, root: Path, index: MetadataIndex, *, timeout_seconds: float = 5.0) -> None:
        """Create a bounded service rooted at a fixed repository."""
        self.root = root.resolve()
        self.index = index
        self.timeout_seconds = timeout_seconds
        self.assets = {asset.asset_id: asset for asset in index.assets}
        self.audit_path = self._fixed_path("reports/metadata/mcp-audit.jsonl")

    @classmethod
    def from_output(cls, root: Path, *, timeout_seconds: float = 5.0) -> MetadataService:
        """Load the fixed normalized index path."""
        service_root = root.resolve()
        path = (service_root / "reports/metadata/metadata-index.json").resolve()
        if not path.is_relative_to(service_root):
            raise ArtifactSecurityError("metadata index path escapes repository root")
        return cls(
            root,
            MetadataIndex.model_validate_json(path.read_text()),
            timeout_seconds=timeout_seconds,
        )

    def call(self, tool: str, arguments: dict[str, Any]) -> dict[str, Any]:
        """Validate, time-bound, redact, size-bound, and audit one tool call."""
        request_id = str(uuid.uuid4())
        started = time.monotonic()
        response: ToolResponse
        error_category: str | None = None
        try:
            model_type = INPUT_MODELS.get(tool)
            if model_type is None:
                response = self._response("invalid_request", warnings=["unknown tool"])
                error_category = "unknown_tool"
            else:
                validated = model_type.model_validate(arguments)
                executor = ThreadPoolExecutor(max_workers=1)
                future = executor.submit(self._dispatch, tool, validated)
                try:
                    response = future.result(timeout=self.timeout_seconds)
                except FutureTimeoutError:
                    future.cancel()
                    response = self._response(
                        "error", warnings=["request exceeded the configured timeout"]
                    )
                    error_category = "timeout"
                finally:
                    executor.shutdown(wait=False, cancel_futures=True)
        except ValidationError as error:
            response = self._response(
                "invalid_request",
                data={"validation_errors": self._safe_validation_errors(error)},
                warnings=["request schema validation failed"],
            )
            error_category = "validation"
        except (ArtifactSecurityError, OSError, ValueError) as error:
            response = self._response(
                "error", warnings=[f"sanitized {type(error).__name__} while reading metadata"]
            )
            error_category = type(error).__name__
        payload = cast(dict[str, Any], redact(response.model_dump(mode="json")))
        try:
            result_size = len(bounded_json(payload).encode("utf-8"))
        except ArtifactSecurityError:
            response = self._response("error", warnings=["result exceeded configured size limit"])
            payload = response.model_dump(mode="json")
            result_size = len(bounded_json(payload).encode("utf-8"))
            error_category = "result_too_large"
        self._audit(
            request_id=request_id,
            tool=tool,
            arguments=arguments,
            response=response,
            duration_ms=round((time.monotonic() - started) * 1000, 3),
            result_size=result_size,
            error_category=error_category,
        )
        return payload

    def _dispatch(self, tool: str, request: StrictInput) -> ToolResponse:
        handlers: dict[str, Callable[[Any], ToolResponse]] = {
            "search_data_assets": self._search,
            "get_model_schema": self._schema,
            "get_model_owner": self._owner,
            "get_lineage": self._lineage,
            "get_upstream_lineage": self._upstream,
            "get_downstream_impact": self._impact,
            "get_quality_status": self._quality,
            "get_recent_pipeline_failures": self._failures,
            "get_consumer_lag": self._lag,
            "get_cost_estimate": self._cost,
        }
        return handlers[tool](request)

    def _search(self, request: SearchInput) -> ToolResponse:
        query = request.query.casefold()
        matches: list[dict[str, Any]] = []
        for asset in self.index.assets:
            if request.resource_types and asset.resource_type not in request.resource_types:
                continue
            if request.owner and asset.owner != request.owner:
                continue
            if request.domain and asset.domain != request.domain:
                continue
            reasons: list[str] = []
            if query == asset.name.casefold() or query == asset.asset_id.casefold():
                reasons.append("exact_name")
            elif query in asset.name.casefold():
                reasons.append("partial_name")
            if query in asset.description.casefold():
                reasons.append("description")
            if any(query in column.name.casefold() for column in asset.columns):
                reasons.append("column")
            if reasons:
                matches.append(
                    {
                        "asset_id": asset.asset_id,
                        "name": asset.name,
                        "resource_type": asset.resource_type,
                        "match_reasons": sorted(reasons),
                        "owner": asset.owner,
                        "description": asset.description,
                        "quality_status": asset.quality_status,
                        "evidence_artifact_ids": asset.source_artifacts,
                    }
                )
        matches.sort(key=lambda item: ("exact_name" not in item["match_reasons"], item["name"]))
        return self._response("ok", data={"matches": matches[: request.limit]})

    def _schema(self, request: ModelInput) -> ToolResponse:
        asset, response = self._find(request.model_name)
        if response:
            return response
        if asset is None:
            raise RuntimeError("asset lookup invariant failed")
        return self._response(
            "ok",
            data={
                "asset_id": asset.asset_id,
                "name": asset.name,
                "grain": asset.grain,
                "materialization": asset.materialization,
                "columns": [column.model_dump(mode="json") for column in asset.columns],
                "contract_status": asset.contract_status,
                "tests": asset.tests,
            },
            evidence=asset.source_artifacts,
        )

    def _owner(self, request: ModelInput) -> ToolResponse:
        asset, response = self._find(request.model_name)
        if response:
            return response
        if asset is None:
            raise RuntimeError("asset lookup invariant failed")
        status: Literal["ok", "partial"] = "ok" if asset.owner else "partial"
        warnings = [] if asset.owner else ["owner metadata is missing"]
        return self._response(
            status,
            data={
                "owner": asset.owner,
                "domain": asset.domain,
                "data_product": asset.data_product,
                "maturity": asset.maturity,
                "slo": asset.slo,
                "source_metadata_location": "dbt_manifest",
            },
            evidence=["dbt_manifest"],
            warnings=warnings,
        )

    def _lineage(self, request: LineageInput) -> ToolResponse:
        asset, response = self._find(request.model_name)
        if response:
            return response
        if asset is None:
            raise RuntimeError("asset lookup invariant failed")
        upstream, up_edges, up_truncated = self._walk(asset.asset_id, "upstream", request.max_depth)
        downstream, down_edges, down_truncated = self._walk(
            asset.asset_id, "downstream", request.max_depth
        )
        if request.direction == "upstream":
            downstream, down_edges = [], []
        elif request.direction == "downstream":
            upstream, up_edges = [], []
        return self._response(
            "ok",
            data={
                "model": asset.asset_id,
                "upstream_nodes": upstream,
                "downstream_nodes": downstream,
                "edges": sorted(up_edges + down_edges, key=lambda item: (item[0], item[1])),
                "truncated": up_truncated or down_truncated,
            },
            evidence=["lineage_graph", "dbt_manifest"],
        )

    def _upstream(self, request: ImpactInput) -> ToolResponse:
        return self._lineage(
            LineageInput(
                model_name=request.model_name, max_depth=request.max_depth, direction="upstream"
            )
        )

    def _impact(self, request: ImpactInput) -> ToolResponse:
        asset, response = self._find(request.model_name)
        if response:
            return response
        if asset is None:
            raise RuntimeError("asset lookup invariant failed")
        nodes, edges, truncated = self._walk(asset.asset_id, "downstream", request.max_depth)
        impacted = [self.assets[node] for node in nodes]
        return self._response(
            "ok",
            data={
                "model": asset.asset_id,
                "impacted_assets": [item.asset_id for item in impacted],
                "published_marts": [item.asset_id for item in impacted if item.layer == "marts"],
                "exposures": [],
                "impact_paths": edges,
                "quality_and_maturity": [
                    {
                        "asset_id": item.asset_id,
                        "quality_status": item.quality_status,
                        "maturity": item.maturity,
                    }
                    for item in impacted
                ],
                "truncated": truncated,
            },
            evidence=["lineage_graph", "dbt_manifest"],
        )

    def _quality(self, request: QualityInput) -> ToolResponse:
        asset, response = self._find(request.model_name)
        if response:
            return response
        if asset is None:
            raise RuntimeError("asset lookup invariant failed")
        warnings = list(self.index.warnings)
        if request.include_history:
            warnings.append(
                "quality history is not available; only the latest allowlisted run is indexed"
            )
        return self._response(
            "partial" if warnings else "ok",
            data={
                "overall_status": asset.quality_status,
                "tests": asset.quality_counts,
                "freshness": asset.freshness_status,
                "contract_status": asset.contract_status,
                "last_run": asset.last_run_at,
                "failure_evidence": [
                    test for test in asset.tests if asset.quality_status == "fail"
                ],
                "stale_artifact_warning": self._is_stale(asset.last_run_at),
                "history": None,
            },
            evidence=asset.source_artifacts,
            warnings=warnings,
        )

    def _failures(self, request: FailureInput) -> ToolResponse:
        reports = self._reports("reports/data-quality/phase8a/*/phase8a-orchestration-report.json")
        failures: list[dict[str, Any]] = []
        for artifact_id, report in reports:
            tasks = report.get("task_results", report.get("tasks", []))
            if not isinstance(tasks, list):
                continue
            for task in tasks:
                if isinstance(task, dict) and str(task.get("status")) in {"failed", "error"}:
                    failures.append(
                        {
                            "run_id": report.get("run_id"),
                            "task": task.get("task_id", task.get("name")),
                            "status": task.get("status"),
                            "started_at": task.get("started_at"),
                            "ended_at": task.get("ended_at"),
                            "error_category": task.get(
                                "error_category", "local_validation_failure"
                            ),
                            "artifact_reference": artifact_id,
                        }
                    )
        warnings = ["Phase 8C cloud Airflow artifact is not available"]
        if request.pipeline_name != "retail_data_platform_pipeline":
            return self._response("not_found", warnings=["pipeline is not indexed"])
        return self._response(
            "partial" if failures else "not_available",
            data={
                "pipeline_name": request.pipeline_name,
                "failures": failures[-request.limit :],
                "cloud_execution_status": "not_available",
            },
            evidence=sorted({item["artifact_reference"] for item in failures}),
            evidence_level="not_available" if not failures else "static_validation",
            warnings=warnings,
        )

    def _lag(self, request: LagInput) -> ToolResponse:
        path = self._fixed_path("reports/consumer-lag.json")
        if not path.is_file():
            return self._response(
                "not_available",
                data={
                    "consumer_group": request.consumer_group,
                    "cloud_execution_status": "not_available",
                },
                evidence_level="not_available",
                warnings=["consumer lag report is missing; no live Kafka query was attempted"],
            )
        report = load_json(path)
        groups = report.get("groups")
        if not isinstance(groups, list):
            raise ValueError("consumer lag groups must be a list")
        matching = [
            item
            for item in groups
            if isinstance(item, dict) and item.get("consumer_group") == request.consumer_group
        ]
        if not matching:
            return self._response(
                "not_found", warnings=["consumer group is absent from the report"]
            )
        rows: list[dict[str, Any]] = []
        for group in matching:
            for row in group.get("rows", []):
                if isinstance(row, dict):
                    rows.append(
                        {
                            "topic": row.get("topic"),
                            "partition": row.get("partition"),
                            "committed_offset": row.get("current_offset"),
                            "log_end_offset": row.get("log_end_offset"),
                            "lag": row.get("lag"),
                            "status": row.get("status"),
                        }
                    )
        return self._response(
            "ok",
            data={
                "consumer_group": request.consumer_group,
                "observation_timestamp": report.get("observed_at"),
                "partitions": rows,
            },
            evidence=["consumer_lag_report"],
        )

    def _cost(self, request: CostInput) -> ToolResponse:
        desired = request.preferred_evidence_level
        if desired in {"sandbox_observed", "cloud_observed"}:
            phase = "8B Sandbox" if desired == "sandbox_observed" else "8C Cloud"
            return self._response(
                "not_available",
                data={
                    "model_name": request.model_name,
                    "cloud_execution_status": "not_available",
                    "estimated_bytes": None,
                    "observed_job_id": None,
                },
                evidence_level="not_available",
                warnings=[f"{phase} evidence is not available and no fixture fallback was used"],
            )
        reports = self._reports("reports/data-quality/phase8a/*/cost-policy-report.json")
        candidates = [
            (artifact_id, report)
            for artifact_id, report in reports
            if report.get("model") == request.model_name
            and report.get("evidence_level") == "simulated"
        ]
        if not candidates:
            return self._response(
                "not_available",
                data={
                    "model_name": request.model_name,
                    "cloud_execution_status": "not_available",
                    "estimated_bytes": None,
                    "observed_job_id": None,
                },
                evidence_level="not_available",
                warnings=["simulated cost report is not available"],
            )
        artifact_id, report = candidates[-1]
        return self._response(
            "ok",
            data={
                "model_name": request.model_name,
                "evidence_level": "simulated",
                "estimated_bytes": report.get("estimated_bytes"),
                "warning_threshold_bytes": report.get("warning_threshold_bytes"),
                "threshold_bytes": report.get("blocking_threshold_bytes"),
                "decision": report.get("status"),
                "observed_job_id": None,
                "cloud_execution_status": "not_executed",
            },
            evidence=[artifact_id],
            evidence_level="simulated",
            warnings=["Fixture-based estimate; not a real BigQuery measurement."],
        )

    def _find(self, name: str) -> tuple[AssetMetadata | None, ToolResponse | None]:
        matches = [
            asset for asset in self.index.assets if asset.name == name or asset.asset_id == name
        ]
        if not matches:
            return None, self._response("not_found", warnings=["model is not indexed"])
        if len(matches) > 1:
            return None, self._response(
                "partial",
                data={"matching_asset_ids": sorted(asset.asset_id for asset in matches)},
                warnings=["model name is ambiguous; use the exact asset_id"],
            )
        return matches[0], None

    def _walk(
        self, start: str, direction: Literal["upstream", "downstream"], max_depth: int
    ) -> tuple[list[str], list[list[str]], bool]:
        visited = {start}
        frontier = [(start, 0)]
        edges: set[tuple[str, str]] = set()
        truncated = False
        while frontier:
            current, depth = frontier.pop(0)
            neighbours = getattr(self.assets[current], direction)
            if depth >= max_depth:
                truncated = truncated or bool(neighbours)
                continue
            for neighbour in neighbours:
                edge = (neighbour, current) if direction == "upstream" else (current, neighbour)
                edges.add(edge)
                if neighbour not in visited:
                    if len(visited) >= MAX_LINEAGE_NODES:
                        truncated = True
                        continue
                    visited.add(neighbour)
                    frontier.append((neighbour, depth + 1))
        return sorted(visited - {start}), [list(edge) for edge in sorted(edges)], truncated

    def _reports(self, pattern: str) -> list[tuple[str, dict[str, Any]]]:
        prefix = (self.root / pattern.split("*")[0]).resolve()
        if not prefix.is_relative_to(self.root):
            raise ArtifactSecurityError("report pattern escapes repository root")
        reports: list[tuple[str, dict[str, Any]]] = []
        for path in sorted(self.root.glob(pattern)):
            resolved = path.resolve()
            if resolved.is_file() and resolved.is_relative_to(self.root):
                reports.append((resolved.relative_to(self.root).as_posix(), load_json(resolved)))
        return reports

    def _fixed_path(self, relative: str) -> Path:
        path = (self.root / relative).resolve()
        if Path(relative).is_absolute() or ".." in Path(relative).parts:
            raise ArtifactSecurityError("fixed artifact path is invalid")
        if not path.is_relative_to(self.root):
            raise ArtifactSecurityError("fixed artifact path escapes repository root")
        return path

    def _response(
        self,
        status: Literal["ok", "partial", "not_found", "not_available", "invalid_request", "error"],
        *,
        data: dict[str, Any] | None = None,
        evidence: list[str] | None = None,
        evidence_level: Literal[
            "static_validation", "simulated", "sandbox_observed", "cloud_observed", "not_available"
        ] = "static_validation",
        warnings: list[str] | None = None,
    ) -> ToolResponse:
        return ToolResponse(
            status=status,
            data=data or {},
            evidence=evidence or [],
            evidence_level=evidence_level,
            warnings=warnings or [],
            generated_at=iso_now(),
        )

    @staticmethod
    def _safe_validation_errors(error: ValidationError) -> list[dict[str, Any]]:
        return [
            {"type": item["type"], "loc": list(item["loc"]), "msg": item["msg"]}
            for item in error.errors(include_url=False, include_input=False)
        ]

    @staticmethod
    def _is_stale(last_run: datetime | None) -> bool | None:
        if last_run is None:
            return None
        return (iso_now() - last_run).total_seconds() > 24 * 60 * 60

    def _audit(
        self,
        *,
        request_id: str,
        tool: str,
        arguments: dict[str, Any],
        response: ToolResponse,
        duration_ms: float,
        result_size: int,
        error_category: str | None,
    ) -> None:
        self.audit_path.parent.mkdir(parents=True, exist_ok=True)
        entry = {
            "request_id": request_id,
            "timestamp": iso_now().isoformat(),
            "tool_name": tool,
            "sanitized_arguments": redact(arguments),
            "status": response.status,
            "duration_ms": duration_ms,
            "result_size_bytes": result_size,
            "evidence_count": len(response.evidence),
            "error_category": error_category,
        }
        with self.audit_path.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(entry, sort_keys=True) + "\n")
