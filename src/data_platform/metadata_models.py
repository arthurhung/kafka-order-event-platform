"""Validated Phase 9 metadata and MCP response models."""

from datetime import datetime
from typing import Any, Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

EvidenceLevel = Literal[
    "static_validation",
    "simulated",
    "sandbox_observed",
    "cloud_observed",
    "not_available",
]
ToolStatus = Literal["ok", "partial", "not_found", "not_available", "invalid_request", "error"]


class ColumnMetadata(BaseModel):
    """A documented column without row-level values."""

    model_config = ConfigDict(extra="forbid")

    name: str
    data_type: str | None = None
    nullable: bool | None = None
    description: str = ""


class AssetMetadata(BaseModel):
    """Normalized metadata for one dbt asset."""

    model_config = ConfigDict(extra="forbid")

    asset_id: str
    name: str
    resource_type: str
    layer: str
    owner: str | None = None
    domain: str | None = None
    data_product: str | None = None
    description: str = ""
    grain: str | None = None
    maturity: str | None = None
    materialization: str | None = None
    slo: dict[str, Any] = Field(default_factory=dict)
    columns: list[ColumnMetadata] = Field(default_factory=list)
    upstream: list[str] = Field(default_factory=list)
    downstream: list[str] = Field(default_factory=list)
    quality_status: str = "unknown"
    quality_counts: dict[str, int] = Field(default_factory=dict)
    freshness_status: str | None = None
    contract_status: str = "not_applicable"
    tests: list[str] = Field(default_factory=list)
    last_run_at: AwareDatetime | None = None
    source_artifacts: list[str] = Field(default_factory=list)


class EvidenceArtifact(BaseModel):
    """Sanitized identity and classification for an indexed report."""

    model_config = ConfigDict(extra="forbid")

    artifact_id: str
    report_type: str
    status: str
    validation_evidence_level: Literal["static_validation"] = "static_validation"
    source_evidence_level: str
    observed_at: AwareDatetime | None = None


class MetadataIndex(BaseModel):
    """Deterministic collection of normalized assets."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[1] = 1
    status: Literal["complete", "degraded"]
    generated_at: AwareDatetime
    assets: list[AssetMetadata]
    evidence_artifacts: list[EvidenceArtifact] = Field(default_factory=list)
    missing_artifacts: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)


class LineageEdge(BaseModel):
    """Directed dbt lineage edge."""

    model_config = ConfigDict(extra="forbid")

    upstream: str
    downstream: str


class LineageGraph(BaseModel):
    """Bounded-query input graph stored independently from the index."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[1] = 1
    nodes: list[str]
    edges: list[LineageEdge]


class IndexSummary(BaseModel):
    """Machine-readable index build result."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[1] = 1
    status: Literal["complete", "degraded", "failed"]
    generated_at: AwareDatetime
    asset_count: int
    edge_count: int
    missing_artifacts: list[str]
    warnings: list[str]
    evidence_level: Literal["static_validation"] = "static_validation"


class ToolResponse(BaseModel):
    """Common response envelope returned by every discovery tool."""

    model_config = ConfigDict(extra="forbid")

    status: ToolStatus
    data: dict[str, Any]
    evidence: list[str]
    evidence_level: EvidenceLevel
    warnings: list[str]
    generated_at: AwareDatetime


def iso_now() -> datetime:
    """Return a timezone-aware UTC timestamp for runtime-only metadata."""
    from datetime import UTC

    return datetime.now(UTC)
