"""Versioned, UTF-8 benchmark report models and status rules."""

from enum import StrEnum
from pathlib import Path
from typing import Any

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field


class BenchmarkStatus(StrEnum):
    """Overall completion states for benchmark and demo runs."""

    PASSED = "passed"
    FAILED = "failed"
    PARTIAL = "partial"


class LatencyMetric(BaseModel):
    """Named latency distribution with explicit availability semantics."""

    model_config = ConfigDict(extra="forbid")

    status: str
    definition: str
    unit: str = "milliseconds"
    sample_count: int
    average_ms: float | None
    p95_ms: float | None
    p99_ms: float | None
    percentile_method: str = "nearest_rank"
    reason: str | None = None


class BenchmarkReport(BaseModel):
    """Machine-readable evidence for one isolated benchmark run."""

    model_config = ConfigDict(extra="forbid")

    schema_version: str = "1.0"
    run_id: str
    profile: str
    started_at: AwareDatetime
    finished_at: AwareDatetime
    status: BenchmarkStatus
    failure_stage: str | None = None
    exit_code: int
    environment: dict[str, Any]
    configuration: dict[str, Any]
    producer: dict[str, Any]
    consumer: dict[str, Any]
    latency: dict[str, LatencyMetric]
    runtime: dict[str, Any]
    database: dict[str, Any]
    run_scope: dict[str, Any]
    errors: list[str] = Field(default_factory=list)
    limitations: list[str] = Field(default_factory=list)
    artifacts: dict[str, Any] = Field(default_factory=dict)


def write_benchmark_report(report: BenchmarkReport, path: Path) -> None:
    """Write one indented UTF-8 report without silently changing its path."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(report.model_dump_json(indent=2) + "\n", encoding="utf-8")


def status_for_run(*, failed: bool, unavailable_required_metric: bool) -> BenchmarkStatus:
    """Derive the stable report status from execution and metric completeness."""
    if failed:
        return BenchmarkStatus.FAILED
    if unavailable_required_metric:
        return BenchmarkStatus.PARTIAL
    return BenchmarkStatus.PASSED
