"""Deterministic Phase 10 dbt scaffold workflow."""

from __future__ import annotations

import json
import os
import re
import shutil
import tempfile
from pathlib import Path
from typing import Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator, model_validator

from data_platform.incidents import ReadOnlyEvidenceClient

MODEL_NAME = re.compile(r"^(stg_|int_|fct_|dim_|mart_)[a-z0-9_]+$")
IDENTITY = re.compile(r"^[a-z][a-z0-9_-]{1,63}$")


class ScaffoldWorkflowError(ValueError):
    """Raised before any target files are written."""


class DbtScaffoldRequest(BaseModel):
    """Complete, strict input for one production-ready dbt scaffold."""

    model_config = ConfigDict(extra="forbid")

    model_name: str
    layer: Literal["staging", "intermediate", "marts"]
    business_requirement: str = Field(min_length=5, max_length=500)
    grain: str | None = Field(default=None, max_length=300)
    expected_consumers: list[str] = Field(min_length=1, max_length=20)
    owner: str
    domain: str
    required_metrics: list[str] = Field(default_factory=list, max_length=30)
    freshness_minutes: int = Field(ge=1, le=10080)
    upstream_model: str
    selected_columns: list[str] = Field(min_length=1, max_length=100)
    generated_at: AwareDatetime

    @field_validator("model_name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        """Enforce a layer-specific model name without path characters."""
        if not MODEL_NAME.fullmatch(value):
            raise ValueError("model_name must use an allowed dbt prefix and snake_case")
        return value

    @field_validator("owner", "domain")
    @classmethod
    def validate_identity(cls, value: str) -> str:
        """Reject paths and free-form owner/domain values."""
        if not IDENTITY.fullmatch(value):
            raise ValueError("owner and domain must be lowercase safe identifiers")
        return value

    @field_validator("upstream_model")
    @classmethod
    def validate_upstream(cls, value: str) -> str:
        """Reject arbitrary paths and source expressions."""
        if not re.fullmatch(r"[a-z][a-z0-9_.-]{1,149}", value):
            raise ValueError("upstream_model must be a metadata asset name")
        return value

    @field_validator("selected_columns")
    @classmethod
    def validate_columns(cls, value: list[str]) -> list[str]:
        """Require unique, safe column names."""
        if len(set(value)) != len(value) or any(
            not re.fullmatch(r"[a-z][a-z0-9_]{0,99}", item) for item in value
        ):
            raise ValueError("selected_columns must be unique snake_case names")
        return value

    @model_validator(mode="after")
    def validate_layer(self) -> DbtScaffoldRequest:
        """Match name/layer and require a documented grain for published models."""
        prefixes = {
            "staging": ("stg_",),
            "intermediate": ("int_",),
            "marts": ("fct_", "dim_", "mart_"),
        }
        if not self.model_name.startswith(prefixes[self.layer]):
            raise ValueError("model_name prefix does not match layer")
        if self.layer in {"intermediate", "marts"} and (
            self.grain is None or not self.grain.lower().startswith("one row per ")
        ):
            raise ValueError("intermediate and mart grain must start with 'one row per '")
        return self


class ScaffoldCommandResult(BaseModel):
    """One validation command outcome supplied by the executor."""

    command: str
    exit_code: int
    result: Literal["passed", "failed"]


class DbtScaffoldReport(BaseModel):
    """Machine-readable output for scaffold generation and validation."""

    report_type: Literal["dbt_scaffold_smoke"] = "dbt_scaffold_smoke"
    schema_version: Literal[1] = 1
    status: Literal["completed", "failed"]
    evidence_level: Literal["static_validation"] = "static_validation"
    request: DbtScaffoldRequest
    selected_sources: list[str]
    selected_columns: list[str]
    selected_layer: str
    grain: str | None
    generated_files: list[str]
    validation_commands: list[ScaffoldCommandResult]
    evidence_ids: list[str]
    warnings: list[str]
    assumptions: list[str]
    human_review_items: list[str]
    deterministic_status: Literal["deterministic"] = "deterministic"
    generated_at: AwareDatetime


def inspect_available_columns(
    client: ReadOnlyEvidenceClient, upstream_model: str
) -> tuple[dict[str, str | None], list[str]]:
    """Read real model columns through the Phase 9 schema tool."""
    response = client.call("get_model_schema", {"model_name": upstream_model})
    if response.get("status") not in {"ok", "partial"}:
        raise ScaffoldWorkflowError("upstream model is not available in metadata")
    data = response.get("data")
    if not isinstance(data, dict) or not isinstance(data.get("columns"), list):
        raise ScaffoldWorkflowError("upstream schema response has no columns")
    columns = {
        str(item["name"]): str(item["data_type"]) if item.get("data_type") is not None else None
        for item in data["columns"]
        if isinstance(item, dict) and isinstance(item.get("name"), str)
    }
    evidence = [str(item) for item in response.get("evidence", [])]
    return columns, sorted(evidence)


def _render(
    request: DbtScaffoldRequest,
    template_dir: Path,
    available_columns: dict[str, str | None],
) -> dict[str, str]:
    relation_macro = (
        "source('streaming_source', '" + request.upstream_model + "')"
        if request.layer == "staging"
        else "ref('" + request.upstream_model + "')"
    )
    replacements = {
        "MODEL_NAME": request.model_name,
        "SQL_UPSTREAM_RELATION": "{{ " + relation_macro + " }}",
        "UNIT_TEST_INPUT": relation_macro,
        "COLUMNS": ",\n    ".join(request.selected_columns),
        "GRAIN": request.grain or "one row per source record",
        "OWNER": request.owner,
        "DOMAIN": request.domain,
        "FRESHNESS_MINUTES": str(request.freshness_minutes),
        "COLUMN_YAML": "\n".join(
            (
                f"      - name: {column}\n"
                f"        description: Verified column `{column}`.\n"
                f"        data_type: {available_columns[column]}"
            )
            for column in request.selected_columns
        ),
    }
    sql_name = "mart.sql.j2" if request.layer == "marts" else f"{request.layer}.sql.j2"
    names = (sql_name, "model.yml.j2", "unit_test.yml.j2")
    rendered: dict[str, str] = {}
    for name in names:
        content = (template_dir / name).read_text(encoding="utf-8")
        for key, value in replacements.items():
            content = content.replace(f"{{{{ {key} }}}}", value)
        if re.search(r"\{\{ [A-Z][A-Z0-9_]+ \}\}", content):
            raise ScaffoldWorkflowError(f"unresolved template value in {name}")
        rendered[name] = content
    return rendered


def scaffold_verified_model(
    request: DbtScaffoldRequest,
    *,
    client: ReadOnlyEvidenceClient,
    project_dir: Path,
    template_dir: Path,
) -> DbtScaffoldReport:
    """Validate metadata and atomically create SQL, YAML, and unit-test YAML."""
    root = project_dir.resolve()
    available, evidence = inspect_available_columns(client, request.upstream_model)
    missing = sorted(set(request.selected_columns) - set(available))
    if missing:
        raise ScaffoldWorkflowError(f"selected columns do not exist: {missing}")
    missing_types = sorted(column for column in request.selected_columns if not available[column])
    if missing_types:
        raise ScaffoldWorkflowError(f"selected columns have no verified data type: {missing_types}")
    target = root / "models" / request.layer
    file_names = (
        f"{request.model_name}.sql",
        f"{request.model_name}.yml",
        f"{request.model_name}_unit_test.yml",
    )
    paths = tuple(target / name for name in file_names)
    if any(path.exists() for path in paths):
        raise ScaffoldWorkflowError("refusing to overwrite an existing model file")
    rendered = _render(request, template_dir, available)
    target.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix="phase10-scaffold-", dir=target))
    try:
        staged = (
            temporary / file_names[0],
            temporary / file_names[1],
            temporary / file_names[2],
        )
        contents = (
            rendered["mart.sql.j2" if request.layer == "marts" else f"{request.layer}.sql.j2"],
            rendered["model.yml.j2"],
            rendered["unit_test.yml.j2"],
        )
        for path, content in zip(staged, contents, strict=True):
            path.write_text(content, encoding="utf-8")
        for source, destination in zip(staged, paths, strict=True):
            os.replace(source, destination)
    except OSError:
        for path in paths:
            if path.exists():
                path.unlink()
        raise
    finally:
        shutil.rmtree(temporary, ignore_errors=True)
    relative = [path.relative_to(root).as_posix() for path in paths]
    return DbtScaffoldReport(
        status="completed",
        request=request,
        selected_sources=[request.upstream_model],
        selected_columns=request.selected_columns,
        selected_layer=request.layer,
        grain=request.grain,
        generated_files=relative,
        validation_commands=[],
        evidence_ids=evidence,
        warnings=["Compile success does not establish business correctness."],
        assumptions=[],
        human_review_items=[
            "Review business semantics and execute dbt parse, compile, and affected build."
        ],
        generated_at=request.generated_at,
    )


def write_scaffold_report(report: DbtScaffoldReport, path: Path) -> None:
    """Write a stable machine-readable report."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(report.model_dump(mode="json"), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
