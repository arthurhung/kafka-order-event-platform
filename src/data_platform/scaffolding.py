"""Create deterministic draft dbt model files without guessing data semantics."""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

Layer = Literal["staging", "intermediate", "marts"]

_PREFIXES: dict[Layer, str] = {
    "staging": "stg_",
    "intermediate": "int_",
    "marts": "mart_",
}
_KNOWN_PREFIXES = tuple(_PREFIXES.values())
_MODEL_NAME = re.compile(r"^[a-z][a-z0-9_]*$")


class ScaffoldError(ValueError):
    """Raised when a scaffold request is unsafe or incomplete as an operation."""


@dataclass(frozen=True, slots=True)
class ScaffoldRequest:
    """Validated inputs used to create one draft dbt model."""

    name: str
    layer: Layer
    owner: str
    domain: str
    grain: str


@dataclass(frozen=True, slots=True)
class ScaffoldResult:
    """Paths and normalized identity of a generated scaffold draft."""

    model_name: str
    sql_path: Path
    yaml_path: Path
    status: Literal["draft"] = "draft"


def normalize_model_name(name: str, layer: Layer) -> str:
    """Return a snake-case model name with the prefix required by its layer."""
    candidate = name.strip()
    if not _MODEL_NAME.fullmatch(candidate):
        raise ScaffoldError("model name must be lowercase snake_case")
    required_prefix = _PREFIXES[layer]
    supplied_prefix = next(
        (prefix for prefix in _KNOWN_PREFIXES if candidate.startswith(prefix)),
        None,
    )
    if supplied_prefix is not None and supplied_prefix != required_prefix:
        raise ScaffoldError(f"model prefix {supplied_prefix!r} conflicts with layer {layer!r}")
    return candidate if supplied_prefix else f"{required_prefix}{candidate}"


def _validate_request(request: ScaffoldRequest) -> None:
    for field_name, value in (
        ("owner", request.owner),
        ("domain", request.domain),
        ("grain", request.grain),
    ):
        if not value.strip():
            raise ScaffoldError(f"{field_name} must not be empty")
    if not request.grain.strip().lower().startswith("one row per "):
        raise ScaffoldError("grain must start with 'one row per '")


def _render_template(template_path: Path, replacements: dict[str, str]) -> str:
    rendered = template_path.read_text(encoding="utf-8")
    for key, value in replacements.items():
        rendered = rendered.replace(f"{{{{ {key} }}}}", value)
    unresolved = re.findall(r"\{\{ [A-Z][A-Z0-9_]* \}\}", rendered)
    if unresolved:
        raise ScaffoldError(f"unresolved template placeholders: {sorted(set(unresolved))}")
    return rendered


def scaffold_model(
    request: ScaffoldRequest,
    *,
    project_dir: Path = Path("dbt"),
    template_dir: Path | None = None,
) -> ScaffoldResult:
    """Generate SQL and YAML draft files, refusing any existing target."""
    _validate_request(request)
    model_name = normalize_model_name(request.name, request.layer)
    templates = template_dir or Path(__file__).parent / "templates" / "dbt"
    sql_template = templates / f"{request.layer.removesuffix('s')}.sql.tmpl"
    if request.layer == "marts":
        sql_template = templates / "mart.sql.tmpl"
    yaml_template = templates / "model.yml.tmpl"
    if not sql_template.is_file() or not yaml_template.is_file():
        raise ScaffoldError("required scaffold template is missing")

    model_dir = project_dir / "models" / request.layer
    sql_path = model_dir / f"{model_name}.sql"
    yaml_path = model_dir / f"{model_name}.yml"
    existing = [path for path in (sql_path, yaml_path) if path.exists()]
    if existing:
        names = ", ".join(str(path) for path in existing)
        raise ScaffoldError(f"refusing to overwrite existing scaffold target: {names}")

    replacements = {
        "MODEL_NAME": model_name,
        "OWNER": request.owner.strip(),
        "DOMAIN": request.domain.strip(),
        "GRAIN": request.grain.strip(),
        "LAYER": request.layer,
        "BIGQUERY_METADATA": (
            "      warehouse_compatibility:\n"
            "        postgres: supported\n"
            "        bigquery: planned\n"
            "      bigquery:\n"
            "        validation_evidence_level: static_validation\n"
            "        # BLOCKING_TODO: declare verified partition, cluster, incremental, "
            "scan-window, and cost policy.\n"
            "        partition_by: null\n"
            "        cluster_by: []\n"
            "        require_partition_filter: false"
            if request.layer == "marts"
            else ""
        ),
    }
    sql = _render_template(sql_template, replacements)
    yaml = _render_template(yaml_template, replacements)
    model_dir.mkdir(parents=True, exist_ok=True)
    try:
        sql_path.write_text(sql, encoding="utf-8", errors="strict")
        yaml_path.write_text(yaml, encoding="utf-8", errors="strict")
    except OSError:
        if sql_path.exists() and not yaml_path.exists():
            sql_path.unlink()
        raise
    return ScaffoldResult(model_name=model_name, sql_path=sql_path, yaml_path=yaml_path)
