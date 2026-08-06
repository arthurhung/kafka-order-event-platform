import json
from pathlib import Path

from data_platform.conventions import validate_manifest


def _node(
    name: str,
    layer: str,
    *,
    raw_code: str = "select order_id from {{ ref('upstream') }}",
    description: str = "Grain: one row per order_id.",
    columns: dict | None = None,
    meta: dict | None = None,
    contract: dict | None = None,
    dependencies: list[str] | None = None,
):
    return {
        "name": name,
        "resource_type": "model",
        "original_file_path": f"models/{layer}/{name}.sql",
        "raw_code": raw_code,
        "description": description,
        "columns": columns or {"order_id": {"description": "Order identifier."}},
        "meta": meta or {},
        "contract": contract or {"enforced": False},
        "depends_on": {"nodes": dependencies or []},
    }


def _mart_meta():
    return {
        "owner": "data-platform",
        "domain": "commerce",
        "data_product": "orders",
        "maturity": "experimental",
        "contains_pii": False,
        "sla": {"freshness_minutes": 60, "availability": "best_effort"},
        "contract_policy": "breaking_changes_blocked",
    }


def _write_manifest(tmp_path: Path, nodes: dict) -> Path:
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps({"nodes": nodes}))
    return path


def test_valid_manifest_passes(tmp_path: Path):
    nodes = {
        "model.project.stg_orders": _node(
            "stg_orders",
            "staging",
            dependencies=["source.project.platform.orders"],
        ),
        "model.project.int_orders": _node("int_orders", "intermediate"),
        "model.project.mart_orders": _node(
            "mart_orders",
            "marts",
            meta=_mart_meta(),
            contract={"enforced": True},
        ),
    }

    report = validate_manifest(_write_manifest(tmp_path, nodes))

    assert report.status == "passed"
    assert report.error_count == 0


def test_missing_mart_column_description_blocks(tmp_path: Path):
    node = _node(
        "mart_orders",
        "marts",
        columns={"order_id": {"description": ""}},
        meta=_mart_meta(),
        contract={"enforced": True},
    )

    report = validate_manifest(_write_manifest(tmp_path, {"model.p.mart_orders": node}))

    assert report.status == "failed"
    assert any(item.rule == "column-description" for item in report.findings)


def test_wildcard_policy_depends_on_layer(tmp_path: Path):
    nodes = {
        "model.p.stg_orders": _node("stg_orders", "staging", raw_code="select * from source"),
        "model.p.int_orders": _node(
            "int_orders", "intermediate", raw_code="select *, value from input"
        ),
    }

    report = validate_manifest(_write_manifest(tmp_path, nodes))

    assert any(item.rule == "select-star" and item.severity == "error" for item in report.findings)
    assert any(
        item.rule == "select-star" and item.severity == "warning" for item in report.findings
    )


def test_scaffold_draft_and_direct_source_in_mart_block(tmp_path: Path):
    meta = _mart_meta() | {"scaffold_status": "draft"}
    node = _node(
        "mart_orders",
        "marts",
        raw_code="-- BLOCKING_TODO\nselect order_id from input",
        meta=meta,
        contract={"enforced": True},
        dependencies=["source.p.platform.orders"],
    )

    report = validate_manifest(_write_manifest(tmp_path, {"model.p.mart_orders": node}))

    rules = {item.rule for item in report.findings if item.severity == "error"}
    assert {"blocking-todo", "direct-source-outside-staging"} <= rules


def test_staging_multiple_sources_blocks(tmp_path: Path):
    node = _node(
        "stg_orders",
        "staging",
        dependencies=["source.p.platform.orders", "source.p.other.users"],
    )

    report = validate_manifest(_write_manifest(tmp_path, {"model.p.stg_orders": node}))

    assert any(item.rule == "staging-multiple-sources" for item in report.findings)
