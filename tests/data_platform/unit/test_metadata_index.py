import json
from copy import deepcopy
from pathlib import Path

import pytest

from data_platform.metadata_index import (
    MetadataBuilder,
    MetadataBuildError,
    validate_build,
    write_build,
)
from data_platform.metadata_security import ArtifactSecurityError, resolve_allowlisted


def _write(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")


def artifact_fixture(root: Path) -> None:
    source_id = "source.retail_data_platform.streaming_platform.valid_orders"
    staging_id = "model.retail_data_platform.stg_order_events"
    fact_id = "model.retail_data_platform.fct_orders"
    mart_id = "model.retail_data_platform.mart_daily_sales"
    base_column = {
        "name": "order_id",
        "description": "Order identifier.",
        "data_type": "text",
        "constraints": [{"type": "not_null"}],
    }

    def model(name: str, layer: str, upstream: str, *, published: bool = False) -> dict:
        meta = (
            {
                "owner": "data-platform",
                "domain": "commerce",
                "data_product": "orders",
                "maturity": "experimental",
                "sla": {"freshness_minutes": 45},
            }
            if published
            else {}
        )
        return {
            "name": name,
            "resource_type": "model",
            "fqn": ["retail_data_platform", layer, name],
            "description": "Grain: one row per order_id. Test asset.",
            "columns": {"order_id": deepcopy(base_column)},
            "depends_on": {"nodes": [upstream]},
            "config": {
                "materialized": "table",
                "contract": {"enforced": published},
                "meta": meta,
            },
        }

    test_id = "test.retail_data_platform.not_null_fct_orders_order_id.fixture"
    manifest = {
        "metadata": {"generated_at": "2026-08-07T00:00:00Z"},
        "nodes": {
            staging_id: model("stg_order_events", "staging", source_id),
            fact_id: model("fct_orders", "marts", staging_id, published=True),
            mart_id: model("mart_daily_sales", "marts", fact_id, published=True),
            test_id: {
                "name": "not_null_fct_orders_order_id",
                "resource_type": "test",
                "depends_on": {"nodes": [fact_id]},
            },
        },
        "sources": {
            source_id: {
                "name": "valid_orders",
                "resource_type": "source",
                "fqn": ["retail_data_platform", "streaming_platform", "valid_orders"],
                "description": "Kafka Core order events.",
                "columns": {"order_id": deepcopy(base_column)},
                "config": {},
            }
        },
        "child_map": {
            source_id: [staging_id],
            staging_id: [fact_id],
            fact_id: [mart_id, test_id],
            mart_id: [],
        },
    }
    catalog = {
        "metadata": {},
        "nodes": {
            key: {"columns": {"order_id": {"name": "order_id", "type": "text"}}}
            for key in (staging_id, fact_id, mart_id)
        },
        "sources": {source_id: {"columns": {"order_id": {"name": "order_id", "type": "text"}}}},
    }
    run_results = {
        "metadata": {"generated_at": "2026-08-07T00:01:00Z"},
        "results": [{"unique_id": test_id, "status": "pass"}],
    }
    freshness = {"results": [{"unique_id": source_id, "status": "pass"}]}
    _write(root / "dbt/target/manifest.json", manifest)
    _write(root / "dbt/target/catalog.json", catalog)
    _write(root / "dbt/target/run_results.json", run_results)
    _write(root / "dbt/target/sources.json", freshness)
    _write(root / "reports/data-quality/phase7-ci-summary.json", {"status": "passed"})
    _write(root / "reports/data-quality/phase7-contract-diff.json", {"status": "passed"})
    _write(
        root / "reports/data-quality/phase8a/run/cost-policy-report.json",
        {
            "model": "mart_daily_sales",
            "status": "pass",
            "evidence_level": "simulated",
            "estimated_bytes": 100,
            "warning_threshold_bytes": 200,
            "blocking_threshold_bytes": 300,
            "observed_job_id": None,
        },
    )
    _write(
        root / "reports/data-quality/phase8a/run/phase8a-orchestration-report.json",
        {"run_id": "local", "task_results": [{"task_id": "quality", "status": "passed"}]},
    )
    _write(
        root / "reports/consumer-lag.json",
        {
            "observed_at": "2026-08-07T00:02:00Z",
            "groups": [
                {
                    "consumer_group": "order-processing-group-v1",
                    "rows": [
                        {
                            "topic": "ecommerce.orders.raw.v1",
                            "partition": 0,
                            "current_offset": 10,
                            "log_end_offset": 12,
                            "lag": 2,
                            "status": "measured",
                        }
                    ],
                }
            ],
        },
    )


def test_build_is_deterministic_and_valid(tmp_path: Path) -> None:
    artifact_fixture(tmp_path)
    first = MetadataBuilder(tmp_path).build()
    second = MetadataBuilder(tmp_path).build()
    assert first.index.model_dump_json() == second.index.model_dump_json()
    assert first.graph.model_dump_json() == second.graph.model_dump_json()
    assert first.summary.status == "complete"
    assert first.summary.asset_count == 4
    assert first.summary.edge_count == 3
    output = tmp_path / "reports/metadata"
    write_build(first, output)
    assert validate_build(output).asset_count == 4
    rendered = (output / "metadata-index.json").read_text()
    assert str(tmp_path) not in rendered


def test_missing_catalog_is_degraded_and_uses_declared_type(tmp_path: Path) -> None:
    artifact_fixture(tmp_path)
    (tmp_path / "dbt/target/catalog.json").unlink()
    result = MetadataBuilder(tmp_path).build()
    assert result.index.status == "degraded"
    assert "catalog" in result.index.missing_artifacts
    asset = next(item for item in result.index.assets if item.name == "fct_orders")
    assert asset.columns[0].data_type == "text"


def test_missing_manifest_and_malformed_manifest_fail(tmp_path: Path) -> None:
    with pytest.raises(MetadataBuildError, match="manifest artifact is required"):
        MetadataBuilder(tmp_path).build()
    path = tmp_path / "dbt/target/manifest.json"
    path.parent.mkdir(parents=True)
    path.write_text("not-json")
    with pytest.raises(MetadataBuildError, match="invalid manifest"):
        MetadataBuilder(tmp_path).build()


def test_path_traversal_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(ArtifactSecurityError, match="not allowlisted"):
        resolve_allowlisted(tmp_path, "../secrets.json", {"dbt/target/manifest.json"})


def test_validation_rejects_conflicting_output(tmp_path: Path) -> None:
    artifact_fixture(tmp_path)
    output = tmp_path / "reports/metadata"
    write_build(MetadataBuilder(tmp_path).build(), output)
    graph = json.loads((output / "lineage-graph.json").read_text())
    graph["nodes"].append("model.unknown")
    _write(output / "lineage-graph.json", graph)
    with pytest.raises(MetadataBuildError, match="lineage nodes"):
        validate_build(output)
