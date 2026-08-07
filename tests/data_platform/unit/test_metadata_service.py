import json
import time
from datetime import UTC, datetime
from pathlib import Path

import pytest

from data_platform.mcp_server import StdioMcpServer
from data_platform.metadata_index import MetadataBuilder, write_build
from data_platform.metadata_service import MetadataService
from tests.data_platform.unit.test_metadata_index import artifact_fixture


@pytest.fixture
def service(tmp_path: Path) -> MetadataService:
    artifact_fixture(tmp_path)
    result = MetadataBuilder(tmp_path).build()
    write_build(result, tmp_path / "reports/metadata")
    return MetadataService(tmp_path, result.index, timeout_seconds=0.02)


def test_required_tools_cover_positive_and_not_found(service: MetadataService) -> None:
    search = service.call("search_data_assets", {"query": "orders"})
    schema = service.call("get_model_schema", {"model_name": "fct_orders"})
    owner = service.call("get_model_owner", {"model_name": "mart_daily_sales"})
    lineage = service.call(
        "get_lineage", {"model_name": "mart_daily_sales", "direction": "both", "max_depth": 3}
    )
    upstream = service.call("get_upstream_lineage", {"model_name": "fct_orders", "max_depth": 3})
    impact = service.call(
        "get_downstream_impact", {"model_name": "stg_order_events", "max_depth": 5}
    )
    quality = service.call("get_quality_status", {"model_name": "fct_orders"})
    missing = service.call("get_model_schema", {"model_name": "does_not_exist"})
    assert search["status"] == schema["status"] == owner["status"] == "ok"
    assert lineage["data"]["upstream_nodes"]
    assert upstream["data"]["upstream_nodes"]
    assert "model.retail_data_platform.mart_daily_sales" in impact["data"]["published_marts"]
    assert quality["data"]["overall_status"] == "pass"
    assert missing["status"] == "not_found"


def test_lag_cost_and_missing_cloud_evidence_are_not_confused(service: MetadataService) -> None:
    lag = service.call("get_consumer_lag", {"consumer_group": "order-processing-group-v1"})
    simulated = service.call("get_cost_estimate", {"model_name": "mart_daily_sales"})
    cloud = service.call(
        "get_cost_estimate",
        {"model_name": "mart_daily_sales", "preferred_evidence_level": "cloud_observed"},
    )
    failures = service.call(
        "get_recent_pipeline_failures",
        {"pipeline_name": "retail_data_platform_pipeline", "limit": 10},
    )
    assert lag["data"]["partitions"][0]["committed_offset"] == 10
    assert simulated["evidence_level"] == "simulated"
    assert simulated["data"]["observed_job_id"] is None
    assert cloud["status"] == "not_available"
    assert cloud["data"]["estimated_bytes"] is None
    assert failures["status"] == "not_available"
    assert failures["data"]["cloud_execution_status"] == "not_available"


def test_local_pipeline_failure_is_partial_not_cloud_success(service: MetadataService) -> None:
    report = service.root / ("reports/data-quality/phase8a/run/phase8a-orchestration-report.json")
    report.write_text(
        json.dumps(
            {
                "run_id": "failed-local",
                "task_results": [
                    {
                        "task_id": "quality_gate",
                        "status": "failed",
                        "error_category": "quality_gate",
                    }
                ],
            }
        )
    )
    response = service.call(
        "get_recent_pipeline_failures",
        {"pipeline_name": "retail_data_platform_pipeline", "limit": 10},
    )
    assert response["status"] == "partial"
    assert response["evidence_level"] == "static_validation"
    assert response["data"]["cloud_execution_status"] == "not_available"
    assert response["data"]["failures"][0]["error_category"] == "quality_gate"


def test_stale_and_history_warning_are_explicit(service: MetadataService) -> None:
    asset = next(item for item in service.index.assets if item.name == "fct_orders")
    stale = asset.model_copy(update={"last_run_at": datetime(2020, 1, 1, tzinfo=UTC)})
    service.index = service.index.model_copy(
        update={
            "assets": [
                stale if item.asset_id == stale.asset_id else item for item in service.index.assets
            ]
        }
    )
    service.assets[stale.asset_id] = stale
    response = service.call(
        "get_quality_status", {"model_name": "fct_orders", "include_history": True}
    )
    assert response["status"] == "partial"
    assert response["data"]["stale_artifact_warning"] is True
    assert response["data"]["history"] is None


def test_missing_lag_is_not_available_without_live_fallback(service: MetadataService) -> None:
    (service.root / "reports/consumer-lag.json").unlink()
    response = service.call("get_consumer_lag", {"consumer_group": "order-processing-group-v1"})
    assert response["status"] == "not_available"
    assert response["evidence_level"] == "not_available"


def test_invalid_sql_extra_fields_and_bounded_depth_are_rejected(service: MetadataService) -> None:
    sql = service.call("search_data_assets", {"query": "select * from orders"})
    extra = service.call("get_model_schema", {"model_name": "fct_orders", "path": "/etc/passwd"})
    depth = service.call("get_lineage", {"model_name": "fct_orders", "max_depth": 999})
    assert sql["status"] == extra["status"] == depth["status"] == "invalid_request"


def test_timeout_returns_sanitized_error_and_is_audited(
    service: MetadataService, monkeypatch: pytest.MonkeyPatch
) -> None:
    original = service._dispatch

    def slow_dispatch(tool, request):
        time.sleep(0.1)
        return original(tool, request)

    monkeypatch.setattr(service, "_dispatch", slow_dispatch)
    response = service.call("get_model_schema", {"model_name": "fct_orders"})
    assert response["status"] == "error"
    assert "timeout" in response["warnings"][0]
    audit = json.loads(service.audit_path.read_text().splitlines()[-1])
    assert audit["error_category"] == "timeout"


def test_secret_redaction_applies_to_response_and_audit(service: MetadataService) -> None:
    response = service.call(
        "get_model_schema", {"model_name": "fct_orders", "password": "super-secret"}
    )
    audit_text = service.audit_path.read_text()
    assert response["status"] == "invalid_request"
    assert "super-secret" not in audit_text
    assert "[REDACTED]" in audit_text


def test_stdio_protocol_lists_read_only_tools_and_calls_search(service: MetadataService) -> None:
    server = StdioMcpServer(service)
    initialized = server.handle(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {"protocolVersion": "2025-11-25"},
        }
    )
    listed = server.handle({"jsonrpc": "2.0", "id": 2, "method": "tools/list"})
    called = server.handle(
        {
            "jsonrpc": "2.0",
            "id": 3,
            "method": "tools/call",
            "params": {"name": "search_data_assets", "arguments": {"query": "orders"}},
        }
    )
    assert initialized is not None and initialized["result"]["serverInfo"]["name"]
    assert listed is not None and len(listed["result"]["tools"]) == 10
    assert all(tool["annotations"]["readOnlyHint"] for tool in listed["result"]["tools"])
    assert called is not None and called["result"]["structuredContent"]["status"] == "ok"
