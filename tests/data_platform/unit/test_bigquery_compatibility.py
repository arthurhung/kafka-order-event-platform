import copy
import json
from datetime import UTC, datetime
from pathlib import Path

from data_platform.bigquery_compatibility import validate_compatibility_manifest


def _policy(model: str):
    policies = {
        "fct_order_events": (
            "event_date",
            ["order_id", "event_type"],
            "merge",
            ["event_id"],
            "event_id",
            31,
            "high",
        ),
        "mart_daily_sales": (
            "event_date",
            ["currency", "channel"],
            "insert_overwrite",
            ["event_date", "currency", "channel"],
            None,
            31,
            "medium",
        ),
        "mart_service_health": (
            "metric_date",
            ["service"],
            "insert_overwrite",
            ["metric_minute", "service"],
            None,
            7,
            "low",
        ),
    }
    if model == "fct_orders":
        return {
            "validation_evidence_level": "static_validation",
            "partition_by": None,
            "partition_exemption": {
                "reason": "mutable latest-state row has no stable calendar partition key",
                "owner": "data-platform",
                "review_at": "2027-08-06T00:00:00Z",
            },
            "cluster_by": ["order_id"],
            "require_partition_filter": False,
            "incremental": {
                "expected_strategy": "merge",
                "unique_key": ["order_id"],
                "merge_key": "order_id",
                "late_arriving_data": {
                    "lookback_days": 7,
                    "outside_window_action": "manual_bounded_backfill",
                },
            },
            "maximum_expected_scan_window_days": 31,
            "cost_class": "low_lookup",
        }
    partition, cluster, strategy, unique_key, merge_key, window, cost_class = policies[model]
    return {
        "validation_evidence_level": "static_validation",
        "partition_by": {"field": partition, "data_type": "date", "granularity": "day"},
        "cluster_by": cluster,
        "require_partition_filter": True,
        "incremental": {
            "expected_strategy": strategy,
            "unique_key": unique_key,
            "merge_key": merge_key,
            "late_arriving_data": {
                "lookback_days": 7,
                "outside_window_action": "manual_bounded_backfill",
            },
        },
        "maximum_expected_scan_window_days": window,
        "cost_class": cost_class,
    }


def _manifest():
    columns = {
        "event_id",
        "event_date",
        "order_id",
        "event_type",
        "currency",
        "channel",
        "metric_date",
        "metric_minute",
        "service",
    }
    return {
        "nodes": {
            f"model.p.{name}": {
                "name": name,
                "resource_type": "model",
                "original_file_path": f"models/marts/{name}.sql",
                "columns": {
                    column: {"data_type": "date" if column.endswith("date") else "text"}
                    for column in columns
                },
                "meta": {
                    "warehouse_compatibility": {"postgres": "supported", "bigquery": "planned"},
                    "bigquery": _policy(name),
                },
                "raw_code": "select event_id from upstream",
            }
            for name in (
                "fct_order_events",
                "fct_orders",
                "mart_daily_sales",
                "mart_service_health",
            )
        }
    }


def _validate(tmp_path: Path, manifest):
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(manifest))
    return validate_compatibility_manifest(path, now=datetime(2026, 8, 6, tzinfo=UTC))


def test_valid_required_model_policies_pass(tmp_path: Path):
    report = _validate(tmp_path, _manifest())
    assert report.status == "passed"
    assert set(report.effective_statuses.values()) == {"static_validated"}


def test_missing_partition_and_cluster_fields_block(tmp_path: Path):
    manifest = _manifest()
    policy = manifest["nodes"]["model.p.mart_daily_sales"]["meta"]["bigquery"]
    policy["partition_by"]["field"] = "missing_date"
    policy["cluster_by"] = ["missing_cluster"]
    rules = {
        item.rule for item in _validate(tmp_path, manifest).findings if item.severity == "error"
    }
    assert {"partition-field-exists", "cluster-field-exists"} <= rules


def test_cluster_count_overlap_and_partition_type_block(tmp_path: Path):
    manifest = _manifest()
    policy = manifest["nodes"]["model.p.mart_daily_sales"]["meta"]["bigquery"]
    policy["cluster_by"] = ["event_date", "currency", "channel", "order_id", "event_type"]
    policy["partition_by"]["data_type"] = "varchar"
    rules = {
        item.rule for item in _validate(tmp_path, manifest).findings if item.severity == "error"
    }
    assert {"cluster-fields", "partition-cluster-overlap", "partition-type"} <= rules


def test_invalid_status_and_cloud_evidence_block(tmp_path: Path):
    manifest = _manifest()
    node = manifest["nodes"]["model.p.fct_order_events"]
    node["meta"]["warehouse_compatibility"]["bigquery"] = "cloud_validated"
    node["meta"]["bigquery"]["validation_evidence_level"] = "cloud_observed"
    rules = {
        item.rule for item in _validate(tmp_path, manifest).findings if item.severity == "error"
    }
    assert {"compatibility-status", "evidence-level"} <= rules


def test_missing_metadata_and_expired_exemption_block(tmp_path: Path):
    manifest = _manifest()
    del manifest["nodes"]["model.p.fct_order_events"]["meta"]["bigquery"]
    manifest["nodes"]["model.p.fct_orders"]["meta"]["bigquery"]["partition_exemption"][
        "review_at"
    ] = "2020-01-01T00:00:00Z"
    rules = {
        item.rule for item in _validate(tmp_path, manifest).findings if item.severity == "error"
    }
    assert {"bigquery-metadata", "partition-exemption-review"} <= rules


def test_blocking_todo_blocks(tmp_path: Path):
    manifest = copy.deepcopy(_manifest())
    manifest["nodes"]["model.p.fct_orders"]["raw_code"] = "-- BLOCKING_TODO"
    assert any(item.rule == "blocking-todo" for item in _validate(tmp_path, manifest).findings)
