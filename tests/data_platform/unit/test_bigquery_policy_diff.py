import copy
from datetime import UTC, datetime

from data_platform.bigquery_policy_diff import compare_cost_thresholds, compare_policy_values


def _policy():
    return {
        "partition_by": {"field": "event_date", "data_type": "date", "granularity": "day"},
        "cluster_by": ["currency", "channel"],
        "require_partition_filter": True,
        "validation_evidence_level": "static_validation",
        "incremental": {
            "merge_key": None,
            "late_arriving_data": {
                "lookback_days": 7,
                "outside_window_action": "manual_bounded_backfill",
            },
        },
        "maximum_expected_scan_window_days": 31,
        "cost_class": "medium",
    }


def _change(path: tuple[str, ...], value):
    previous = {"mart_daily_sales": _policy()}
    current = copy.deepcopy(previous)
    target = current["mart_daily_sales"]
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = value
    return compare_policy_values(previous, current)


def test_partition_filter_merge_key_and_evidence_changes_block():
    cases = [
        (("partition_by", "field"), "other_date"),
        (("require_partition_filter",), False),
        (("incremental", "merge_key"), "event_id"),
        (("validation_evidence_level",), "cloud_observed"),
    ]
    for path, value in cases:
        assert any(item.severity == "blocking" for item in _change(path, value))


def test_scan_and_cluster_changes_require_manual_review():
    assert _change(("maximum_expected_scan_window_days",), 62)[0].severity == "manual_review"
    assert _change(("cluster_by",), ["channel", "currency"])[0].severity == "manual_review"


def test_threshold_loosening_blocks():
    previous = {"models": {"mart": {"blocking_threshold_bytes": 10}}}
    current = {"models": {"mart": {"blocking_threshold_bytes": 20}}}
    assert compare_cost_thresholds(previous, current)[0].severity == "blocking"


def test_threshold_loosening_with_bounded_waiver_is_recorded_as_info():
    previous = {"models": {"mart": {"blocking_threshold_bytes": 10}}}
    current = {"models": {"mart": {"blocking_threshold_bytes": 20}}}
    waivers = {
        "mart": {
            "owner": "data-platform",
            "reason": "bounded migration",
            "expires_at": "2027-01-01T00:00:00Z",
            "approved_threshold_bytes": 20,
        }
    }
    finding = compare_cost_thresholds(
        previous, current, waivers=waivers, now=datetime(2026, 8, 6, tzinfo=UTC)
    )[0]
    assert finding.severity == "info"
