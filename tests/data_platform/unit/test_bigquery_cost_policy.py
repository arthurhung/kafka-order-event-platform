import copy
from datetime import UTC, datetime
from pathlib import Path

import pytest

from data_platform.bigquery_cost_policy import evaluate_cost_policy, load_cost_policy
from data_platform.phase8a_common import load_json_object

POLICY = Path("config/data_platform/bigquery_cost_policy.json")
FIXTURES = Path("tests/data_platform/fixtures/bigquery_dry_run")
NOW = datetime(2026, 8, 6, tzinfo=UTC)


def _evaluate(name: str):
    return evaluate_cost_policy(
        load_cost_policy(POLICY), load_json_object(FIXTURES / name), now=NOW
    )


@pytest.mark.parametrize("name", ["below_warning.json"])
def test_below_warning_passes(name: str):
    assert _evaluate(name).decision == "pass"


@pytest.mark.parametrize("name", ["exactly_warning.json", "above_warning.json"])
def test_warning_threshold_warns_without_blocking(name: str):
    result = _evaluate(name)
    assert result.decision == "warn"
    assert not result.blocked


@pytest.mark.parametrize("name", ["exactly_blocking.json", "above_blocking.json"])
def test_blocking_threshold_is_inclusive(name: str):
    assert _evaluate(name).decision == "block"


@pytest.mark.parametrize("name", ["missing_partition_filter.json", "oversized_scan_window.json"])
def test_partition_and_scan_policy_block(name: str):
    assert _evaluate(name).decision == "block"


def test_valid_waiver_passes_and_is_recorded():
    result = _evaluate("valid_waiver.json")
    assert result.decision == "pass"
    assert result.waiver_applied is True


@pytest.mark.parametrize(
    "name",
    [
        "expired_waiver.json",
        "malformed_waiver.json",
        "missing_waiver_owner.json",
        "missing_waiver_reason.json",
    ],
)
def test_expired_and_malformed_waivers_block(name: str):
    assert _evaluate(name).decision == "block"


def test_mutated_missing_waiver_fields_block():
    fixture = load_json_object(FIXTURES / "valid_waiver.json")
    for field in ("owner", "reason"):
        changed = copy.deepcopy(fixture)
        del changed["waiver"][field]
        result = evaluate_cost_policy(load_cost_policy(POLICY), changed, now=NOW)
        assert result.decision == "block"


def test_missing_estimate_remains_null_and_is_invalid():
    result = _evaluate("missing_estimate.json")
    assert result.decision == "invalid"
    assert result.estimated_bytes is None
    assert result.to_dict()["estimated_bytes"] is None


def test_invalid_query_fixture_is_invalid_without_zero_estimate():
    result = _evaluate("invalid_query.json")
    assert result.decision == "invalid"
    assert result.estimated_bytes is None
