import json
from pathlib import Path

from data_platform.dbt_review import (
    DbtReviewRequest,
    detect_changed_model_paths,
    review_dbt_changes,
)

FIXTURES = Path("tests/data_platform/fixtures/phase10")


def _review(name: str):
    request = DbtReviewRequest.model_validate_json((FIXTURES / name).read_text())
    return review_dbt_changes(request)


def test_normal_review_is_passed_and_deterministic():
    first = _review("review_normal.json")
    second = _review("review_normal.json")
    assert first.status == "passed"
    assert first.model_dump() == second.model_dump()


def test_blocking_fixture_covers_contract_and_governance_risks():
    report = _review("review_blocking.json")
    rules = {finding.rule: finding.severity for finding in report.findings}
    assert report.status == "blocked"
    assert rules["published-contract-column-removed"] == "blocking"
    assert rules["missing-owner"] == "blocking"
    assert rules["missing-slo"] == "blocking"
    assert rules["missing-contract"] == "blocking"
    assert rules["unsafe-incremental-lookback"] == "blocking"
    assert rules["multi-currency-aggregation"] == "blocking"


def test_warning_fixture_marks_missing_baseline_and_lineage_degraded():
    report = _review("review_warning.json")
    rules = {finding.rule for finding in report.findings}
    assert report.status == "degraded"
    assert {"baseline-unavailable", "lineage-unavailable", "divide-by-zero"} <= rules


def test_type_change_missing_docs_partition_and_direct_source_are_detected():
    payload = json.loads((FIXTURES / "review_normal.json").read_text())
    model = payload["models"][0]
    model.update(
        {
            "columns": {
                "order_id": {"data_type": "integer", "description": "Identifier."},
                "amount": {"data_type": "numeric", "description": ""},
            },
            "baseline_columns": {"order_id": "varchar"},
            "sql": "select * from {{ source('x', 'y') }} where event_date is not null",
            "materialized": "incremental",
        }
    )
    report = review_dbt_changes(DbtReviewRequest.model_validate(payload))
    rules = {finding.rule for finding in report.findings}
    assert {
        "published-contract-type-changed",
        "missing-column-description",
        "direct-source-usage",
        "select-star",
        "unsafe-incremental-lookback",
        "missing-partition-filter",
    } <= rules


def test_finding_schema_has_required_stable_fields():
    finding = _review("review_blocking.json").findings[0].model_dump()
    assert set(finding) == {
        "severity",
        "file",
        "model",
        "rule",
        "reason",
        "impact",
        "recommendation",
        "evidence",
    }


def test_changed_model_detection_is_stable_and_rejects_traversal():
    assert detect_changed_model_paths(
        [
            "README.md",
            "dbt/models/marts/z.sql",
            "dbt/models/../profiles.yml",
            "dbt/models/staging/a.yml",
            "dbt/models/marts/z.sql",
        ]
    ) == ["dbt/models/marts/z.sql", "dbt/models/staging/a.yml"]


def test_duplicated_business_logic_is_reported():
    payload = json.loads((FIXTURES / "review_normal.json").read_text())
    duplicate = dict(payload["models"][0])
    duplicate.update({"name": "fct_safe_copy", "file": "dbt/models/marts/fct_safe_copy.sql"})
    payload["models"].append(duplicate)
    report = review_dbt_changes(DbtReviewRequest.model_validate(payload))
    assert any(finding.rule == "duplicated-business-logic" for finding in report.findings)


def test_join_weighted_average_model_description_and_grain_risks_are_detected():
    payload = json.loads((FIXTURES / "review_normal.json").read_text())
    model = payload["models"][0]
    model.update(
        {
            "description": "",
            "baseline_grain": "Grain: one row per customer_id",
            "sql": (
                "select avg(average_response_time_ms) "
                "from {{ ref('int_a') }} join {{ ref('int_b') }}"
            ),
        }
    )
    report = review_dbt_changes(DbtReviewRequest.model_validate(payload))
    rules = {finding.rule for finding in report.findings}
    assert {"missing-model-description", "join-explosion-risk", "unweighted-average"} <= rules
    model["description"] = "Grain: one row per account_id."
    grain_report = review_dbt_changes(DbtReviewRequest.model_validate(payload))
    assert any(finding.rule == "published-grain-changed" for finding in grain_report.findings)
