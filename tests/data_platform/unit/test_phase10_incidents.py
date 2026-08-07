import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from data_platform.incidents import (
    FixtureEvidenceClient,
    IncidentAlert,
    IncidentDiagnoser,
    IncidentState,
    render_markdown,
    write_incident_report,
)

FIXTURES = Path(".agents/skills/incident-diagnosis/fixtures")


def _diagnose(name: str):
    fixture = json.loads((FIXTURES / name).read_text())
    alert = IncidentAlert.model_validate(fixture["alert"])
    return IncidentDiagnoser(FixtureEvidenceClient(fixture["responses"])).diagnose(alert)


@pytest.mark.parametrize(
    ("fixture", "status", "phrase"),
    [
        ("freshness_failure.json", "completed", "pipeline task failure"),
        ("lag_failure.json", "completed", "Kafka lag"),
        ("quality_failure.json", "completed", "quality gate failure"),
        ("insufficient_evidence.json", "degraded", "does not support a single"),
    ],
)
def test_required_scenarios(fixture, status, phrase):
    report = _diagnose(fixture)
    assert report.status == status
    assert phrase.casefold() in report.most_likely_cause.description.casefold()
    assert IncidentState.HUMAN_REVIEW_REQUIRED in report.state_history
    assert all(fact.evidence for fact in report.confirmed_facts)
    evidence_ids = {item.evidence_id for item in report.evidence_inventory}
    assert all(item in evidence_ids for fact in report.confirmed_facts for item in fact.evidence)


def test_insufficient_evidence_is_degraded_deterministic_and_has_unknowns():
    first = _diagnose("insufficient_evidence.json")
    second = _diagnose("insufficient_evidence.json")
    assert first.model_dump() == second.model_dump()
    assert IncidentState.DEGRADED_DIAGNOSIS in first.state_history
    assert first.unknowns
    assert first.most_likely_cause.confidence == "low"
    quality = next(item for item in first.evidence_inventory if item.source == "get_quality_status")
    assert quality.freshness_status == "stale"


def test_evidence_levels_and_optional_cloud_unavailability_are_distinct():
    report = _diagnose("lag_failure.json")
    levels = {item.evidence_level for item in report.evidence_inventory}
    assert {"local_execution", "static_validation", "not_available"} <= levels
    assert report.status == "completed"


def test_stale_required_evidence_degrades_but_stale_optional_cost_does_not():
    fixture = json.loads((FIXTURES / "lag_failure.json").read_text())
    fixture["responses"]["get_quality_status"]["generated_at"] = "2026-01-01T00:00:00Z"
    stale_required = IncidentDiagnoser(FixtureEvidenceClient(fixture["responses"])).diagnose(
        IncidentAlert.model_validate(fixture["alert"])
    )
    assert stale_required.status == "degraded"

    fixture = json.loads((FIXTURES / "lag_failure.json").read_text())
    fixture["responses"]["get_cost_estimate"]["generated_at"] = "2026-01-01T00:00:00Z"
    stale_optional = IncidentDiagnoser(FixtureEvidenceClient(fixture["responses"])).diagnose(
        IncidentAlert.model_validate(fixture["alert"])
    )
    assert stale_optional.status == "completed"


def test_alert_rejects_extra_mutation_path_sql_and_shell_fields():
    base = {
        "incident_id": "INC-SAFE-001",
        "alert_type": "quality",
        "asset": "fct_orders",
        "observed_at": "2026-08-07T00:00:00Z",
        "severity": "high",
        "message": "Quality failed.",
    }
    for extra in (
        {"sql": "drop table x"},
        {"shell": "rm -rf x"},
        {"artifact_path": "../../secret"},
    ):
        with pytest.raises(ValidationError):
            IncidentAlert.model_validate(base | extra)
    with pytest.raises(ValidationError):
        IncidentAlert.model_validate(base | {"message": "select * from x"})


def test_report_formats_redact_secret_and_do_not_execute_validation(tmp_path):
    fixture = json.loads((FIXTURES / "quality_failure.json").read_text())
    fixture["responses"]["get_model_owner"]["data"].update(
        {"api_token": "phase10-redaction-fixture"}
    )
    report = IncidentDiagnoser(FixtureEvidenceClient(fixture["responses"])).diagnose(
        IncidentAlert.model_validate(fixture["alert"])
    )
    json_path, markdown_path = write_incident_report(report, tmp_path)
    combined = json_path.read_text() + markdown_path.read_text()
    assert "phase10-redaction-fixture" not in combined
    assert "[REDACTED]" in combined
    assert "make dbt-build" in json_path.read_text()
    assert render_markdown(report).startswith("# Incident")


def test_mutation_methods_do_not_exist():
    forbidden = {
        "execute_sql",
        "execute_shell",
        "rerun_pipeline",
        "reset_offsets",
        "mutate_schema",
        "merge_pr",
        "create_cloud_resource",
    }
    assert forbidden.isdisjoint(dir(IncidentDiagnoser))
    assert forbidden.isdisjoint(dir(FixtureEvidenceClient))


def test_tool_failure_degrades_without_fabricated_root_cause():
    fixture = json.loads((FIXTURES / "freshness_failure.json").read_text())
    del fixture["responses"]["get_recent_pipeline_failures"]
    report = IncidentDiagnoser(FixtureEvidenceClient(fixture["responses"])).diagnose(
        IncidentAlert.model_validate(fixture["alert"])
    )
    assert report.status == "degraded"
    assert report.most_likely_cause.confidence == "low"
    assert "single root cause" in report.most_likely_cause.description


def test_timeout_degrades_without_fabricated_root_cause():
    fixture = json.loads((FIXTURES / "freshness_failure.json").read_text())

    class TimeoutClient(FixtureEvidenceClient):
        def call(self, tool_name, arguments):
            if tool_name == "get_recent_pipeline_failures":
                raise TimeoutError
            return super().call(tool_name, arguments)

    report = IncidentDiagnoser(TimeoutClient(fixture["responses"])).diagnose(
        IncidentAlert.model_validate(fixture["alert"])
    )
    assert report.status == "degraded"
    assert report.most_likely_cause.confidence == "low"
