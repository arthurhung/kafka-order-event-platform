import json
import os
from pathlib import Path

import pytest
from pydantic import ValidationError

from data_platform.incidents import FixtureEvidenceClient
from data_platform.skill_scaffold import (
    DbtScaffoldRequest,
    ScaffoldWorkflowError,
    scaffold_verified_model,
)

FIXTURE = Path("tests/data_platform/fixtures/phase10/scaffold_request.json")
TEMPLATES = Path(".agents/skills/dbt-scaffold/templates")


def _request(**updates):
    payload = json.loads(FIXTURE.read_text())
    payload.update(updates)
    return DbtScaffoldRequest.model_validate(payload)


def _client(columns=("event_id", "order_id")):
    return FixtureEvidenceClient(
        {
            "get_model_schema": {
                "status": "ok",
                "data": {"columns": [{"name": name, "data_type": "text"} for name in columns]},
                "evidence": ["dbt_manifest"],
            }
        }
    )


def test_valid_scaffold_is_deterministic_and_uses_metadata(tmp_path):
    outputs = []
    for index in range(2):
        project = tmp_path / str(index)
        report = scaffold_verified_model(
            _request(), client=_client(), project_dir=project, template_dir=TEMPLATES
        )
        outputs.append({path: (project / path).read_text() for path in report.generated_files})
        assert report.evidence_ids == ["dbt_manifest"]
    assert outputs[0] == outputs[1]
    sql = outputs[0]["models/intermediate/int_phase10_reserved_smoke.sql"]
    unit_test = outputs[0]["models/intermediate/int_phase10_reserved_smoke_unit_test.yml"]
    assert "{{ ref('stg_order_events') }}" in sql
    assert "input: ref('stg_order_events')" in unit_test


@pytest.mark.parametrize(
    ("updates", "message"),
    [
        ({"model_name": "../escape"}, "model_name"),
        ({"model_name": "Invalid-Name"}, "model_name"),
        ({"model_name": "mart_no_grain", "layer": "marts", "grain": None}, "grain"),
        ({"owner": "../../owner"}, "owner"),
    ],
)
def test_request_rejects_unsafe_or_incomplete_values(updates, message):
    with pytest.raises(ValidationError, match=message):
        _request(**updates)


def test_missing_column_and_fake_source_are_rejected_before_write(tmp_path):
    with pytest.raises(ScaffoldWorkflowError, match="do not exist"):
        scaffold_verified_model(
            _request(selected_columns=["invented_column"]),
            client=_client(),
            project_dir=tmp_path,
            template_dir=TEMPLATES,
        )
    with pytest.raises(ScaffoldWorkflowError, match="not available"):
        scaffold_verified_model(
            _request(upstream_model="fake_source"),
            client=FixtureEvidenceClient(
                {"get_model_schema": {"status": "not_found", "data": {}, "evidence": []}}
            ),
            project_dir=tmp_path,
            template_dir=TEMPLATES,
        )
    assert not list(tmp_path.rglob("*.sql"))


def test_missing_metadata_type_is_rejected_instead_of_inventing_contract(tmp_path):
    client = FixtureEvidenceClient(
        {
            "get_model_schema": {
                "status": "ok",
                "data": {"columns": [{"name": "event_id", "data_type": None}]},
                "evidence": ["dbt_manifest"],
            }
        }
    )
    with pytest.raises(ScaffoldWorkflowError, match="no verified data type"):
        scaffold_verified_model(
            _request(selected_columns=["event_id"]),
            client=client,
            project_dir=tmp_path,
            template_dir=TEMPLATES,
        )
    assert not list(tmp_path.rglob("*.sql"))


def test_existing_model_refuses_overwrite(tmp_path):
    first = scaffold_verified_model(
        _request(), client=_client(), project_dir=tmp_path, template_dir=TEMPLATES
    )
    original = (tmp_path / first.generated_files[0]).read_text()
    with pytest.raises(ScaffoldWorkflowError, match="overwrite"):
        scaffold_verified_model(
            _request(), client=_client(), project_dir=tmp_path, template_dir=TEMPLATES
        )
    assert (tmp_path / first.generated_files[0]).read_text() == original


def test_validation_command_result_preserves_failures():
    from data_platform.skill_scaffold import ScaffoldCommandResult

    result = ScaffoldCommandResult(
        command="dbt build --select int_x+", exit_code=1, result="failed"
    )
    assert result.exit_code == 1
    assert result.result == "failed"


def test_partial_generation_failure_cleans_all_targets(tmp_path, monkeypatch):
    original = os.replace
    calls = 0

    def fail_second(source, destination):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError("fixture write failure")
        original(source, destination)

    monkeypatch.setattr("data_platform.skill_scaffold.os.replace", fail_second)
    with pytest.raises(OSError, match="fixture write failure"):
        scaffold_verified_model(
            _request(), client=_client(), project_dir=tmp_path, template_dir=TEMPLATES
        )
    assert not list(tmp_path.rglob("int_phase10_reserved_smoke*"))
