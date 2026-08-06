import copy
import json
from pathlib import Path

from data_platform.phase8a_common import load_json_object
from data_platform.phase8a_orchestration import quality_gate, validate_orchestration_contract

CONTRACT = Path("config/data_platform/phase8a_dag_contract.json")


def test_local_orchestration_contract_passes_without_airflow():
    report = validate_orchestration_contract(CONTRACT)
    assert report["status"] == "passed"
    assert report["airflow_runtime"] == "not_available"
    assert report["airflow_import_status"] == "not_available"
    assert report["cloud_execution_status"] == "not_executed"


def test_unbounded_retry_timeout_and_cloud_task_block(tmp_path: Path):
    value = copy.deepcopy(load_json_object(CONTRACT))
    value["tasks"][0]["retries"] = 99
    value["tasks"][1]["timeout_seconds"] = 0
    value["cloud_tasks"] = ["bigquery_load"]
    path = tmp_path / "contract.json"
    path.write_text(json.dumps(value))
    report = validate_orchestration_contract(path)
    assert report["status"] == "blocked"
    assert {item["rule"] for item in report["policy_findings"] if item["severity"] == "error"} == {
        "retries",
        "timeout",
        "cloud-tasks",
    }


def test_quality_gate_propagates_failures():
    assert quality_gate(["passed", "warn", "pass"]) == "passed"
    assert quality_gate(["passed", "blocked"]) == "blocked"
