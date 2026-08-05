"""Executable Phase 5 mixed demo and recovery path over real services."""

import subprocess

import pytest

from streaming_platform.benchmark.report import BenchmarkReport

pytestmark = pytest.mark.e2e


def test_demo_generates_report_and_leaves_no_consumer_process() -> None:
    completed = subprocess.run(
        ["make", "demo"],  # noqa: S607
        check=False,
        capture_output=True,
        text=True,
        timeout=180,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
    report_line = next(
        line for line in completed.stdout.splitlines() if '"report_path"' in line
    )
    path_text = report_line.split('"report_path": "', maxsplit=1)[1].split('"', maxsplit=1)[0]
    report = BenchmarkReport.model_validate_json(open(path_text, encoding="utf-8").read())
    assert report.status == "passed"
    assert report.database["valid_orders_count"] > 0
    assert report.database["log_processed_events_count"] > 0
    assert report.consumer["dlq_count"] > 0
    assert report.consumer["failure_scenario"]["observed_lag_after_stop"] > 0
    assert report.consumer["failure_scenario"]["final_available_lag"] == 0
    assert report.consumer["uncommitted_replay"]["database_rows_after_restart"] == 1

    processes = subprocess.run(
        ["ps", "-ax", "-o", "command="],  # noqa: S607
        check=True,
        capture_output=True,
        text=True,
        timeout=10,
    ).stdout
    assert "-m apps.order_consumer" not in processes
    assert "-m apps.log_consumer" not in processes
