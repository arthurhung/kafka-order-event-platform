import json
from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from streaming_platform.benchmark.identity import create_run_id, report_path
from streaming_platform.benchmark.report import (
    BenchmarkReport,
    BenchmarkStatus,
    LatencyMetric,
    status_for_run,
    write_benchmark_report,
)


def report(now: datetime) -> BenchmarkReport:
    unavailable = LatencyMetric(
        status="not_available",
        definition="test",
        sample_count=0,
        average_ms=None,
        p95_ms=None,
        p99_ms=None,
        reason="empty sample",
    )
    return BenchmarkReport(
        run_id="run-1",
        profile="smoke",
        started_at=now,
        finished_at=now,
        status=BenchmarkStatus.PASSED,
        exit_code=0,
        environment={},
        configuration={},
        producer={},
        consumer={},
        latency={"producer_delivery_latency_ms": unavailable},
        runtime={},
        database={},
        run_scope={},
    )


def test_report_serializes_utf8_utc_and_null_metrics(tmp_path) -> None:
    now = datetime(2026, 8, 5, 10, 0, tzinfo=UTC)
    value = report(now)
    path = tmp_path / "報告.json"
    write_benchmark_report(value, path)
    decoded = json.loads(path.read_text(encoding="utf-8"))
    assert decoded["started_at"].endswith("Z")
    assert decoded["latency"]["producer_delivery_latency_ms"]["average_ms"] is None


def test_report_rejects_naive_timestamp() -> None:
    with pytest.raises(ValidationError, match="timezone"):
        report(datetime(2026, 8, 5, 10, 0))


def test_status_rules_and_unique_filename() -> None:
    assert status_for_run(failed=True, unavailable_required_metric=False) == "failed"
    assert status_for_run(failed=False, unavailable_required_metric=True) == "partial"
    assert status_for_run(failed=False, unavailable_required_metric=False) == "passed"
    run_id = create_run_id(datetime(2026, 8, 5, tzinfo=UTC))
    path = report_path(__import__("pathlib").Path("reports"), run_id, "smoke")
    assert run_id.startswith("20260805T000000")
    assert run_id in path.name
