"""Tests for injection counters and JSON report output."""

import json
from datetime import UTC, datetime
from uuid import uuid4

from streaming_platform.generator.factory import GeneratedRecord, InjectionKind, InvalidKind
from streaming_platform.generator.report import DeliveryTracker, ProducerReport, write_report
from streaming_platform.models import EventType


def make_record(kind, invalid_kind=None) -> GeneratedRecord:
    return GeneratedRecord(
        topic="orders.v1",
        key=b"ORD-1",
        value=b"{}",
        event_id=uuid4(),
        event_type=EventType.ORDER_CREATED,
        family="order",
        injection_kind=kind,
        invalid_kind=invalid_kind,
    )


def test_tracker_keeps_injection_counts_separate() -> None:
    tracker = DeliveryTracker()
    records = (
        make_record(InjectionKind.INVALID, InvalidKind.NEGATIVE_AMOUNT),
        make_record(InjectionKind.STALE),
        make_record(InjectionKind.DUPLICATE),
    )
    for item in records:
        tracker.delivered(tracker.begin_attempt(item))

    snapshot = tracker.snapshot()
    assert snapshot["invalid_events_injected"] == 1
    assert snapshot["stale_events_injected"] == 1
    assert snapshot["duplicate_events_injected"] == 1
    assert snapshot["invalid_events_by_type"] == {"negative_amount": 1}


def test_write_report_creates_utf8_json(tmp_path) -> None:
    now = datetime.now(UTC)
    report = ProducerReport(
        started_at=now,
        finished_at=now,
        target_events_per_second=10,
        duration_seconds=1,
        elapsed_seconds=1,
        actual_events_per_second=10,
        attempted=10,
        delivered=10,
        failed=0,
        order_events_attempted=5,
        log_events_attempted=5,
        invalid_events_injected=1,
        stale_events_injected=1,
        duplicate_events_injected=1,
        invalid_events_by_type={"negative_amount": 1},
        seed=42,
        stale_hours=168,
    )
    path = tmp_path / "nested" / "report.json"
    write_report(report, path)

    assert json.loads(path.read_text(encoding="utf-8"))["stale_events_injected"] == 1
