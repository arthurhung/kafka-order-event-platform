"""Tests for EPS pacing, flush, and measured runner reports."""

import json
import logging
from datetime import UTC, datetime, timedelta
from uuid import uuid4

from streaming_platform.generator.factory import GeneratedRecord, InjectionKind
from streaming_platform.generator.options import GeneratorOptions
from streaming_platform.generator.report import DeliveryTracker
from streaming_platform.generator.runner import GeneratorRunner
from streaming_platform.models import EventType


class FakeTime:
    def __init__(self):
        self.elapsed = 0.0
        self.started_at = datetime(2026, 8, 4, 12, 0, tzinfo=UTC)

    def monotonic(self):
        return self.elapsed

    def sleep(self, seconds):
        self.elapsed += seconds

    def now(self):
        return self.started_at + timedelta(seconds=self.elapsed)


class FakeFactory:
    def __init__(self, generated_record):
        self.generated_record = generated_record
        self.calls = 0

    def next_record(self, options):
        self.calls += 1
        return self.generated_record


class ImmediateProducer:
    def __init__(self, tracker):
        self.tracker = tracker
        self.flush_calls = 0

    def send(self, generated_record):
        token = self.tracker.begin_attempt(generated_record)
        self.tracker.delivered(token)
        return True

    def flush(self):
        self.flush_calls += 1
        return 0


def test_runner_paces_events_flushes_and_writes_measured_report(tmp_path) -> None:
    generated_record = GeneratedRecord(
        topic="orders.v1",
        key=b"ORD-1",
        value=b"{}",
        event_id=uuid4(),
        event_type=EventType.ORDER_CREATED,
        family="order",
        injection_kind=InjectionKind.NORMAL,
    )
    options = GeneratorOptions(
        events_per_second=2,
        duration_seconds=1,
        order_ratio=1,
        log_ratio=0,
        report_path=tmp_path / "report.json",
    )
    tracker = DeliveryTracker()
    fake_time = FakeTime()
    factory = FakeFactory(generated_record)
    producer = ImmediateProducer(tracker)
    runner = GeneratorRunner(
        options,
        factory,
        producer,
        tracker,
        logging.LoggerAdapter(logging.getLogger("runner-test"), {}),
        clock=fake_time.now,
        monotonic_clock=fake_time.monotonic,
        sleeper=fake_time.sleep,
    )

    report = runner.run()

    assert fake_time.elapsed == 1
    assert factory.calls == 2
    assert producer.flush_calls == 1
    assert report.attempted == report.delivered == 2
    assert report.actual_events_per_second == 2
    assert json.loads(options.report_path.read_text())["delivered"] == 2
