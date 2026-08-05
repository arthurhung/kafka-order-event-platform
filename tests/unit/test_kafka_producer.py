"""Tests for callback accounting, bounded queue backoff, and flush."""

import logging
from typing import cast
from uuid import uuid4

from confluent_kafka import KafkaError, Message

from streaming_platform.config import Settings
from streaming_platform.generator.factory import GeneratedRecord, InjectionKind
from streaming_platform.generator.report import DeliveryTracker
from streaming_platform.kafka.producer import QUEUE_FULL_BACKOFF_SECONDS, TrackedKafkaProducer
from streaming_platform.models import EventType


def record() -> GeneratedRecord:
    return GeneratedRecord(
        topic="orders.v1",
        key=b"ORD-1",
        value=b"{}",
        event_id=uuid4(),
        event_type=EventType.ORDER_CREATED,
        family="order",
        injection_kind=InjectionKind.NORMAL,
    )


class FakeProducer:
    def __init__(self, queue_failures=0, deliver=True, delivery_error=None):
        self.queue_failures = queue_failures
        self.deliver = deliver
        self.delivery_error = delivery_error
        self.produce_calls = 0
        self.callback = None
        self.last_produce_arguments = None

    def produce(self, topic, **arguments):
        self.produce_calls += 1
        self.last_produce_arguments = {"topic": topic, **arguments}
        if self.produce_calls <= self.queue_failures:
            raise BufferError
        self.callback = arguments["on_delivery"]

    def poll(self, timeout):
        if self.deliver and self.callback is not None:
            callback, self.callback = self.callback, None
            callback(self.delivery_error, cast(Message, object()))
        return 0

    def flush(self, timeout):
        self.poll(timeout)
        return 0 if self.callback is None else 1


def logger() -> logging.LoggerAdapter[logging.Logger]:
    return logging.LoggerAdapter(logging.getLogger("producer-test"), {})


def test_delivery_callback_records_success(settings: Settings) -> None:
    tracker = DeliveryTracker()
    fake = FakeProducer()
    producer = TrackedKafkaProducer(settings, tracker, logger(), producer=fake)

    assert producer.send(record()) is True
    producer.flush()
    assert fake.last_produce_arguments["key"] == b"ORD-1"
    assert "partition" not in fake.last_produce_arguments
    assert tracker.snapshot()["delivered"] == 1
    assert tracker.snapshot()["failed"] == 0


def test_queue_full_uses_bounded_backoff(settings: Settings) -> None:
    tracker = DeliveryTracker()
    fake = FakeProducer(queue_failures=3)
    delays = []
    producer = TrackedKafkaProducer(
        settings, tracker, logger(), producer=fake, sleeper=delays.append
    )

    assert producer.send(record()) is True
    producer.flush()
    assert delays == list(QUEUE_FULL_BACKOFF_SECONDS)
    assert fake.produce_calls == 4
    assert tracker.snapshot()["delivered"] == 1


def test_delivery_callback_records_failure(settings: Settings) -> None:
    tracker = DeliveryTracker()
    fake = FakeProducer(delivery_error=KafkaError(KafkaError._ALL_BROKERS_DOWN))
    producer = TrackedKafkaProducer(settings, tracker, logger(), producer=fake)

    assert producer.send(record()) is True
    producer.flush()
    assert tracker.snapshot()["delivered"] == 0
    assert tracker.snapshot()["failed"] == 1


def test_flush_marks_unconfirmed_delivery_failed(settings: Settings) -> None:
    tracker = DeliveryTracker()
    producer = TrackedKafkaProducer(
        settings, tracker, logger(), producer=FakeProducer(deliver=False)
    )
    producer.send(record())

    assert producer.flush() == 1
    assert tracker.snapshot()["failed"] == 1
    assert tracker.snapshot()["pending"] == 0
