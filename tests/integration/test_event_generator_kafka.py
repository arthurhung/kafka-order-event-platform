"""Real-Kafka integration tests for Phase 2 producer behavior."""

import json
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from time import monotonic
from uuid import uuid4

import pytest
from confluent_kafka import Consumer, TopicPartition
from pydantic import ValidationError

from streaming_platform.config import Settings
from streaming_platform.generator.factory import (
    LOG_SERVICE_CANDIDATES,
    EventFactory,
    GeneratedRecord,
    InjectionKind,
    InvalidKind,
)
from streaming_platform.generator.report import DeliveryTracker
from streaming_platform.kafka.admin import ensure_topics
from streaming_platform.kafka.producer import TrackedKafkaProducer
from streaming_platform.logging import configure_logging
from streaming_platform.models import (
    PLATFORM_EVENT_ADAPTER,
    ApplicationErrorLogEvent,
    ApplicationErrorLogPayload,
    EventType,
)

pytestmark = pytest.mark.integration


def tail_consumer(settings: Settings) -> Consumer:
    consumer = Consumer(
        {
            "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
            "group.id": f"phase-two-integration-{uuid4()}",
            "enable.auto.commit": False,
        }
    )
    metadata = consumer.list_topics(timeout=10)
    assignments = []
    for topic in (settings.KAFKA_ORDER_TOPIC, settings.KAFKA_LOG_TOPIC):
        for partition_id in metadata.topics[topic].partitions:
            partition = TopicPartition(topic, partition_id)
            _low, high = consumer.get_watermark_offsets(partition, timeout=10)
            assignments.append(TopicPartition(topic, partition_id, high))
    if not assignments:
        consumer.close()
        raise RuntimeError("Kafka integration topics have no partitions")
    consumer.assign(assignments)
    return consumer


def read_messages(consumer: Consumer, count: int) -> list:
    messages = []
    deadline = monotonic() + 15
    while len(messages) < count and monotonic() < deadline:
        message = consumer.poll(0.25)
        if message is None:
            continue
        if message.error() is not None:
            raise RuntimeError(str(message.error()))
        messages.append(message)
    if len(messages) != count:
        raise TimeoutError(f"Expected {count} Kafka messages, received {len(messages)}")
    return messages


def application_error_record(settings: Settings, service: str) -> GeneratedRecord:
    event = ApplicationErrorLogEvent(
        event_id=uuid4(),
        event_time=datetime.now(UTC),
        source=service,
        payload=ApplicationErrorLogPayload(
            request_id=f"REQ-{uuid4()}",
            service=service,
            error_type="PartitionRoutingTest",
            error_message="Phase 2 partition routing verification",
        ),
    )
    return GeneratedRecord(
        topic=settings.KAFKA_LOG_TOPIC,
        key=service.encode(),
        value=event.model_dump_json().encode(),
        event_id=event.event_id,
        event_type=event.event_type,
        family="log",
        injection_kind=InjectionKind.NORMAL,
    )


def test_all_event_types_round_trip_with_expected_topics_and_keys(settings: Settings) -> None:
    ensure_topics(settings)
    consumer = tail_consumer(settings)
    tracker = DeliveryTracker()
    logger = configure_logging(settings, "phase-two-integration")
    producer = TrackedKafkaProducer(settings, tracker, logger)
    factory = EventFactory(settings, seed=42)
    records = [factory.create_normal(event_type) for event_type in EventType]
    try:
        for record in records:
            producer.send(record)
        producer.flush()
        messages = read_messages(consumer, len(records))
    finally:
        consumer.close()

    parsed = [PLATFORM_EVENT_ADAPTER.validate_json(message.value()) for message in messages]
    by_id = {
        event.event_id: (event, message) for event, message in zip(parsed, messages, strict=True)
    }
    assert set(by_id) == {record.event_id for record in records}
    for record in records:
        event, message = by_id[record.event_id]
        assert message.topic() == record.topic
        expected_key = (
            event.payload.order_id if hasattr(event.payload, "order_id") else event.payload.service
        )
        assert message.key().decode() == expected_key
    assert tracker.snapshot()["delivered"] == 6
    assert tracker.snapshot()["failed"] == 0


def test_invalid_stale_and_duplicate_are_distinct_over_real_kafka(settings: Settings) -> None:
    ensure_topics(settings)
    consumer = tail_consumer(settings)
    tracker = DeliveryTracker()
    logger = configure_logging(settings, "phase-two-integration")
    producer = TrackedKafkaProducer(settings, tracker, logger)
    now = datetime.now(UTC)
    factory = EventFactory(settings, seed=7, clock=lambda: now)
    original = factory.create_normal(EventType.ORDER_CREATED)
    records = [
        original,
        factory.create_duplicate(original),
        factory.create_stale(EventType.APPLICATION_ERROR_LOG, stale_hours=168),
        factory.create_invalid(EventType.API_ACCESS_LOG, InvalidKind.INVALID_HTTP_METHOD),
    ]
    try:
        for record in records:
            producer.send(record)
        producer.flush()
        messages = read_messages(consumer, len(records))
    finally:
        consumer.close()

    values = [json.loads(message.value()) for message in messages]
    duplicate_ids = [value["event_id"] for value in values]
    assert duplicate_ids.count(str(original.event_id)) == 2
    stale_value = next(value for value in values if value["event_id"] == str(records[2].event_id))
    stale = PLATFORM_EVENT_ADAPTER.validate_python(stale_value)
    assert stale.event_time == now - timedelta(hours=168)
    invalid_value = next(value for value in values if value["event_id"] == str(records[3].event_id))
    with pytest.raises(ValidationError):
        PLATFORM_EVENT_ADAPTER.validate_python(invalid_value)

    snapshot = tracker.snapshot()
    assert snapshot["invalid_events_injected"] == 1
    assert snapshot["stale_events_injected"] == 1
    assert snapshot["duplicate_events_injected"] == 1
    assert snapshot["delivered"] == 4


def test_service_keys_are_stable_and_span_multiple_partitions(settings: Settings) -> None:
    ensure_topics(settings)
    consumer = tail_consumer(settings)
    tracker = DeliveryTracker()
    logger = configure_logging(settings, "phase-two-partition-integration")
    producer = TrackedKafkaProducer(settings, tracker, logger)
    records = [
        application_error_record(settings, service)
        for service in LOG_SERVICE_CANDIDATES
        for _ in range(2)
    ]
    try:
        for record in records:
            producer.send(record)
        producer.flush()
        messages = read_messages(consumer, len(records))
    finally:
        consumer.close()

    partitions_by_service: defaultdict[str, set[int]] = defaultdict(set)
    for message in messages:
        partitions_by_service[message.key().decode()].add(message.partition())

    assert set(partitions_by_service) == set(LOG_SERVICE_CANDIDATES)
    assert all(len(partitions) == 1 for partitions in partitions_by_service.values())
    observed_partitions = {next(iter(partitions)) for partitions in partitions_by_service.values()}
    assert len(observed_partitions) >= 2
    assert tracker.snapshot()["delivered"] == len(records)
    assert tracker.snapshot()["failed"] == 0

    mapping = {
        service: next(iter(partitions_by_service[service])) for service in LOG_SERVICE_CANDIDATES
    }
    print("service_partition_mapping=" + json.dumps(mapping, sort_keys=True))
