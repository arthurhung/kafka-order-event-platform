"""Real Kafka and PostgreSQL integration tests for Phase 4 log processing."""

import json
import logging
from collections.abc import Iterator
from datetime import UTC, datetime
from decimal import Decimal
from time import monotonic
from uuid import UUID, uuid4

import pytest
from confluent_kafka import Consumer, Message, Producer, TopicPartition
from sqlalchemy import delete, select
from sqlalchemy.exc import OperationalError

from streaming_platform.config import Settings
from streaming_platform.consumer.log import LogFlushService, LogMessageProcessor
from streaming_platform.consumer.offsets import ContiguousOffsetTracker
from streaming_platform.consumer.retry import RetriesExhaustedError, RetryPolicy
from streaming_platform.database.log_repository import LogMetricRepository
from streaming_platform.database.models import LogMetricMinute, ProcessedEvent
from streaming_platform.database.session import create_database_engine, create_session_factory
from streaming_platform.kafka.admin import ensure_topics
from streaming_platform.kafka.consumer import LogKafkaConsumer
from streaming_platform.kafka.dlq import DlqProducer
from streaming_platform.metrics.log_aggregation import KafkaCoordinate, LogAggregationBuffer
from streaming_platform.models import ApiAccessLogEvent, ApiAccessLogPayload

pytestmark = pytest.mark.integration


@pytest.fixture
def engine(settings: Settings) -> Iterator:
    database_engine = create_database_engine(settings)
    with database_engine.begin() as connection:
        connection.execute(delete(LogMetricMinute))
        connection.execute(
            delete(ProcessedEvent).where(
                ProcessedEvent.consumer_group == settings.KAFKA_LOG_CONSUMER_GROUP
            )
        )
    yield database_engine
    database_engine.dispose()


def logger() -> logging.LoggerAdapter[logging.Logger]:
    return logging.LoggerAdapter(logging.getLogger("phase-four-integration"), {})


def event(
    *,
    event_id: UUID | None = None,
    minute: datetime | None = None,
    service: str = "order-api",
    endpoint: str = "/orders",
    status: int = 200,
    response_ms: int = 10,
) -> ApiAccessLogEvent:
    return ApiAccessLogEvent(
        event_id=event_id or uuid4(),
        event_time=minute or datetime(2026, 8, 5, 10, 1, 30, tzinfo=UTC),
        source=service,
        payload=ApiAccessLogPayload(
            request_id=f"REQ-{uuid4()}",
            service=service,
            endpoint=endpoint,
            http_method="GET",
            status_code=status,
            response_time_ms=response_ms,
            client_ip="10.0.0.1",
        ),
    )


def produce(
    settings: Settings,
    value: bytes,
    *,
    key: bytes = b"order-api",
    partition: int | None = None,
) -> TopicPartition:
    delivered: list[TopicPartition] = []
    errors: list[str] = []

    def callback(error, message: Message) -> None:
        if error is not None:
            errors.append(str(error))
        else:
            delivered.append(TopicPartition(message.topic(), message.partition(), message.offset()))

    producer = Producer({"bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS})
    arguments = {"key": key, "value": value, "on_delivery": callback}
    if partition is not None:
        arguments["partition"] = partition
    producer.produce(settings.KAFKA_LOG_TOPIC, **arguments)
    remaining = producer.flush(10)
    if remaining or errors or not delivered:
        raise RuntimeError(errors[0] if errors else "log test message was not delivered")
    return delivered[0]


def assigned_consumer(
    settings: Settings, positions: list[TopicPartition]
) -> tuple[LogKafkaConsumer, Consumer]:
    earliest: dict[int, int] = {}
    for position in positions:
        earliest[position.partition] = min(
            position.offset, earliest.get(position.partition, position.offset)
        )
    assignments = [
        TopicPartition(settings.KAFKA_LOG_TOPIC, partition, offset)
        for partition, offset in earliest.items()
    ]
    raw = Consumer(
        {
            "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
            "group.id": settings.KAFKA_LOG_CONSUMER_GROUP,
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
        }
    )
    raw.assign(assignments)
    raw.commit(offsets=assignments, asynchronous=False)
    wrapped = LogKafkaConsumer(settings, consumer=raw)
    raw.assign(assignments)
    return wrapped, raw


def read_messages(consumer: LogKafkaConsumer, count: int) -> list:
    messages = []
    deadline = monotonic() + 10
    while len(messages) < count and monotonic() < deadline:
        message = consumer.poll(0.25)
        if message is not None:
            messages.append(message)
    if len(messages) != count:
        raise TimeoutError(f"expected {count} messages, received {len(messages)}")
    return messages


def committed_offset(settings: Settings, position: TopicPartition) -> int:
    observer = Consumer(
        {
            "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
            "group.id": settings.KAFKA_LOG_CONSUMER_GROUP,
            "enable.auto.commit": False,
        }
    )
    try:
        return observer.committed(
            [TopicPartition(position.topic, position.partition)], timeout=10
        )[0].offset
    finally:
        observer.close()


class UnusedDlq:
    def publish(self, _key, _message):
        raise AssertionError("valid records must not use DLQ")


def pipeline(settings, engine, consumer, *, repository=None, dlq=None):
    buffer = LogAggregationBuffer()
    offsets = ContiguousOffsetTracker()
    policy = RetryPolicy(max_retries=0)
    processor = LogMessageProcessor(
        buffer,
        offsets,
        dlq or UnusedDlq(),
        settings.KAFKA_LOG_CONSUMER_GROUP,
        policy,
        logger(),
    )
    flush = LogFlushService(
        buffer,
        offsets,
        create_session_factory(engine),
        repository or LogMetricRepository(),
        consumer,
        settings.KAFKA_LOG_CONSUMER_GROUP,
        policy,
        logger(),
    )
    return buffer, offsets, processor, flush


def accept(offsets, processor, message) -> None:
    offsets.observe(KafkaCoordinate(message.topic(), message.partition(), message.offset()))
    processor.process(message)


def metric_rows(engine) -> list[LogMetricMinute]:
    with create_session_factory(engine)() as session:
        return list(session.scalars(select(LogMetricMinute)).all())


def test_real_kafka_aggregation_upsert_late_event_and_duplicate(settings, engine) -> None:
    ensure_topics(settings)
    events = [
        event(status=200, response_ms=10),
        event(status=404, response_ms=20),
        event(status=500, response_ms=30),
        event(service="payment-api", endpoint="/payments", response_ms=7),
        event(minute=datetime(2026, 8, 5, 9, 55, 45, tzinfo=UTC), response_ms=5),
    ]
    positions = [produce(settings, item.model_dump_json().encode()) for item in events]
    consumer, _raw = assigned_consumer(settings, positions)
    _buffer, offsets, processor, flush = pipeline(settings, engine, consumer)
    try:
        for message in read_messages(consumer, len(events)):
            accept(offsets, processor, message)
        assert committed_offset(settings, positions[0]) == min(
            item.offset for item in positions if item.partition == positions[0].partition
        )
        flush.flush()
    finally:
        consumer.close()

    rows = metric_rows(engine)
    current = next(
        row
        for row in rows
        if row.service == "order-api" and row.metric_minute.hour == 10
    )
    assert (
        current.request_count,
        current.success_count,
        current.client_error_count,
        current.server_error_count,
    ) == (3, 1, 1, 1)
    assert current.response_time_sum_ms == 60
    assert current.max_response_time_ms == 30
    assert Decimal(current.response_time_sum_ms) / current.request_count == Decimal(20)
    assert len(rows) == 3

    duplicate_position = produce(settings, events[0].model_dump_json().encode())
    replay_consumer, _raw = assigned_consumer(settings, [duplicate_position])
    _buffer, replay_offsets, replay_processor, replay_flush = pipeline(
        settings, engine, replay_consumer
    )
    try:
        message = read_messages(replay_consumer, 1)[0]
        accept(replay_offsets, replay_processor, message)
        replay_flush.flush()
    finally:
        replay_consumer.close()
    replayed_current = next(
        row
        for row in metric_rows(engine)
        if row.service == "order-api" and row.metric_minute.hour == 10
    )
    assert replayed_current.request_count == 3


def tail_dlq(settings: Settings) -> Consumer:
    observer = Consumer(
        {
            "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
            "group.id": f"phase-four-dlq-{uuid4()}",
            "enable.auto.commit": False,
        }
    )
    metadata = observer.list_topics(settings.KAFKA_DLQ_TOPIC, timeout=10)
    assignments = []
    for partition_id in metadata.topics[settings.KAFKA_DLQ_TOPIC].partitions:
        partition = TopicPartition(settings.KAFKA_DLQ_TOPIC, partition_id)
        _low, high = observer.get_watermark_offsets(partition, timeout=10)
        assignments.append(TopicPartition(settings.KAFKA_DLQ_TOPIC, partition_id, high))
    observer.assign(assignments)
    return observer


def test_invalid_after_buffered_valid_does_not_jump_contiguous_offset(settings, engine) -> None:
    ensure_topics(settings)
    valid = event()
    valid_position = produce(settings, valid.model_dump_json().encode(), partition=0)
    invalid_value = json.loads(valid.model_dump_json())
    invalid_value["event_id"] = str(uuid4())
    invalid_value["payload"]["endpoint"] = "invalid"
    invalid_position = produce(settings, json.dumps(invalid_value).encode(), partition=0)
    observer = tail_dlq(settings)
    consumer, _raw = assigned_consumer(settings, [valid_position, invalid_position])
    dlq = DlqProducer(settings, delivery_timeout_seconds=1)
    _buffer, offsets, processor, flush = pipeline(settings, engine, consumer, dlq=dlq)
    try:
        valid_message, invalid_message = read_messages(consumer, 2)
        accept(offsets, processor, valid_message)
        accept(offsets, processor, invalid_message)
        flush.commit_ready()
        assert committed_offset(settings, invalid_position) == valid_position.offset

        dlq_body = None
        deadline = monotonic() + 10
        while dlq_body is None and monotonic() < deadline:
            message = observer.poll(0.25)
            if message is not None and message.error() is None:
                candidate = json.loads(message.value())
                if candidate["original_offset"] == invalid_position.offset:
                    dlq_body = candidate
        assert dlq_body is not None
        flush.flush()
        assert committed_offset(settings, invalid_position) == invalid_position.offset + 1
    finally:
        dlq.flush()
        observer.close()
        consumer.close()


class FailingRepository(LogMetricRepository):
    def persist_snapshot(self, session, snapshot, consumer_group):
        raise OperationalError("forced failure", {}, TimeoutError("offline"))


def test_database_failure_keeps_offset_and_buffer_then_recovers(settings, engine) -> None:
    ensure_topics(settings)
    position = produce(settings, event().model_dump_json().encode())
    consumer, _raw = assigned_consumer(settings, [position])
    buffer, offsets, processor, failing_flush = pipeline(
        settings, engine, consumer, repository=FailingRepository()
    )
    message = read_messages(consumer, 1)[0]
    accept(offsets, processor, message)
    with pytest.raises(RetriesExhaustedError):
        failing_flush.flush()
    assert len(buffer) == 1
    assert committed_offset(settings, position) == position.offset

    recovered = LogFlushService(
        buffer,
        offsets,
        create_session_factory(engine),
        LogMetricRepository(),
        consumer,
        settings.KAFKA_LOG_CONSUMER_GROUP,
        RetryPolicy(max_retries=0),
        logger(),
    )
    try:
        recovered.flush()
        assert committed_offset(settings, position) == position.offset + 1
        assert metric_rows(engine)[0].request_count == 1
    finally:
        consumer.close()


class CommitFailingConsumer:
    def __init__(self, wrapped):
        self.wrapped = wrapped

    def commit_offsets(self, _offsets):
        raise TimeoutError("forced commit timeout")


def test_commit_failure_replay_does_not_double_count(settings, engine) -> None:
    ensure_topics(settings)
    original = event()
    position = produce(settings, original.model_dump_json().encode())
    consumer, _raw = assigned_consumer(settings, [position])
    buffer = LogAggregationBuffer()
    offsets = ContiguousOffsetTracker()
    processor = LogMessageProcessor(
        buffer,
        offsets,
        UnusedDlq(),
        settings.KAFKA_LOG_CONSUMER_GROUP,
        RetryPolicy(max_retries=0),
        logger(),
    )
    failing_flush = LogFlushService(
        buffer,
        offsets,
        create_session_factory(engine),
        LogMetricRepository(),
        CommitFailingConsumer(consumer),
        settings.KAFKA_LOG_CONSUMER_GROUP,
        RetryPolicy(max_retries=0),
        logger(),
    )
    message = read_messages(consumer, 1)[0]
    accept(offsets, processor, message)
    with pytest.raises(RetriesExhaustedError):
        failing_flush.flush()
    consumer.close()
    assert metric_rows(engine)[0].request_count == 1
    assert committed_offset(settings, position) == position.offset

    replay_consumer, _raw = assigned_consumer(settings, [position])
    _buffer, replay_offsets, replay_processor, replay_flush = pipeline(
        settings, engine, replay_consumer
    )
    try:
        replay = read_messages(replay_consumer, 1)[0]
        accept(replay_offsets, replay_processor, replay)
        replay_flush.flush()
    finally:
        replay_consumer.close()
    assert metric_rows(engine)[0].request_count == 1
    assert committed_offset(settings, position) == position.offset + 1


def test_multiple_partitions_can_advance_independently(settings, engine) -> None:
    ensure_topics(settings)
    valid = event()
    valid_position = produce(settings, valid.model_dump_json().encode(), partition=0)
    invalid = json.loads(valid.model_dump_json())
    invalid["event_id"] = str(uuid4())
    invalid["payload"]["status_code"] = 700
    invalid_position = produce(settings, json.dumps(invalid).encode(), partition=1)
    consumer, _raw = assigned_consumer(settings, [valid_position, invalid_position])
    dlq = DlqProducer(settings, delivery_timeout_seconds=1)
    _buffer, offsets, processor, flush = pipeline(settings, engine, consumer, dlq=dlq)
    try:
        for message in read_messages(consumer, 2):
            accept(offsets, processor, message)
        flush.commit_ready()
        assert committed_offset(settings, valid_position) == valid_position.offset
        assert committed_offset(settings, invalid_position) == invalid_position.offset + 1
        flush.flush()
        assert committed_offset(settings, valid_position) == valid_position.offset + 1
    finally:
        dlq.flush()
        consumer.close()
