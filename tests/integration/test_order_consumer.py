"""Real Kafka and PostgreSQL integration tests for the Phase 3 order consumer."""

import json
import logging
from collections.abc import Iterator
from time import monotonic

import pytest
from confluent_kafka import Consumer, Message, Producer, TopicPartition
from sqlalchemy import delete, func, select
from sqlalchemy.exc import OperationalError

from streaming_platform.config import Settings
from streaming_platform.consumer.order import OrderProcessor, ProcessingResult
from streaming_platform.consumer.retry import RetriesExhaustedError, RetryPolicy
from streaming_platform.database.models import ProcessedEvent, ValidOrder
from streaming_platform.database.order_repository import OrderRepository
from streaming_platform.database.session import create_database_engine, create_session_factory
from streaming_platform.generator.factory import EventFactory, InvalidKind
from streaming_platform.kafka.admin import ensure_topics
from streaming_platform.kafka.dlq import DlqProducer
from streaming_platform.models import EventType
from tests.integration.kafka_helpers import ManuallyAssignedConsumer

pytestmark = pytest.mark.integration


class UnusedDlqProducer:
    def publish(self, _key, _message):
        raise AssertionError("valid events must not use the DLQ")


class FailAfterBusinessInsertRepository(OrderRepository):
    def persist(self, session, event, metadata, consumer_group):
        super().persist(session, event, metadata, consumer_group)
        raise OperationalError("forced transaction failure", {}, TimeoutError("offline"))


@pytest.fixture
def engine(settings: Settings) -> Iterator:
    database_engine = create_database_engine(settings)
    with database_engine.begin() as connection:
        connection.execute(delete(ValidOrder))
        connection.execute(delete(ProcessedEvent))
    yield database_engine
    database_engine.dispose()


def logger() -> logging.LoggerAdapter[logging.Logger]:
    return logging.LoggerAdapter(logging.getLogger("phase-three-integration"), {})


def produce(settings: Settings, key: bytes, value: bytes) -> TopicPartition:
    delivered: list[TopicPartition] = []
    errors: list[str] = []

    def callback(error, message: Message) -> None:
        if error is not None:
            errors.append(str(error))
        else:
            delivered.append(TopicPartition(message.topic(), message.partition(), message.offset()))

    producer = Producer({"bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS})
    producer.produce(settings.KAFKA_ORDER_TOPIC, key=key, value=value, on_delivery=callback)
    remaining = producer.flush(10)
    if remaining or errors or not delivered:
        raise RuntimeError(errors[0] if errors else "order test message was not delivered")
    return delivered[0]


def set_group_offset(settings: Settings, position: TopicPartition) -> None:
    consumer = Consumer(
        {
            "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
            "group.id": settings.KAFKA_ORDER_CONSUMER_GROUP,
            "enable.auto.commit": False,
        }
    )
    try:
        consumer.assign([position])
        consumer.commit(offsets=[position], asynchronous=False)
    finally:
        consumer.close()


def committed_offset(settings: Settings, position: TopicPartition) -> int:
    consumer = Consumer(
        {
            "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
            "group.id": settings.KAFKA_ORDER_CONSUMER_GROUP,
            "enable.auto.commit": False,
        }
    )
    try:
        return consumer.committed(
            [TopicPartition(position.topic, position.partition)], timeout=10
        )[0].offset
    finally:
        consumer.close()


def assigned_consumer(settings: Settings, position: TopicPartition) -> ManuallyAssignedConsumer:
    client = Consumer(
        {
            "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
            "group.id": settings.KAFKA_ORDER_CONSUMER_GROUP,
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
        }
    )
    client.assign([position])
    return ManuallyAssignedConsumer(client)


def next_message(consumer: ManuallyAssignedConsumer):
    deadline = monotonic() + 10
    while monotonic() < deadline:
        message = consumer.poll(0.25)
        if message is not None:
            return message
    raise TimeoutError("timed out waiting for assigned order message")


def processor(settings, engine, repository=None, dlq=None, retries=0) -> OrderProcessor:
    return OrderProcessor(
        create_session_factory(engine),
        repository or OrderRepository(),
        dlq or UnusedDlqProducer(),
        settings.KAFKA_ORDER_CONSUMER_GROUP,
        RetryPolicy(max_retries=retries, base_seconds=0.001, max_seconds=0.001),
        logger(),
    )


def counts(engine) -> tuple[int, int]:
    with create_session_factory(engine)() as session:
        return (
            session.scalar(select(func.count()).select_from(ProcessedEvent)) or 0,
            session.scalar(select(func.count()).select_from(ValidOrder)) or 0,
        )


def test_valid_event_and_duplicate_are_transactional_and_idempotent(settings, engine) -> None:
    ensure_topics(settings)
    record = EventFactory(settings, seed=301).create_normal(EventType.ORDER_CREATED)
    first = produce(settings, record.key, record.value)
    duplicate = produce(settings, record.key, record.value)
    assert first.partition == duplicate.partition
    set_group_offset(settings, first)
    consumer = assigned_consumer(settings, first)
    order_processor = processor(settings, engine)
    try:
        first_message = next_message(consumer)
        assert order_processor.process(first_message) is ProcessingResult.PROCESSED
        consumer.commit(first_message)
        duplicate_message = next_message(consumer)
        assert order_processor.process(duplicate_message) is ProcessingResult.DUPLICATE
        consumer.commit(duplicate_message)
    finally:
        consumer.close()

    assert counts(engine) == (1, 1)
    assert committed_offset(settings, duplicate) == duplicate.offset + 1


def test_failed_transaction_does_not_persist_or_advance_and_can_recover(settings, engine) -> None:
    ensure_topics(settings)
    record = EventFactory(settings, seed=302).create_normal(EventType.ORDER_PAID)
    position = produce(settings, record.key, record.value)
    set_group_offset(settings, position)
    consumer = assigned_consumer(settings, position)
    message = next_message(consumer)
    try:
        with pytest.raises(RetriesExhaustedError):
            processor(
                settings, engine, repository=FailAfterBusinessInsertRepository()
            ).process(message)
    finally:
        consumer.close()

    assert counts(engine) == (0, 0)
    assert committed_offset(settings, position) == position.offset

    recovered_consumer = assigned_consumer(settings, position)
    try:
        replay = next_message(recovered_consumer)
        assert processor(settings, engine).process(replay) is ProcessingResult.PROCESSED
        recovered_consumer.commit(replay)
    finally:
        recovered_consumer.close()

    assert counts(engine) == (1, 1)
    assert committed_offset(settings, position) == position.offset + 1


def test_invalid_event_reaches_real_dlq_before_source_offset_advances(settings, engine) -> None:
    ensure_topics(settings)
    dlq_tail = Consumer(
        {
            "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
            "group.id": "phase-three-dlq-observer",
            "enable.auto.commit": False,
        }
    )
    metadata = dlq_tail.list_topics(settings.KAFKA_DLQ_TOPIC, timeout=10)
    assignments = []
    for partition_id in metadata.topics[settings.KAFKA_DLQ_TOPIC].partitions:
        partition = TopicPartition(settings.KAFKA_DLQ_TOPIC, partition_id)
        _low, high = dlq_tail.get_watermark_offsets(partition, timeout=10)
        assignments.append(TopicPartition(settings.KAFKA_DLQ_TOPIC, partition_id, high))
    dlq_tail.assign(assignments)

    record = EventFactory(settings, seed=303).create_invalid(
        EventType.ORDER_CREATED, InvalidKind.NEGATIVE_AMOUNT
    )
    event_id = json.loads(record.value)["event_id"]
    position = produce(settings, record.key, record.value)
    set_group_offset(settings, position)
    consumer = assigned_consumer(settings, position)
    dlq = DlqProducer(settings)
    try:
        message = next_message(consumer)
        assert committed_offset(settings, position) == position.offset
        assert processor(settings, engine, dlq=dlq).process(message) is ProcessingResult.DLQED
        assert committed_offset(settings, position) == position.offset
        consumer.commit(message)
        deadline = monotonic() + 10
        dlq_message = None
        while dlq_message is None and monotonic() < deadline:
            candidate = dlq_tail.poll(0.25)
            if candidate is not None and candidate.error() is None:
                body = json.loads(candidate.value())
                payload = body.get("original_payload")
                if (
                    body.get("original_topic") == position.topic
                    and body.get("original_partition") == position.partition
                    and body.get("original_offset") == position.offset
                    and isinstance(payload, dict)
                    and payload.get("event_id") == event_id
                ):
                    dlq_message = candidate
    finally:
        dlq.flush()
        consumer.close()
        dlq_tail.close()

    assert dlq_message is not None
    body = json.loads(dlq_message.value())
    assert body["original_topic"] == settings.KAFKA_ORDER_TOPIC
    assert body["original_offset"] == position.offset
    assert body["error_type"] == "ValidationError"
    assert committed_offset(settings, position) == position.offset + 1
    assert counts(engine) == (0, 0)
