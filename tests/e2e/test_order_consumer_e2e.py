"""Executable-level Phase 3 flow using real Kafka and PostgreSQL services."""

import json
import logging
import os
import signal
import subprocess
import sys
from collections.abc import Callable
from time import monotonic, sleep

import pytest
from confluent_kafka import Consumer, TopicPartition
from sqlalchemy import delete, func, select

from streaming_platform.config import Settings, get_settings
from streaming_platform.consumer.order import OrderProcessor, ProcessingResult
from streaming_platform.consumer.retry import RetryPolicy
from streaming_platform.database.models import ProcessedEvent, ValidOrder
from streaming_platform.database.order_repository import OrderRepository
from streaming_platform.database.session import create_database_engine, create_session_factory
from streaming_platform.generator.factory import EventFactory, InvalidKind
from streaming_platform.kafka.admin import ensure_topics
from streaming_platform.kafka.consumer import OrderKafkaConsumer
from streaming_platform.kafka.dlq import DlqProducer
from streaming_platform.models import EventType
from tests.integration.test_order_consumer import produce

pytestmark = pytest.mark.e2e


def wait_until(predicate: Callable[[], bool], description: str, timeout: float = 20) -> None:
    """Poll a condition with a hard deadline."""
    deadline = monotonic() + timeout
    while monotonic() < deadline:
        if predicate():
            return
        sleep(0.2)
    raise TimeoutError(f"timed out waiting for {description}")


def reset_group_to_tail(settings: Settings) -> None:
    """Make the fixed Phase 3 group start after pre-existing test messages."""
    consumer = Consumer(
        {
            "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
            "group.id": settings.KAFKA_ORDER_CONSUMER_GROUP,
            "enable.auto.commit": False,
        }
    )
    try:
        metadata = consumer.list_topics(settings.KAFKA_ORDER_TOPIC, timeout=10)
        positions = []
        for partition_id in metadata.topics[settings.KAFKA_ORDER_TOPIC].partitions:
            partition = TopicPartition(settings.KAFKA_ORDER_TOPIC, partition_id)
            _low, high = consumer.get_watermark_offsets(partition, timeout=10)
            positions.append(TopicPartition(settings.KAFKA_ORDER_TOPIC, partition_id, high))
        consumer.assign(positions)
        consumer.commit(offsets=positions, asynchronous=False)
    finally:
        consumer.close()


def tail_dlq(settings: Settings) -> Consumer:
    """Assign a test observer to the current end of every DLQ partition."""
    consumer = Consumer(
        {
            "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
            "group.id": "phase-three-e2e-dlq-observer",
            "enable.auto.commit": False,
        }
    )
    metadata = consumer.list_topics(settings.KAFKA_DLQ_TOPIC, timeout=10)
    positions = []
    for partition_id in metadata.topics[settings.KAFKA_DLQ_TOPIC].partitions:
        partition = TopicPartition(settings.KAFKA_DLQ_TOPIC, partition_id)
        _low, high = consumer.get_watermark_offsets(partition, timeout=10)
        positions.append(TopicPartition(settings.KAFKA_DLQ_TOPIC, partition_id, high))
    consumer.assign(positions)
    return consumer


def start_consumer() -> subprocess.Popen[str]:
    """Start the real order consumer application as a child process."""
    return subprocess.Popen(
        [sys.executable, "-m", "apps.order_consumer"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        env=os.environ.copy(),
    )


def stop_consumer(process: subprocess.Popen[str]) -> str:
    """Request graceful SIGTERM shutdown and return captured structured logs."""
    process.send_signal(signal.SIGTERM)
    output, _ = process.communicate(timeout=15)
    assert process.returncode == 143, output
    return output


def table_counts(engine) -> tuple[int, int]:
    """Return current idempotency and business row counts."""
    with create_session_factory(engine)() as session:
        return (
            session.scalar(select(func.count()).select_from(ProcessedEvent)) or 0,
            session.scalar(select(func.count()).select_from(ValidOrder)) or 0,
        )


def committed_offset(settings: Settings, position: TopicPartition) -> int:
    """Read the fixed consumer group's committed position for one partition."""
    observer = Consumer(
        {
            "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
            "group.id": settings.KAFKA_ORDER_CONSUMER_GROUP,
            "enable.auto.commit": False,
        }
    )
    try:
        return observer.committed([TopicPartition(position.topic, position.partition)], timeout=10)[
            0
        ].offset
    finally:
        observer.close()


def test_order_consumer_end_to_end_and_uncommitted_restart() -> None:
    subprocess.run(
        ["docker", "compose", "up", "-d", "kafka", "postgres", "kafka-ui"],  # noqa: S607
        check=True,
        timeout=120,
    )
    subprocess.run([sys.executable, "scripts/wait_for_services.py"], check=True, timeout=120)
    subprocess.run([sys.executable, "-m", "alembic", "upgrade", "head"], check=True, timeout=60)
    get_settings.cache_clear()
    settings = get_settings()
    ensure_topics(settings)
    reset_group_to_tail(settings)
    engine = create_database_engine(settings)
    with engine.begin() as connection:
        connection.execute(delete(ValidOrder))
        connection.execute(delete(ProcessedEvent))

    dlq_observer = tail_dlq(settings)
    factory = EventFactory(settings, seed=401)
    normal = factory.create_normal(EventType.ORDER_CREATED)
    invalid = factory.create_invalid(EventType.ORDER_PAID, InvalidKind.NEGATIVE_AMOUNT)
    process = start_consumer()
    try:
        sleep(1)
        produce(settings, normal.key, normal.value)
        produce(settings, normal.key, normal.value)
        invalid_position = produce(settings, invalid.key, invalid.value)
        wait_until(lambda: table_counts(engine) == (1, 1), "normal and duplicate DB rows")

        dlq_body = None
        deadline = monotonic() + 15
        while dlq_body is None and monotonic() < deadline:
            message = dlq_observer.poll(0.25)
            if message is not None and message.error() is None:
                candidate = json.loads(message.value())
                if candidate["original_offset"] == invalid_position.offset:
                    dlq_body = candidate
        assert dlq_body is not None
        assert dlq_body["error_type"] == "ValidationError"
        wait_until(
            lambda: committed_offset(settings, invalid_position) == invalid_position.offset + 1,
            "invalid source offset commit",
        )
    finally:
        if process.poll() is None:
            stop_consumer(process)
        dlq_observer.close()

    pending = factory.create_normal(EventType.PAYMENT_FAILED)
    pending_position = produce(settings, pending.key, pending.value)
    client = Consumer(
        {
            "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
            "group.id": settings.KAFKA_ORDER_CONSUMER_GROUP,
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
        }
    )
    direct_consumer = OrderKafkaConsumer(settings, consumer=client)
    client.assign([pending_position])
    message = direct_consumer.poll(10)
    assert message is not None
    direct_processor = OrderProcessor(
        create_session_factory(engine),
        OrderRepository(),
        DlqProducer(settings),
        settings.KAFKA_ORDER_CONSUMER_GROUP,
        RetryPolicy(max_retries=0),
        logging.LoggerAdapter(logging.getLogger("phase-three-e2e-crash-window"), {}),
    )
    assert direct_processor.process(message) is ProcessingResult.PROCESSED
    direct_consumer.close()  # Intentionally omit Kafka commit to simulate the crash window.
    assert table_counts(engine) == (2, 2)
    assert committed_offset(settings, pending_position) == pending_position.offset

    restarted = start_consumer()
    try:
        wait_until(
            lambda: committed_offset(settings, pending_position) == pending_position.offset + 1,
            "replayed event offset after restart",
        )
        assert table_counts(engine) == (2, 2)
    finally:
        if restarted.poll() is None:
            stop_consumer(restarted)
        engine.dispose()
