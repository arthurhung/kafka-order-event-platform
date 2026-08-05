"""Real-Kafka integration coverage for Phase 5 lag inspection."""

from time import monotonic
from uuid import uuid4

import pytest
from confluent_kafka import Consumer, Message, Producer, TopicPartition

from streaming_platform.config import Settings
from streaming_platform.kafka.admin import ensure_topics
from streaming_platform.kafka.lag import LagInspector

pytestmark = pytest.mark.integration


def establish_group_at_tail(settings: Settings, group: str, topic: str) -> None:
    consumer = Consumer(
        {
            "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
            "group.id": group,
            "enable.auto.commit": False,
        }
    )
    try:
        metadata = consumer.list_topics(topic, timeout=10)
        positions = []
        for partition in metadata.topics[topic].partitions:
            item = TopicPartition(topic, partition)
            _low, high = consumer.get_watermark_offsets(item, timeout=10)
            positions.append(TopicPartition(topic, partition, high))
        consumer.assign(positions)
        consumer.commit(offsets=positions, asynchronous=False)
    finally:
        consumer.close()


def produce_one(settings: Settings) -> TopicPartition:
    positions: list[TopicPartition] = []

    def delivered(error, message: Message) -> None:
        if error is not None:
            raise RuntimeError(str(error))
        positions.append(TopicPartition(message.topic(), message.partition(), message.offset()))

    producer = Producer({"bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS})
    producer.produce(
        settings.KAFKA_ORDER_TOPIC,
        key=f"phase-five-lag-{uuid4()}".encode(),
        value=b"{}",
        on_delivery=delivered,
    )
    assert producer.flush(10) == 0
    return positions[0]


def test_real_offsets_show_lag_rise_and_commit_recovery(settings: Settings) -> None:
    ensure_topics(settings)
    order_group = f"phase-five-order-lag-{uuid4()}"
    log_group = f"phase-five-log-lag-{uuid4()}"
    isolated = settings.model_copy(
        update={
            "KAFKA_ORDER_CONSUMER_GROUP": order_group,
            "KAFKA_LOG_CONSUMER_GROUP": log_group,
        }
    )
    establish_group_at_tail(isolated, order_group, isolated.KAFKA_ORDER_TOPIC)
    establish_group_at_tail(isolated, log_group, isolated.KAFKA_LOG_TOPIC)
    assert LagInspector(isolated).inspect().combined_total_lag == 0

    position = produce_one(isolated)
    raised = LagInspector(isolated).inspect()
    assert raised.combined_total_lag == 1

    consumer = Consumer(
        {
            "bootstrap.servers": isolated.KAFKA_BOOTSTRAP_SERVERS,
            "group.id": order_group,
            "enable.auto.commit": False,
        }
    )
    consumer.assign([position])
    deadline = monotonic() + 10
    try:
        while monotonic() < deadline:
            message = consumer.poll(0.25)
            if message is not None and message.error() is None:
                consumer.commit(message=message, asynchronous=False)
                break
        else:
            raise TimeoutError("temporary lag consumer did not receive its source record")
    finally:
        consumer.close()

    recovered = LagInspector(isolated).inspect()
    assert recovered.combined_total_lag == 0
    assert all(
        row.lag is None or row.lag >= 0
        for group in recovered.groups
        for row in group.rows
    )
