"""Kafka readiness helpers shared by real-service tests."""

import os
import subprocess
import sys
from time import monotonic, sleep
from uuid import uuid4

from confluent_kafka import Consumer, KafkaError, KafkaException, Message, TopicPartition

from streaming_platform.config import Settings
from streaming_platform.kafka.admin import ensure_topics


def ensure_test_infrastructure() -> None:
    """Start managed test services unless CI has already provisioned them."""
    infrastructure_mode = os.environ.get("BENCHMARK_INFRASTRUCTURE_MODE", "managed")
    if infrastructure_mode == "preprovisioned":
        return
    if infrastructure_mode != "managed":
        raise ValueError(
            "BENCHMARK_INFRASTRUCTURE_MODE must be 'managed' or 'preprovisioned'"
        )
    subprocess.run(
        ["docker", "compose", "up", "-d", "kafka", "postgres", "kafka-ui"],  # noqa: S607
        check=True,
        timeout=120,
    )
    subprocess.run([sys.executable, "scripts/wait_for_services.py"], check=True, timeout=120)
    subprocess.run([sys.executable, "-m", "alembic", "upgrade", "head"], check=True, timeout=60)


class ManuallyAssignedConsumer:
    """Small real-Kafka adapter that never joins a subscribed consumer group."""

    def __init__(self, consumer: Consumer) -> None:
        self._consumer = consumer

    def poll(self, timeout: float) -> Message | None:
        message = self._consumer.poll(timeout)
        if message is None:
            return None
        error = message.error()
        if error is None:
            return message
        if error.code() == KafkaError._PARTITION_EOF:
            return None
        raise KafkaException(error)

    def commit(self, message: Message) -> None:
        result = self._consumer.commit(message=message, asynchronous=False)
        self._raise_partition_errors(result)

    def commit_offsets(self, offsets: list[TopicPartition]) -> None:
        result = self._consumer.commit(offsets=offsets, asynchronous=False)
        self._raise_partition_errors(result)

    @staticmethod
    def _raise_partition_errors(result: list[TopicPartition] | None) -> None:
        for partition in result or []:
            if partition.error is not None:
                raise KafkaException(partition.error)

    def close(self) -> None:
        self._consumer.close()


def ensure_test_topics_ready(settings: Settings, timeout: float = 15) -> None:
    """Create configured topics and wait until every partition has a leader."""
    ensure_topics(settings)
    observer = Consumer(
        {
            "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
            "group.id": f"topic-readiness-{uuid4()}",
            "enable.auto.commit": False,
        }
    )
    topics = (
        settings.KAFKA_ORDER_TOPIC,
        settings.KAFKA_LOG_TOPIC,
        settings.KAFKA_DLQ_TOPIC,
    )
    deadline = monotonic() + timeout
    last_error: Exception | None = None
    try:
        while monotonic() < deadline:
            try:
                metadata = observer.list_topics(timeout=1)
                for topic in topics:
                    topic_metadata = metadata.topics[topic]
                    if topic_metadata.error is not None:
                        raise KafkaException(topic_metadata.error)
                    for partition_id in topic_metadata.partitions:
                        observer.get_watermark_offsets(
                            TopicPartition(topic, partition_id), timeout=1
                        )
                return
            except (KafkaException, KeyError) as error:
                last_error = error
                sleep(0.1)
    finally:
        observer.close()
    raise TimeoutError("timed out waiting for test topic partition leaders") from last_error
