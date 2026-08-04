"""Verify an actual Kafka produce/consume round trip, including the record key."""

import json
from dataclasses import dataclass
from time import monotonic
from uuid import uuid4

from confluent_kafka import Consumer, KafkaError, Message, Producer, TopicPartition

from streaming_platform.config import Settings, get_settings
from streaming_platform.logging import configure_logging


@dataclass(frozen=True, slots=True)
class ProducedRecord:
    """Broker coordinates returned after a successful produce."""

    topic: str
    partition: int
    offset: int


def produce_smoke_record(settings: Settings, key: bytes, value: bytes) -> ProducedRecord:
    """Produce one record and return its acknowledged broker coordinates."""
    result: list[ProducedRecord] = []
    errors: list[str] = []

    def on_delivery(error: KafkaError | None, message: Message) -> None:
        if error is not None:
            errors.append(str(error))
            return
        result.append(ProducedRecord(message.topic(), message.partition(), message.offset()))

    producer = Producer({"bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS})
    producer.produce(settings.KAFKA_ORDER_TOPIC, key=key, value=value, on_delivery=on_delivery)
    remaining = producer.flush(10.0)
    if remaining or errors or not result:
        detail = errors[0] if errors else f"{remaining} message(s) not delivered"
        raise RuntimeError(f"Kafka smoke produce failed: {detail}")
    return result[0]


def consume_exact_record(
    settings: Settings,
    produced: ProducedRecord,
    expected_key: bytes,
    expected_value: bytes,
    timeout_seconds: float = 10.0,
) -> None:
    """Read the exact produced offset and verify its key and value."""
    consumer = Consumer(
        {
            "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
            "group.id": f"phase-one-smoke-{uuid4()}",
            "enable.auto.commit": False,
        }
    )
    consumer.assign([TopicPartition(produced.topic, produced.partition, produced.offset)])
    deadline = monotonic() + timeout_seconds
    try:
        while monotonic() < deadline:
            message = consumer.poll(0.5)
            if message is None:
                continue
            if message.error() is not None:
                raise RuntimeError(f"Kafka smoke consume failed: {message.error()}")
            if message.offset() != produced.offset:
                continue
            if message.key() != expected_key or message.value() != expected_value:
                raise RuntimeError("Kafka smoke record key or value did not round-trip")
            return
    finally:
        consumer.close()
    raise TimeoutError("Timed out waiting for the Kafka smoke record")


def main() -> None:
    """Run the Phase 1 Kafka smoke check."""
    settings = get_settings()
    logger = configure_logging(settings, "phase-one-kafka-smoke")
    marker = str(uuid4())
    key = f"SMOKE-{marker}".encode()
    value = json.dumps({"phase": 1, "marker": marker}, separators=(",", ":")).encode()
    produced = produce_smoke_record(settings, key, value)
    consume_exact_record(settings, produced, key, value)
    logger.info(
        "Kafka produce/consume smoke test passed",
        extra={
            "topic": produced.topic,
            "partition": produced.partition,
            "offset": produced.offset,
            "key": key.decode(),
        },
    )


if __name__ == "__main__":
    main()
