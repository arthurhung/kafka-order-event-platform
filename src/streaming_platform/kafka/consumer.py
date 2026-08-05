"""Manual-commit Kafka consumer wrapper for order processing."""

from typing import Protocol, cast

from confluent_kafka import Consumer, KafkaError, KafkaException, Message

from streaming_platform.config import Settings


class KafkaMessage(Protocol):
    """Message fields required by the order processing service."""

    def topic(self) -> str: ...  # noqa: D102

    def partition(self) -> int: ...  # noqa: D102

    def offset(self) -> int: ...  # noqa: D102

    def key(self) -> bytes | None: ...  # noqa: D102

    def value(self) -> bytes | None: ...  # noqa: D102


class ConsumerClient(Protocol):
    """Subset of confluent Consumer used by the application."""

    def subscribe(self, topics: list[str]) -> None: ...  # noqa: D102

    def poll(self, timeout: float) -> Message | None: ...  # noqa: D102

    def commit(  # noqa: D102
        self, message: Message, asynchronous: bool
    ) -> list[object] | None: ...

    def close(self) -> None: ...  # noqa: D102


class OrderKafkaConsumer:
    """Consume orders with automatic commits and offset storage disabled."""

    def __init__(self, settings: Settings, consumer: ConsumerClient | None = None) -> None:
        """Create and subscribe the configured order consumer."""
        self._consumer = consumer or cast(
            ConsumerClient,
            Consumer(
                {
                    "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
                    "group.id": settings.KAFKA_ORDER_CONSUMER_GROUP,
                    "enable.auto.commit": False,
                    "enable.auto.offset.store": False,
                    "auto.offset.reset": "earliest",
                }
            ),
        )
        self._consumer.subscribe([settings.KAFKA_ORDER_TOPIC])

    def poll(self, timeout_seconds: float) -> KafkaMessage | None:
        """Poll one usable record or raise for broker-level errors."""
        message = self._consumer.poll(timeout_seconds)
        if message is None:
            return None
        error = message.error()
        if error is None:
            return cast(KafkaMessage, message)
        if error.code() == KafkaError._PARTITION_EOF:
            return None
        raise KafkaException(error)

    def commit(self, message: KafkaMessage) -> None:
        """Commit the offset synchronously immediately after one message."""
        result = self._consumer.commit(message=cast(Message, message), asynchronous=False)
        for partition in result or []:
            error = getattr(partition, "error", None)
            if error is not None:
                raise KafkaException(error)

    def close(self) -> None:
        """Leave the group without an automatic offset commit."""
        self._consumer.close()


def is_retryable_kafka_error(error: Exception) -> bool:
    """Return whether a Kafka operation failed for a temporary transport reason."""
    if isinstance(error, (BufferError, TimeoutError)):
        return True
    if not isinstance(error, KafkaException) or not error.args:
        return False
    kafka_error = error.args[0]
    return isinstance(kafka_error, KafkaError) and kafka_error.retriable()
