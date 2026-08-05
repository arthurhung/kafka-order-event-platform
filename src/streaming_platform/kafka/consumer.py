"""Manual-commit Kafka consumer wrappers shared by platform applications."""

from collections.abc import Callable
from typing import Protocol, cast

from confluent_kafka import Consumer, KafkaError, KafkaException, Message, TopicPartition

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

    def subscribe(  # noqa: D102
        self,
        topics: list[str],
        on_assign: Callable[[Consumer, list[TopicPartition]], None] | None = None,
        on_revoke: Callable[[Consumer, list[TopicPartition]], None] | None = None,
        on_lost: Callable[[Consumer, list[TopicPartition]], None] | None = None,
    ) -> None: ...

    def poll(self, timeout: float) -> Message | None: ...  # noqa: D102

    def commit(  # noqa: D102
        self,
        message: Message | None = None,
        offsets: list[TopicPartition] | None = None,
        asynchronous: bool = True,
    ) -> list[object] | None: ...

    def close(self) -> None: ...  # noqa: D102


class ManualKafkaConsumer:
    """Consume one configured topic with automatic commit and storage disabled."""

    def __init__(
        self,
        settings: Settings,
        *,
        topic: str,
        consumer_group: str,
        consumer: ConsumerClient | None = None,
        on_assign: Callable[[list[TopicPartition]], None] | None = None,
        on_revoke: Callable[[list[TopicPartition]], None] | None = None,
        on_lost: Callable[[list[TopicPartition]], None] | None = None,
    ) -> None:
        """Create a manual consumer and subscribe with optional rebalance hooks."""
        self._consumer = consumer or cast(
            ConsumerClient,
            Consumer(
                {
                    "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
                    "group.id": consumer_group,
                    "enable.auto.commit": False,
                    "enable.auto.offset.store": False,
                    "auto.offset.reset": "earliest",
                }
            ),
        )
        if on_assign is None and on_revoke is None and on_lost is None:
            self._consumer.subscribe([topic])
        else:
            self._consumer.subscribe(
                [topic],
                on_assign=self._wrap_rebalance_callback(on_assign),
                on_revoke=self._wrap_rebalance_callback(on_revoke),
                on_lost=self._wrap_rebalance_callback(on_lost),
            )

    @staticmethod
    def _wrap_rebalance_callback(
        callback: Callable[[list[TopicPartition]], None] | None,
    ) -> Callable[[Consumer, list[TopicPartition]], None]:
        if callback is None:
            return lambda _consumer, _partitions: None

        def wrapped(_consumer: Consumer, partitions: list[TopicPartition]) -> None:
            callback(partitions)

        return wrapped

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
        result = self._consumer.commit(
            message=cast(Message, message), offsets=None, asynchronous=False
        )
        self._raise_partition_errors(result)

    def commit_offsets(self, offsets: list[TopicPartition]) -> None:
        """Commit explicit next offsets synchronously and validate every result."""
        if not offsets:
            return
        result = self._consumer.commit(message=None, offsets=offsets, asynchronous=False)
        self._raise_partition_errors(result)

    @staticmethod
    def _raise_partition_errors(result: list[object] | None) -> None:
        for partition in result or []:
            error = getattr(partition, "error", None)
            if error is not None:
                raise KafkaException(error)

    def close(self) -> None:
        """Leave the group without an automatic offset commit."""
        self._consumer.close()


class OrderKafkaConsumer(ManualKafkaConsumer):
    """Consume the configured order topic with manual offset commits."""

    def __init__(self, settings: Settings, consumer: ConsumerClient | None = None) -> None:
        """Create and subscribe the configured order consumer."""
        super().__init__(
            settings,
            topic=settings.KAFKA_ORDER_TOPIC,
            consumer_group=settings.KAFKA_ORDER_CONSUMER_GROUP,
            consumer=consumer,
        )


class LogKafkaConsumer(ManualKafkaConsumer):
    """Consume the configured application-log topic with rebalance hooks."""

    def __init__(
        self,
        settings: Settings,
        consumer: ConsumerClient | None = None,
        *,
        on_assign: Callable[[list[TopicPartition]], None] | None = None,
        on_revoke: Callable[[list[TopicPartition]], None] | None = None,
        on_lost: Callable[[list[TopicPartition]], None] | None = None,
    ) -> None:
        """Create and subscribe the configured log consumer."""
        super().__init__(
            settings,
            topic=settings.KAFKA_LOG_TOPIC,
            consumer_group=settings.KAFKA_LOG_CONSUMER_GROUP,
            consumer=consumer,
            on_assign=on_assign,
            on_revoke=on_revoke,
            on_lost=on_lost,
        )


def is_retryable_kafka_error(error: Exception) -> bool:
    """Return whether a Kafka operation failed for a temporary transport reason."""
    if isinstance(error, (BufferError, TimeoutError)):
        return True
    if not isinstance(error, KafkaException) or not error.args:
        return False
    kafka_error = error.args[0]
    return isinstance(kafka_error, KafkaError) and kafka_error.retriable()
