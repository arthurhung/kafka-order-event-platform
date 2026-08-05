"""Synchronous, delivery-confirmed producer for order dead-letter records."""

from collections.abc import Callable
from time import monotonic
from typing import Protocol, cast

from confluent_kafka import KafkaError, KafkaException, Message, Producer

from streaming_platform.config import Settings
from streaming_platform.models.dlq import DlqMessage


class ProducerClient(Protocol):
    """Subset of the confluent producer API needed by the DLQ wrapper."""

    def produce(  # noqa: D102
        self,
        topic: str,
        value: bytes,
        key: bytes,
        on_delivery: Callable[[KafkaError | None, Message], None],
    ) -> None: ...

    def poll(self, timeout: float) -> int: ...  # noqa: D102

    def flush(self, timeout: float) -> int: ...  # noqa: D102


class DlqProducer:
    """Produce one DLQ message and return only after broker acknowledgement."""

    def __init__(
        self,
        settings: Settings,
        producer: ProducerClient | None = None,
        clock: Callable[[], float] = monotonic,
    ) -> None:
        """Create a producer whose acknowledgements are local-cluster durable only."""
        self._topic = settings.KAFKA_DLQ_TOPIC
        self._delivery_timeout = settings.ORDER_CONSUMER_DELIVERY_TIMEOUT_SECONDS
        self._producer = producer or cast(
            ProducerClient,
            Producer(
                {
                    "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
                    "acks": "all",
                }
            ),
        )
        self._clock = clock

    def publish(self, key: str, message: DlqMessage) -> None:
        """Block until the delivery callback confirms success or reports failure."""
        delivered = False
        delivery_error: KafkaError | None = None

        def on_delivery(error: KafkaError | None, _message: Message) -> None:
            nonlocal delivered, delivery_error
            delivered = error is None
            delivery_error = error

        self._producer.produce(
            self._topic,
            key=key.encode("utf-8"),
            value=message.encoded(),
            on_delivery=on_delivery,
        )
        deadline = self._clock() + self._delivery_timeout
        while not delivered and delivery_error is None:
            remaining = deadline - self._clock()
            if remaining <= 0:
                raise TimeoutError("timed out waiting for DLQ delivery acknowledgement")
            self._producer.poll(min(remaining, 0.25))
        if delivery_error is not None:
            raise KafkaException(delivery_error)

    def flush(self) -> int:
        """Wait briefly for any callback still pending during shutdown."""
        return self._producer.flush(self._delivery_timeout)
