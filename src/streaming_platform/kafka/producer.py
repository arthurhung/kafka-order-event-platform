"""Tracked confluent-kafka producer operations for generated events."""

import logging
from collections.abc import Callable
from time import sleep
from typing import Protocol, cast

from confluent_kafka import KafkaError, Message, Producer

from streaming_platform.config import Settings
from streaming_platform.generator.factory import GeneratedRecord
from streaming_platform.generator.report import DeliveryTracker

QUEUE_FULL_BACKOFF_SECONDS = (1.0, 2.0, 4.0)


class ProducerClient(Protocol):
    """Subset of the confluent producer API used by the wrapper."""

    def produce(
        self,
        topic: str,
        value: bytes,
        key: bytes,
        on_delivery: Callable[[KafkaError | None, Message], None],
    ) -> None:
        """Queue one record for asynchronous delivery."""
        ...

    def poll(self, timeout: float) -> int:
        """Serve producer callbacks."""
        ...

    def flush(self, timeout: float) -> int:
        """Wait for outstanding deliveries and return the remaining count."""
        ...


class TrackedKafkaProducer:
    """Produce routed records and account for every delivery outcome."""

    def __init__(
        self,
        settings: Settings,
        tracker: DeliveryTracker,
        logger: logging.LoggerAdapter[logging.Logger],
        producer: ProducerClient | None = None,
        sleeper: Callable[[float], None] = sleep,
    ) -> None:
        """Initialize the confluent client or a compatible test double."""
        self._tracker = tracker
        self._logger = logger
        self._producer = producer or cast(
            ProducerClient,
            Producer({"bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS}),
        )
        self._sleeper = sleeper

    def send(self, record: GeneratedRecord) -> bool:
        """Queue one record, retrying local queue saturation with bounded backoff."""
        token = self._tracker.begin_attempt(record)

        def on_delivery(error: KafkaError | None, message: Message) -> None:
            if error is not None:
                self._tracker.failed(token)
                self._logger.error(
                    "event delivery failed",
                    extra={
                        "event_id": str(record.event_id),
                        "topic": record.topic,
                        "error": str(error),
                    },
                )
                return
            self._tracker.delivered(token)

        for attempt in range(len(QUEUE_FULL_BACKOFF_SECONDS) + 1):
            try:
                self._producer.produce(
                    record.topic,
                    value=record.value,
                    key=record.key,
                    on_delivery=on_delivery,
                )
                self._producer.poll(0.0)
                return True
            except BufferError:
                self._producer.poll(0.0)
                if attempt == len(QUEUE_FULL_BACKOFF_SECONDS):
                    self._tracker.failed(token)
                    self._logger.error(
                        "producer queue remained full",
                        extra={"event_id": str(record.event_id), "topic": record.topic},
                    )
                    return False
                self._sleeper(QUEUE_FULL_BACKOFF_SECONDS[attempt])
        return False

    def flush(self, timeout_seconds: float = 30.0) -> int:
        """Flush queued messages and mark any unconfirmed records as failed."""
        remaining = self._producer.flush(timeout_seconds)
        unconfirmed = self._tracker.fail_pending()
        if remaining or unconfirmed:
            self._logger.error(
                "producer flush left unconfirmed messages",
                extra={"remaining": remaining, "unconfirmed": unconfirmed},
            )
        return unconfirmed
