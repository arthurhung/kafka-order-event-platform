"""Order decoding, transactional processing, DLQ routing, and polling orchestration."""

import logging
from collections.abc import Callable
from datetime import UTC, datetime
from enum import StrEnum
from functools import partial
from time import monotonic, sleep

from pydantic import ValidationError
from sqlalchemy.orm import Session, sessionmaker

from streaming_platform.consumer.errors import (
    PermanentMessageError,
    build_dlq_message,
    decode_json_value,
    is_retryable_database_error,
    safe_validation_message,
)
from streaming_platform.consumer.retry import RetryPolicy, run_with_retry
from streaming_platform.database.order_repository import KafkaRecordMetadata, OrderRepository
from streaming_platform.kafka.consumer import (
    KafkaMessage,
    OrderKafkaConsumer,
    is_retryable_kafka_error,
)
from streaming_platform.kafka.dlq import DlqProducer
from streaming_platform.models import ORDER_EVENT_ADAPTER, OrderEvent


class ProcessingResult(StrEnum):
    """Successful downstream outcomes that permit an offset commit."""

    PROCESSED = "processed"
    DUPLICATE = "duplicate"
    DLQED = "dlqed"


def decode_order_event(raw_value: bytes) -> OrderEvent:
    """Decode UTF-8 JSON and validate it as one of the four order event types."""
    decoded = decode_json_value(raw_value)
    try:
        return ORDER_EVENT_ADAPTER.validate_python(decoded)
    except ValidationError as error:
        safe_message = safe_validation_message(error, "order event validation failed")
        raise PermanentMessageError("ValidationError", safe_message, decoded) from error


class OrderProcessor:
    """Route permanent errors to DLQ and valid events through one DB transaction."""

    def __init__(
        self,
        session_factory: sessionmaker[Session],
        repository: OrderRepository,
        dlq_producer: DlqProducer,
        consumer_group: str,
        retry_policy: RetryPolicy,
        logger: logging.LoggerAdapter[logging.Logger],
        sleeper: Callable[[float], None] = sleep,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        """Store processing dependencies and injectable time functions."""
        self._session_factory = session_factory
        self._repository = repository
        self._dlq_producer = dlq_producer
        self._consumer_group = consumer_group
        self._retry_policy = retry_policy
        self._logger = logger
        self._sleeper = sleeper
        self._clock = clock or (lambda: datetime.now(UTC))

    def process(self, message: KafkaMessage) -> ProcessingResult:
        """Complete DB or DLQ downstream work without committing Kafka offsets."""
        try:
            event = decode_order_event(message.value() or b"")
        except PermanentMessageError as error:
            key, dlq_message = build_dlq_message(
                message, error, self._consumer_group, self._clock()
            )
            run_with_retry(
                lambda: self._dlq_producer.publish(key, dlq_message),
                operation_name="DLQ delivery",
                policy=self._retry_policy,
                is_retryable=is_retryable_kafka_error,
                sleeper=self._sleeper,
                on_retry=self._log_retry,
            )
            return ProcessingResult.DLQED

        metadata = KafkaRecordMetadata(message.topic(), message.partition(), message.offset())
        inserted = run_with_retry(
            lambda: self._persist_once(event, metadata),
            operation_name="order database transaction",
            policy=self._retry_policy,
            is_retryable=is_retryable_database_error,
            sleeper=self._sleeper,
            on_retry=self._log_retry,
        )
        return ProcessingResult.PROCESSED if inserted else ProcessingResult.DUPLICATE

    def _persist_once(self, event: OrderEvent, metadata: KafkaRecordMetadata) -> bool:
        with self._session_factory() as session, session.begin():
            return self._repository.persist(session, event, metadata, self._consumer_group)

    def _log_retry(self, error: Exception, retry_number: int, delay: float) -> None:
        self._logger.warning(
            "temporary operation failure; retry scheduled",
            extra={
                "error_type": type(error).__name__,
                "retry_number": retry_number,
                "retry_delay_seconds": delay,
            },
        )


class OrderConsumerRunner:
    """Small polling loop that commits only completed processing results."""

    def __init__(
        self,
        consumer: OrderKafkaConsumer,
        processor: OrderProcessor,
        retry_policy: RetryPolicy,
        poll_timeout_seconds: float,
        logger: logging.LoggerAdapter[logging.Logger],
        sleeper: Callable[[float], None] = sleep,
    ) -> None:
        """Store the consumer dependencies and initialize running state."""
        self._consumer = consumer
        self._processor = processor
        self._retry_policy = retry_policy
        self._poll_timeout = poll_timeout_seconds
        self._logger = logger
        self._sleeper = sleeper
        self._shutdown_requested = False

    def request_shutdown(self) -> None:
        """Ask the loop to stop after its current message finishes safely."""
        self._shutdown_requested = True

    def run(self) -> None:
        """Poll, process, and synchronously commit until shutdown is requested."""
        while not self._shutdown_requested:
            message = run_with_retry(
                lambda: self._consumer.poll(self._poll_timeout),
                operation_name="Kafka poll",
                policy=self._retry_policy,
                is_retryable=is_retryable_kafka_error,
                sleeper=self._sleeper,
                on_retry=self._log_kafka_retry,
            )
            if message is None:
                continue
            started = monotonic()
            result = self._processor.process(message)
            run_with_retry(
                partial(self._consumer.commit, message),
                operation_name="Kafka offset commit",
                policy=self._retry_policy,
                is_retryable=is_retryable_kafka_error,
                sleeper=self._sleeper,
                on_retry=self._log_kafka_retry,
            )
            self._logger.info(
                "order message completed",
                extra={
                    "result": result.value,
                    "topic": message.topic(),
                    "partition": message.partition(),
                    "offset": message.offset(),
                    "processing_time_ms": round((monotonic() - started) * 1000, 3),
                },
            )

    def _log_kafka_retry(self, error: Exception, retry_number: int, delay: float) -> None:
        self._logger.warning(
            "temporary Kafka failure; retry scheduled",
            extra={
                "error_type": type(error).__name__,
                "retry_number": retry_number,
                "retry_delay_seconds": delay,
            },
        )
