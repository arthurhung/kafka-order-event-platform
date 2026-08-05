"""Application-log validation, buffering, durable flush, and offset orchestration."""

import logging
from collections.abc import Callable
from datetime import UTC, datetime
from enum import StrEnum
from functools import partial
from time import monotonic, sleep

from confluent_kafka import TopicPartition
from pydantic import JsonValue, ValidationError
from sqlalchemy.orm import Session, sessionmaker

from streaming_platform.consumer.errors import (
    PermanentMessageError,
    build_dlq_message,
    decode_json_value,
    is_retryable_database_error,
    safe_validation_message,
)
from streaming_platform.consumer.offsets import ContiguousOffsetTracker, PartitionKey
from streaming_platform.consumer.retry import RetryPolicy, run_with_retry
from streaming_platform.database.log_repository import LogMetricRepository
from streaming_platform.kafka.consumer import (
    KafkaMessage,
    LogKafkaConsumer,
    is_retryable_kafka_error,
)
from streaming_platform.kafka.dlq import DlqProducer
from streaming_platform.metrics.log_aggregation import (
    AggregationSnapshot,
    KafkaCoordinate,
    LogAggregationBuffer,
)
from streaming_platform.models import API_ACCESS_LOG_ADAPTER, ApiAccessLogEvent, EventType


class LogProcessingResult(StrEnum):
    """Outcomes produced before a future contiguous offset commit."""

    BUFFERED = "buffered"
    DUPLICATE_BUFFERED = "duplicate_buffered"
    DLQED = "dlqed"


class PartitionLostError(RuntimeError):
    """Raised when Kafka reports that assignment ownership is already lost."""


def decode_access_log_event(raw_value: bytes) -> ApiAccessLogEvent:
    """Decode and validate the API access-log type handled by minute metrics."""
    decoded = decode_json_value(raw_value)
    if isinstance(decoded, dict):
        event_type = decoded.get("event_type")
        if event_type is not None and event_type != EventType.API_ACCESS_LOG.value:
            raise PermanentMessageError(
                "UnsupportedEventType",
                f"event_type {str(event_type)[:100]} is not supported by log metrics",
                decoded,
            )
    try:
        return API_ACCESS_LOG_ADAPTER.validate_python(decoded)
    except ValidationError as error:
        safe_message = safe_validation_message(error, "application log validation failed")
        raise PermanentMessageError("ValidationError", safe_message, decoded) from error


class LogMessageProcessor:
    """Route permanent errors to DLQ and valid access logs to the buffer."""

    def __init__(
        self,
        buffer: LogAggregationBuffer,
        offsets: ContiguousOffsetTracker,
        dlq_producer: DlqProducer,
        consumer_group: str,
        retry_policy: RetryPolicy,
        logger: logging.LoggerAdapter[logging.Logger],
        sleeper: Callable[[float], None] = sleep,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        """Store reusable buffering, DLQ, retry, and clock dependencies."""
        self._buffer = buffer
        self._offsets = offsets
        self._dlq_producer = dlq_producer
        self._consumer_group = consumer_group
        self._retry_policy = retry_policy
        self._logger = logger
        self._sleeper = sleeper
        self._clock = clock or (lambda: datetime.now(UTC))

    def process(self, message: KafkaMessage) -> LogProcessingResult:
        """Handle one observed message without directly committing Kafka offsets."""
        coordinate = KafkaCoordinate(message.topic(), message.partition(), message.offset())
        try:
            event = decode_access_log_event(message.value() or b"")
            inserted = self._buffer.add(event, coordinate)
        except PermanentMessageError as error:
            self._publish_dlq(message, error)
            self._offsets.complete(coordinate)
            return LogProcessingResult.DLQED
        except ValueError as error:
            permanent = PermanentMessageError(
                "DuplicateEventConflict",
                "event_id was reused with different metric data",
                self._decoded_for_dlq(message.value() or b""),
            )
            self._logger.warning(
                "conflicting duplicate event routed to DLQ",
                extra={"error_type": type(error).__name__, "offset": coordinate.offset},
            )
            self._publish_dlq(message, permanent)
            self._offsets.complete(coordinate)
            return LogProcessingResult.DLQED
        return (
            LogProcessingResult.BUFFERED
            if inserted
            else LogProcessingResult.DUPLICATE_BUFFERED
        )

    def _publish_dlq(self, message: KafkaMessage, error: PermanentMessageError) -> None:
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

    @staticmethod
    def _decoded_for_dlq(raw_value: bytes) -> JsonValue | None:
        try:
            return decode_json_value(raw_value)
        except PermanentMessageError:
            return None

    def _log_retry(self, error: Exception, retry_number: int, delay: float) -> None:
        self._logger.warning(
            "temporary DLQ failure; retry scheduled",
            extra={
                "error_type": type(error).__name__,
                "retry_number": retry_number,
                "retry_delay_seconds": delay,
            },
        )


class LogFlushService:
    """Flush snapshots transactionally and commit only contiguous durable offsets."""

    def __init__(
        self,
        buffer: LogAggregationBuffer,
        offsets: ContiguousOffsetTracker,
        session_factory: sessionmaker[Session],
        repository: LogMetricRepository,
        consumer: LogKafkaConsumer,
        consumer_group: str,
        retry_policy: RetryPolicy,
        logger: logging.LoggerAdapter[logging.Logger],
        sleeper: Callable[[float], None] = sleep,
    ) -> None:
        """Store transaction, offset, retry, and logging dependencies."""
        self._buffer = buffer
        self._offsets = offsets
        self._session_factory = session_factory
        self._repository = repository
        self._consumer = consumer
        self._consumer_group = consumer_group
        self._retry_policy = retry_policy
        self._logger = logger
        self._sleeper = sleeper

    def flush(
        self,
        commit_partitions: set[PartitionKey] | None = None,
        *,
        commit: bool = True,
    ) -> int:
        """Persist a safe snapshot, restore it on failure, and optionally commit."""
        snapshot = self._buffer.take_snapshot()
        if snapshot.events:
            try:
                inserted_count = run_with_retry(
                    lambda: self._persist_once(snapshot),
                    operation_name="log metric database transaction",
                    policy=self._retry_policy,
                    is_retryable=is_retryable_database_error,
                    sleeper=self._sleeper,
                    on_retry=self._log_retry,
                )
            except Exception:
                self._buffer.restore(snapshot)
                raise
            self._offsets.complete_many(snapshot.coordinates)
            self._logger.info(
                "log aggregation snapshot persisted",
                extra={
                    "unique_events": len(snapshot.events),
                    "new_events": inserted_count,
                    "source_records": len(snapshot.coordinates),
                },
            )
        if commit:
            self.commit_ready(commit_partitions)
        return len(snapshot.events)

    def commit_ready(self, partitions: set[PartitionKey] | None = None) -> None:
        """Commit each partition's explicit safe next offset synchronously."""
        for key, next_offset in sorted(self._offsets.committable(partitions).items()):
            topic, partition = key
            position = TopicPartition(topic, partition, next_offset)
            run_with_retry(
                partial(self._consumer.commit_offsets, [position]),
                operation_name="Kafka offset commit",
                policy=self._retry_policy,
                is_retryable=is_retryable_kafka_error,
                sleeper=self._sleeper,
                on_retry=self._log_retry,
            )
            self._offsets.acknowledge_commit(key, next_offset)

    def _persist_once(self, snapshot: AggregationSnapshot) -> int:
        with self._session_factory() as session, session.begin():
            inserted = self._repository.persist_snapshot(
                session, snapshot, self._consumer_group
            )
            return len(inserted)

    def _log_retry(self, error: Exception, retry_number: int, delay: float) -> None:
        self._logger.warning(
            "temporary flush operation failure; retry scheduled",
            extra={
                "error_type": type(error).__name__,
                "retry_number": retry_number,
                "retry_delay_seconds": delay,
            },
        )


class LogConsumerRunner:
    """Poll logs, schedule periodic flushes, and coordinate rebalances."""

    def __init__(
        self,
        consumer: LogKafkaConsumer,
        processor: LogMessageProcessor,
        flush_service: LogFlushService,
        offsets: ContiguousOffsetTracker,
        retry_policy: RetryPolicy,
        poll_timeout_seconds: float,
        flush_interval_seconds: float,
        logger: logging.LoggerAdapter[logging.Logger],
        sleeper: Callable[[float], None] = sleep,
        monotonic_clock: Callable[[], float] = monotonic,
    ) -> None:
        """Store polling dependencies and initialize graceful-stop state."""
        self._consumer = consumer
        self._processor = processor
        self._flush_service = flush_service
        self._offsets = offsets
        self._retry_policy = retry_policy
        self._poll_timeout = poll_timeout_seconds
        self._flush_interval = flush_interval_seconds
        self._logger = logger
        self._sleeper = sleeper
        self._monotonic = monotonic_clock
        self._shutdown_requested = False

    def request_shutdown(self) -> None:
        """Stop polling after the current synchronous critical section."""
        self._shutdown_requested = True

    def run(self) -> None:
        """Poll until shutdown while flushing on the configured monotonic interval."""
        next_flush = self._monotonic() + self._flush_interval
        while not self._shutdown_requested:
            message = run_with_retry(
                lambda: self._consumer.poll(self._poll_timeout),
                operation_name="Kafka poll",
                policy=self._retry_policy,
                is_retryable=is_retryable_kafka_error,
                sleeper=self._sleeper,
                on_retry=self._log_retry,
            )
            if message is not None:
                coordinate = KafkaCoordinate(
                    message.topic(), message.partition(), message.offset()
                )
                self._offsets.observe(coordinate)
                result = self._processor.process(message)
                self._flush_service.commit_ready()
                self._logger.info(
                    "log message accepted",
                    extra={
                        "result": result.value,
                        "topic": coordinate.topic,
                        "partition": coordinate.partition,
                        "offset": coordinate.offset,
                    },
                )
            if self._monotonic() >= next_flush:
                self._flush_service.flush()
                next_flush = self._monotonic() + self._flush_interval

    def shutdown(self) -> None:
        """Durably flush buffered events and commit every safe contiguous offset."""
        self._flush_service.flush()

    def on_revoke(self, partitions: list[TopicPartition]) -> None:
        """Flush before revoke and commit only the partitions being returned."""
        keys = {(partition.topic, partition.partition) for partition in partitions}
        self._flush_service.flush(keys)
        if self._offsets.has_pending(keys):
            raise RuntimeError("partition revoke left non-durable source offsets")
        self._offsets.forget(keys)

    def on_lost(self, partitions: list[TopicPartition]) -> None:
        """Persist if possible but never commit partitions whose ownership is lost."""
        keys = {(partition.topic, partition.partition) for partition in partitions}
        self._flush_service.flush(keys, commit=False)
        self._shutdown_requested = True
        raise PartitionLostError(f"Kafka assignment lost for {sorted(keys)}")

    def _log_retry(self, error: Exception, retry_number: int, delay: float) -> None:
        self._logger.warning(
            "temporary Kafka failure; retry scheduled",
            extra={
                "error_type": type(error).__name__,
                "retry_number": retry_number,
                "retry_delay_seconds": delay,
            },
        )
