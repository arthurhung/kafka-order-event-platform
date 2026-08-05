"""Composition root for the Phase 4 application-log consumer."""

import signal
from types import FrameType

from confluent_kafka import TopicPartition

from streaming_platform.config import get_settings
from streaming_platform.consumer.log import (
    LogConsumerRunner,
    LogFlushService,
    LogMessageProcessor,
)
from streaming_platform.consumer.offsets import ContiguousOffsetTracker
from streaming_platform.consumer.retry import RetryPolicy
from streaming_platform.database.log_repository import LogMetricRepository
from streaming_platform.database.session import create_database_engine, create_session_factory
from streaming_platform.kafka.consumer import LogKafkaConsumer
from streaming_platform.kafka.dlq import DlqProducer
from streaming_platform.logging import configure_logging
from streaming_platform.metrics.log_aggregation import LogAggregationBuffer


def run() -> int:
    """Build dependencies, handle signals, and return an application exit code."""
    settings = get_settings()
    logger = configure_logging(settings, "log-consumer")
    retry_policy = RetryPolicy(
        max_retries=settings.LOG_CONSUMER_MAX_RETRIES,
        base_seconds=settings.LOG_CONSUMER_RETRY_BASE_SECONDS,
        max_seconds=settings.LOG_CONSUMER_RETRY_MAX_SECONDS,
    )
    engine = create_database_engine(settings)
    buffer = LogAggregationBuffer()
    offsets = ContiguousOffsetTracker()
    runner_reference: list[LogConsumerRunner] = []

    def on_revoke(partitions: list[TopicPartition]) -> None:
        runner_reference[0].on_revoke(partitions)

    def on_lost(partitions: list[TopicPartition]) -> None:
        runner_reference[0].on_lost(partitions)

    consumer = LogKafkaConsumer(settings, on_revoke=on_revoke, on_lost=on_lost)
    dlq_producer = DlqProducer(
        settings,
        delivery_timeout_seconds=settings.LOG_CONSUMER_DELIVERY_TIMEOUT_SECONDS,
    )
    processor = LogMessageProcessor(
        buffer,
        offsets,
        dlq_producer,
        settings.KAFKA_LOG_CONSUMER_GROUP,
        retry_policy,
        logger,
    )
    flush_service = LogFlushService(
        buffer,
        offsets,
        create_session_factory(engine),
        LogMetricRepository(),
        consumer,
        settings.KAFKA_LOG_CONSUMER_GROUP,
        retry_policy,
        logger,
    )
    runner = LogConsumerRunner(
        consumer,
        processor,
        flush_service,
        offsets,
        retry_policy,
        settings.LOG_CONSUMER_POLL_TIMEOUT_SECONDS,
        settings.LOG_CONSUMER_FLUSH_INTERVAL_SECONDS,
        logger,
    )
    runner_reference.append(runner)
    received_signal: list[int] = []

    def handle_signal(signum: int, _frame: FrameType | None) -> None:
        received_signal.append(signum)
        logger.info("shutdown signal received", extra={"signal": signum})
        runner.request_shutdown()

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)
    logger.info(
        "log consumer started",
        extra={
            "topic": settings.KAFKA_LOG_TOPIC,
            "consumer_group": settings.KAFKA_LOG_CONSUMER_GROUP,
            "flush_interval_seconds": settings.LOG_CONSUMER_FLUSH_INTERVAL_SECONDS,
        },
    )
    exit_code = 0
    try:
        runner.run()
        runner.shutdown()
    except Exception:
        logger.exception("log consumer stopped after an unrecoverable failure")
        exit_code = 1
    finally:
        remaining = dlq_producer.flush()
        if remaining:
            logger.error(
                "DLQ producer shutdown left messages pending", extra={"remaining": remaining}
            )
            exit_code = 1
        consumer.close()
        engine.dispose()
        logger.info("log consumer stopped", extra={"buffered_events": len(buffer)})
    if exit_code:
        return exit_code
    return 128 + received_signal[-1] if received_signal else 0


def main() -> None:
    """Run the executable and expose its status to the shell."""
    raise SystemExit(run())


if __name__ == "__main__":
    main()
