"""Composition root for the Phase 3 order consumer."""

import signal
from types import FrameType

from streaming_platform.config import get_settings
from streaming_platform.consumer.order import OrderConsumerRunner, OrderProcessor
from streaming_platform.consumer.retry import RetryPolicy
from streaming_platform.database.order_repository import OrderRepository
from streaming_platform.database.session import create_database_engine, create_session_factory
from streaming_platform.kafka.consumer import OrderKafkaConsumer
from streaming_platform.kafka.dlq import DlqProducer
from streaming_platform.logging import configure_logging


def run() -> int:
    """Build dependencies, handle process signals, and return an application exit code."""
    settings = get_settings()
    logger = configure_logging(settings, "order-consumer")
    retry_policy = RetryPolicy(
        max_retries=settings.ORDER_CONSUMER_MAX_RETRIES,
        base_seconds=settings.ORDER_CONSUMER_RETRY_BASE_SECONDS,
        max_seconds=settings.ORDER_CONSUMER_RETRY_MAX_SECONDS,
    )
    engine = create_database_engine(settings)
    consumer = OrderKafkaConsumer(settings)
    dlq_producer = DlqProducer(settings)
    processor = OrderProcessor(
        create_session_factory(engine),
        OrderRepository(),
        dlq_producer,
        settings.KAFKA_ORDER_CONSUMER_GROUP,
        retry_policy,
        logger,
    )
    runner = OrderConsumerRunner(
        consumer,
        processor,
        retry_policy,
        settings.ORDER_CONSUMER_POLL_TIMEOUT_SECONDS,
        logger,
    )
    received_signal: list[int] = []

    def handle_signal(signum: int, _frame: FrameType | None) -> None:
        received_signal.append(signum)
        logger.info("shutdown signal received", extra={"signal": signum})
        runner.request_shutdown()

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)
    logger.info(
        "order consumer started",
        extra={
            "topic": settings.KAFKA_ORDER_TOPIC,
            "consumer_group": settings.KAFKA_ORDER_CONSUMER_GROUP,
        },
    )
    try:
        runner.run()
    except Exception:
        logger.exception("order consumer stopped after an unrecoverable failure")
        return 1
    finally:
        remaining = dlq_producer.flush()
        if remaining:
            logger.error(
                "DLQ producer shutdown left messages pending", extra={"remaining": remaining}
            )
        consumer.close()
        engine.dispose()
        logger.info("order consumer stopped")
    return 128 + received_signal[-1] if received_signal else 0


def main() -> None:
    """Run the executable and expose its status to the shell."""
    raise SystemExit(run())


if __name__ == "__main__":
    main()
