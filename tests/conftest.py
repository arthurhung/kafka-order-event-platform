"""Shared Phase 1 test fixtures."""

import pytest

from streaming_platform.config import Settings


@pytest.fixture
def settings() -> Settings:
    return Settings(
        _env_file=None,
        KAFKA_BOOTSTRAP_SERVERS="localhost:29092",
        KAFKA_ORDER_TOPIC="ecommerce.orders.raw.v1",
        KAFKA_LOG_TOPIC="ecommerce.application-logs.raw.v1",
        KAFKA_DLQ_TOPIC="ecommerce.dlq.v1",
        KAFKA_ORDER_CONSUMER_GROUP="order-processing-group-v1",
        KAFKA_LOG_CONSUMER_GROUP="application-log-processing-group-v1",
        KAFKA_UI_PORT=8080,
        POSTGRES_HOST="localhost",
        POSTGRES_PORT=5432,
        POSTGRES_DB="streaming",
        POSTGRES_USER="streaming",
        POSTGRES_PASSWORD="streaming",
        LOG_LEVEL="INFO",
    )
