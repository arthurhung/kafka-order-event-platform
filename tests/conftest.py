"""Shared test fixtures."""

from uuid import uuid4

import pytest

from streaming_platform.config import Settings
from tests.integration.kafka_helpers import ensure_test_topics_ready


@pytest.fixture
def settings(request: pytest.FixtureRequest) -> Settings:
    suffix = uuid4().hex if request.node.get_closest_marker("integration") else None
    configured = Settings(
        _env_file=None,
        KAFKA_BOOTSTRAP_SERVERS="localhost:29092",
        KAFKA_ORDER_TOPIC=(
            f"integration.orders.{suffix}.v1"
            if suffix
            else "ecommerce.orders.raw.v1"
        ),
        KAFKA_LOG_TOPIC=(
            f"integration.application-logs.{suffix}.v1"
            if suffix
            else "ecommerce.application-logs.raw.v1"
        ),
        KAFKA_DLQ_TOPIC=(
            f"integration.dlq.{suffix}.v1" if suffix else "ecommerce.dlq.v1"
        ),
        KAFKA_ORDER_CONSUMER_GROUP=(
            f"order-processing-integration-{suffix}"
            if suffix
            else "order-processing-group-v1"
        ),
        KAFKA_LOG_CONSUMER_GROUP=(
            f"application-log-integration-{suffix}"
            if suffix
            else "application-log-processing-group-v1"
        ),
        KAFKA_UI_PORT=8080,
        POSTGRES_HOST="localhost",
        POSTGRES_PORT=5432,
        POSTGRES_DB="streaming",
        POSTGRES_USER="streaming",
        POSTGRES_PASSWORD="streaming",
        LOG_LEVEL="INFO",
    )
    if suffix:
        ensure_test_topics_ready(configured)
    return configured
