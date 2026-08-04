"""Tests for environment-backed settings."""

import pytest
from pydantic import SecretStr, ValidationError

from streaming_platform.config import Settings


def test_settings_match_local_development_contract(settings: Settings) -> None:
    assert settings.KAFKA_BOOTSTRAP_SERVERS == "localhost:29092"
    assert settings.KAFKA_ORDER_TOPIC == "ecommerce.orders.raw.v1"
    assert settings.KAFKA_ORDER_CONSUMER_GROUP == "order-processing-group-v1"
    assert settings.KAFKA_LOG_CONSUMER_GROUP == "application-log-processing-group-v1"
    assert settings.KAFKA_UI_PORT == 8080
    assert settings.POSTGRES_PASSWORD == SecretStr("streaming")


def test_database_url_uses_psycopg_driver() -> None:
    settings = Settings(
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
        POSTGRES_PASSWORD="secret",
        LOG_LEVEL="INFO",
        _env_file=None,
    )

    assert settings.database_url == (
        "postgresql+psycopg://streaming:secret@localhost:5432/streaming"
    )


def test_missing_required_settings_fail_clearly(monkeypatch: pytest.MonkeyPatch) -> None:
    for field_name in Settings.model_fields:
        monkeypatch.delenv(field_name, raising=False)

    with pytest.raises(ValidationError, match="KAFKA_BOOTSTRAP_SERVERS"):
        Settings(_env_file=None)
