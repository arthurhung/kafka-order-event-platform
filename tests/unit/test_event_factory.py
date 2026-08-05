"""Tests for deterministic event generation and injection behavior."""

import json
from datetime import UTC, datetime, timedelta

import pytest
from pydantic import ValidationError

from streaming_platform.config import Settings
from streaming_platform.generator.factory import (
    LOG_SERVICE_CANDIDATES,
    EventFactory,
    InjectionKind,
    InvalidKind,
)
from streaming_platform.models import PLATFORM_EVENT_ADAPTER, EventType

FIXED_NOW = datetime(2026, 8, 4, 12, 0, tzinfo=UTC)


def test_same_seed_and_clock_produce_same_record(settings: Settings) -> None:
    first = EventFactory(settings, seed=42, clock=lambda: FIXED_NOW)
    second = EventFactory(settings, seed=42, clock=lambda: FIXED_NOW)

    assert first.create_normal(EventType.ORDER_CREATED) == second.create_normal(
        EventType.ORDER_CREATED
    )


def test_new_events_have_unique_ids(settings: Settings) -> None:
    factory = EventFactory(settings, seed=42, clock=lambda: FIXED_NOW)
    records = [factory.create_normal(EventType.ORDER_CREATED) for _ in range(100)]
    assert len({record.event_id for record in records}) == 100


def test_topic_and_key_routing(settings: Settings) -> None:
    factory = EventFactory(settings, seed=42, clock=lambda: FIXED_NOW)
    order = factory.create_normal(EventType.ORDER_PAID)
    application_log = factory.create_normal(EventType.APPLICATION_ERROR_LOG)

    assert order.topic == settings.KAFKA_ORDER_TOPIC
    assert order.key.decode() == json.loads(order.value)["payload"]["order_id"]
    assert application_log.topic == settings.KAFKA_LOG_TOPIC
    assert application_log.key.decode() == json.loads(application_log.value)["payload"]["service"]


def test_log_service_candidates_are_complete_and_randomly_selected(settings: Settings) -> None:
    expected_services = {
        "order-api",
        "payment-api",
        "user-api",
        "inventory-api",
        "notification-api",
        "gateway-api",
        "catalog-api",
        "auth-api",
        "shipping-api",
    }
    assert set(LOG_SERVICE_CANDIDATES) == expected_services

    factory = EventFactory(settings, seed=42, clock=lambda: FIXED_NOW)
    generated_services = {
        json.loads(factory.create_normal(EventType.APPLICATION_ERROR_LOG).value)["payload"][
            "service"
        ]
        for _ in range(1_000)
    }
    assert generated_services == expected_services


@pytest.mark.parametrize(
    "event_type, invalid_kind",
    [
        (EventType.ORDER_CANCELLED, InvalidKind.MISSING_REQUIRED_FIELD),
        (EventType.ORDER_PAID, InvalidKind.NEGATIVE_AMOUNT),
        (EventType.PAYMENT_FAILED, InvalidKind.INVALID_CURRENCY),
        (EventType.ORDER_CREATED, InvalidKind.INVALID_CHANNEL),
        (EventType.API_ACCESS_LOG, InvalidKind.INVALID_HTTP_METHOD),
        (EventType.API_ACCESS_LOG, InvalidKind.INVALID_STATUS_CODE),
        (EventType.APPLICATION_ERROR_LOG, InvalidKind.UNSUPPORTED_EVENT_TYPE),
        (EventType.APPLICATION_ERROR_LOG, InvalidKind.MALFORMED_PAYLOAD),
    ],
)
def test_each_invalid_injection_fails_schema_validation(
    settings: Settings,
    event_type: EventType,
    invalid_kind: InvalidKind,
) -> None:
    record = EventFactory(settings, seed=42, clock=lambda: FIXED_NOW).create_invalid(
        event_type, invalid_kind
    )

    assert record.injection_kind is InjectionKind.INVALID
    assert record.invalid_kind is invalid_kind
    with pytest.raises(ValidationError):
        PLATFORM_EVENT_ADAPTER.validate_json(record.value)


def test_stale_event_remains_schema_valid_and_is_counted_separately(settings: Settings) -> None:
    factory = EventFactory(settings, seed=42, clock=lambda: FIXED_NOW)
    record = factory.create_stale(EventType.API_ACCESS_LOG, stale_hours=168)
    parsed = PLATFORM_EVENT_ADAPTER.validate_json(record.value)

    assert record.injection_kind is InjectionKind.STALE
    assert parsed.event_time == FIXED_NOW - timedelta(hours=168)


def test_duplicate_is_an_exact_replay_with_same_event_id(settings: Settings) -> None:
    factory = EventFactory(settings, seed=42, clock=lambda: FIXED_NOW)
    original = factory.create_normal(EventType.ORDER_CREATED)
    duplicate = factory.create_duplicate(original)

    assert duplicate.injection_kind is InjectionKind.DUPLICATE
    assert duplicate.event_id == original.event_id
    assert duplicate.topic == original.topic
    assert duplicate.key == original.key
    assert duplicate.value == original.value
