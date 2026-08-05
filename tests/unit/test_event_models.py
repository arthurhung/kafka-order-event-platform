"""Tests for the six concrete Phase 2 event models."""

from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal
from uuid import uuid4

import pytest
from pydantic import ValidationError

from streaming_platform.models import (
    PLATFORM_EVENT_ADAPTER,
    ApiAccessLogEvent,
    ApiAccessLogPayload,
    ApplicationErrorLogEvent,
    ApplicationErrorLogPayload,
    Currency,
    HttpMethod,
    OrderCancelledEvent,
    OrderCancelledPayload,
    OrderChannel,
    OrderCreatedEvent,
    OrderCreatedPayload,
    OrderPaidEvent,
    OrderPaidPayload,
    PaymentFailedEvent,
    PaymentFailedPayload,
)


def test_event_time_is_aware_and_normalized_to_utc() -> None:
    local_time = datetime(2026, 8, 4, 18, 0, tzinfo=timezone(timedelta(hours=8)))
    event = OrderCancelledEvent(
        event_id=uuid4(),
        event_time=local_time,
        source="order-api",
        payload=OrderCancelledPayload(
            order_id="ORD-1", user_id="USR-1", cancellation_reason="requested"
        ),
    )

    assert event.event_time == datetime(2026, 8, 4, 10, 0, tzinfo=UTC)
    assert '"event_time":"2026-08-04T10:00:00Z"' in event.model_dump_json()


def test_event_time_rejects_naive_datetime_but_accepts_stale_datetime() -> None:
    payload = OrderCancelledPayload(
        order_id="ORD-1", user_id="USR-1", cancellation_reason="requested"
    )
    with pytest.raises(ValidationError, match="timezone info"):
        OrderCancelledEvent(
            event_id=uuid4(),
            event_time=datetime(2026, 8, 4, 10, 0),
            source="order-api",
            payload=payload,
        )

    stale = OrderCancelledEvent(
        event_id=uuid4(),
        event_time=datetime(2000, 1, 1, tzinfo=UTC),
        source="order-api",
        payload=payload,
    )
    assert stale.event_time.year == 2000


def test_each_event_type_uses_its_own_payload_model() -> None:
    now = datetime.now(UTC)
    events = (
        OrderCreatedEvent(
            event_id=uuid4(),
            event_time=now,
            source="order-api",
            payload=OrderCreatedPayload(
                order_id="ORD-1",
                user_id="USR-1",
                product_id="PRD-1",
                quantity=1,
                amount=Decimal("10.25"),
                currency=Currency.TWD,
                channel=OrderChannel.WEB,
            ),
        ),
        OrderPaidEvent(
            event_id=uuid4(),
            event_time=now,
            source="payment-api",
            payload=OrderPaidPayload(
                order_id="ORD-1",
                user_id="USR-1",
                payment_id="PAY-1",
                amount=Decimal("10.25"),
                currency=Currency.TWD,
                payment_method="card",
            ),
        ),
        OrderCancelledEvent(
            event_id=uuid4(),
            event_time=now,
            source="order-api",
            payload=OrderCancelledPayload(
                order_id="ORD-1", user_id="USR-1", cancellation_reason="requested"
            ),
        ),
        PaymentFailedEvent(
            event_id=uuid4(),
            event_time=now,
            source="payment-api",
            payload=PaymentFailedPayload(
                order_id="ORD-1",
                user_id="USR-1",
                payment_id="PAY-1",
                amount=Decimal("10.25"),
                currency=Currency.USD,
                failure_code="DECLINED",
                failure_reason="declined",
            ),
        ),
        ApiAccessLogEvent(
            event_id=uuid4(),
            event_time=now,
            source="order-api",
            payload=ApiAccessLogPayload(
                request_id="REQ-1",
                service="order-api",
                endpoint="/orders",
                http_method=HttpMethod.POST,
                status_code=201,
                response_time_ms=10,
                client_ip="10.0.0.1",
            ),
        ),
        ApplicationErrorLogEvent(
            event_id=uuid4(),
            event_time=now,
            source="order-api",
            payload=ApplicationErrorLogPayload(
                request_id="REQ-1",
                service="order-api",
                error_type="RuntimeError",
                error_message="failed",
            ),
        ),
    )

    parsed = [PLATFORM_EVENT_ADAPTER.validate_json(event.model_dump_json()) for event in events]
    assert [type(event.payload) for event in parsed] == [
        OrderCreatedPayload,
        OrderPaidPayload,
        OrderCancelledPayload,
        PaymentFailedPayload,
        ApiAccessLogPayload,
        ApplicationErrorLogPayload,
    ]


def test_order_amount_is_decimal_and_serializes_without_float() -> None:
    payload = OrderPaidPayload(
        order_id="ORD-1",
        user_id="USR-1",
        payment_id="PAY-1",
        amount="1800.00",
        currency="TWD",
        payment_method="card",
    )
    assert payload.amount == Decimal("1800.00")
    assert '"amount":"1800.00"' in payload.model_dump_json()

    with pytest.raises(ValidationError):
        OrderPaidPayload(
            order_id="ORD-1",
            user_id="USR-1",
            payment_id="PAY-1",
            amount="-1.00",
            currency="TWD",
            payment_method="card",
        )


def test_http_method_allowlist_rejects_trace() -> None:
    with pytest.raises(ValidationError, match=r"GET|POST"):
        ApiAccessLogPayload(
            request_id="REQ-1",
            service="order-api",
            endpoint="/orders",
            http_method="TRACE",
            status_code=200,
            response_time_ms=1,
            client_ip="10.0.0.1",
        )
