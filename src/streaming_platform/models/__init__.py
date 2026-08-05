"""Validated version-one platform event models."""

from typing import Annotated

from pydantic import Field, TypeAdapter

from streaming_platform.models.application_log import (
    ApiAccessLogEvent,
    ApiAccessLogPayload,
    ApplicationErrorLogEvent,
    ApplicationErrorLogPayload,
    HttpMethod,
)
from streaming_platform.models.event import BaseEventEnvelope, EventType
from streaming_platform.models.order import (
    Currency,
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

PlatformEvent = Annotated[
    OrderCreatedEvent
    | OrderPaidEvent
    | OrderCancelledEvent
    | PaymentFailedEvent
    | ApiAccessLogEvent
    | ApplicationErrorLogEvent,
    Field(discriminator="event_type"),
]
PLATFORM_EVENT_ADAPTER: TypeAdapter[PlatformEvent] = TypeAdapter(PlatformEvent)

__all__ = [
    "PLATFORM_EVENT_ADAPTER",
    "ApiAccessLogEvent",
    "ApiAccessLogPayload",
    "ApplicationErrorLogEvent",
    "ApplicationErrorLogPayload",
    "BaseEventEnvelope",
    "Currency",
    "EventType",
    "HttpMethod",
    "OrderCancelledEvent",
    "OrderCancelledPayload",
    "OrderChannel",
    "OrderCreatedEvent",
    "OrderCreatedPayload",
    "OrderPaidEvent",
    "OrderPaidPayload",
    "PaymentFailedEvent",
    "PaymentFailedPayload",
    "PlatformEvent",
]
