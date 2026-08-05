"""Order event payloads and discriminated event models."""

from decimal import Decimal
from enum import StrEnum
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter

from streaming_platform.models.event import BaseEventEnvelope, EventType, NonEmptyString


class Currency(StrEnum):
    """Currencies accepted by version-one order events."""

    TWD = "TWD"
    USD = "USD"


class OrderChannel(StrEnum):
    """Channels accepted by order-created events."""

    WEB = "web"
    IOS = "ios"
    ANDROID = "android"


PositiveAmount = Annotated[Decimal, Field(gt=0)]


class OrderCreatedPayload(BaseModel):
    """Business data required when an order is created."""

    model_config = ConfigDict(extra="forbid")

    order_id: NonEmptyString
    user_id: NonEmptyString
    product_id: NonEmptyString
    quantity: Annotated[int, Field(gt=0)]
    amount: PositiveAmount
    currency: Currency
    channel: OrderChannel


class OrderPaidPayload(BaseModel):
    """Business data required when an order is paid."""

    model_config = ConfigDict(extra="forbid")

    order_id: NonEmptyString
    user_id: NonEmptyString
    payment_id: NonEmptyString
    amount: PositiveAmount
    currency: Currency
    payment_method: NonEmptyString


class OrderCancelledPayload(BaseModel):
    """Business data required when an order is cancelled."""

    model_config = ConfigDict(extra="forbid")

    order_id: NonEmptyString
    user_id: NonEmptyString
    cancellation_reason: NonEmptyString


class PaymentFailedPayload(BaseModel):
    """Business data required when an order payment fails."""

    model_config = ConfigDict(extra="forbid")

    order_id: NonEmptyString
    user_id: NonEmptyString
    payment_id: NonEmptyString
    amount: PositiveAmount
    currency: Currency
    failure_code: NonEmptyString
    failure_reason: NonEmptyString


class OrderCreatedEvent(BaseEventEnvelope[OrderCreatedPayload]):
    """An order-created event."""

    event_type: Literal[EventType.ORDER_CREATED] = EventType.ORDER_CREATED


class OrderPaidEvent(BaseEventEnvelope[OrderPaidPayload]):
    """An order-paid event."""

    event_type: Literal[EventType.ORDER_PAID] = EventType.ORDER_PAID


class OrderCancelledEvent(BaseEventEnvelope[OrderCancelledPayload]):
    """An order-cancelled event."""

    event_type: Literal[EventType.ORDER_CANCELLED] = EventType.ORDER_CANCELLED


class PaymentFailedEvent(BaseEventEnvelope[PaymentFailedPayload]):
    """A payment-failed event."""

    event_type: Literal[EventType.PAYMENT_FAILED] = EventType.PAYMENT_FAILED


OrderEvent = Annotated[
    OrderCreatedEvent | OrderPaidEvent | OrderCancelledEvent | PaymentFailedEvent,
    Field(discriminator="event_type"),
]
ORDER_EVENT_ADAPTER: TypeAdapter[OrderEvent] = TypeAdapter(OrderEvent)
