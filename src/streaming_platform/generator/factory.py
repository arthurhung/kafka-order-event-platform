"""Deterministic factories for valid, invalid, stale, and duplicate events."""

import json
import random
from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from typing import Literal
from uuid import UUID

from streaming_platform.config import Settings
from streaming_platform.generator.options import GeneratorOptions
from streaming_platform.models import (
    ApiAccessLogEvent,
    ApiAccessLogPayload,
    ApplicationErrorLogEvent,
    ApplicationErrorLogPayload,
    Currency,
    EventType,
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
    PlatformEvent,
)

EventFamily = Literal["order", "log"]
LOG_SERVICE_CANDIDATES = (
    "order-api",
    "payment-api",
    "user-api",
    "inventory-api",
    "notification-api",
    "gateway-api",
    "catalog-api",
    "auth-api",
    "shipping-api",
)


class InjectionKind(StrEnum):
    """Mutually exclusive generator event classifications."""

    NORMAL = "normal"
    INVALID = "invalid"
    STALE = "stale"
    DUPLICATE = "duplicate"


class InvalidKind(StrEnum):
    """Supported schema-invalid mutations."""

    MISSING_REQUIRED_FIELD = "missing_required_field"
    NEGATIVE_AMOUNT = "negative_amount"
    INVALID_CURRENCY = "invalid_currency"
    INVALID_CHANNEL = "invalid_channel"
    INVALID_HTTP_METHOD = "invalid_http_method"
    INVALID_STATUS_CODE = "invalid_status_code"
    UNSUPPORTED_EVENT_TYPE = "unsupported_event_type"
    MALFORMED_PAYLOAD = "malformed_payload"


@dataclass(frozen=True, slots=True)
class GeneratedRecord:
    """A fully routed Kafka record plus generator classification metadata."""

    topic: str
    key: bytes
    value: bytes
    event_id: UUID
    event_type: EventType
    family: EventFamily
    injection_kind: InjectionKind
    invalid_kind: InvalidKind | None = None


class EventFactory:
    """Generate deterministic event data from one isolated random seed."""

    def __init__(
        self,
        settings: Settings,
        seed: int,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        """Initialize isolated deterministic randomness and routing settings."""
        self._settings = settings
        self._random = random.Random(seed)  # noqa: S311 - reproducible test data, not security
        self._clock = clock or (lambda: datetime.now(UTC))
        self._history: dict[EventFamily, list[GeneratedRecord]] = {"order": [], "log": []}

    def next_record(self, options: GeneratorOptions) -> GeneratedRecord:
        """Generate the next record according to mix and injection rates."""
        family = self._choose_family(options.order_ratio)
        event_type = self._choose_event_type(family)
        roll = self._random.random()
        if roll < options.invalid_rate:
            invalid_kind = self._random.choice(self.invalid_kinds_for(event_type))
            return self.create_invalid(event_type, invalid_kind)
        if roll < options.invalid_rate + options.stale_rate:
            return self.create_stale(event_type, options.stale_hours)
        duplicate_limit = options.invalid_rate + options.stale_rate + options.duplicate_rate
        if roll < duplicate_limit and self._history[family]:
            return self.create_duplicate(self._random.choice(self._history[family]))

        record = self.create_normal(event_type)
        self._history[family].append(record)
        return record

    def create_normal(self, event_type: EventType) -> GeneratedRecord:
        """Create one schema-valid current event."""
        event = self.build_event(event_type, self._clock())
        return self._route(event, InjectionKind.NORMAL)

    def create_stale(self, event_type: EventType, stale_hours: float) -> GeneratedRecord:
        """Create one schema-valid event with an intentionally old timestamp."""
        event_time = self._clock() - timedelta(hours=stale_hours)
        event = self.build_event(event_type, event_time)
        return self._route(event, InjectionKind.STALE)

    def create_duplicate(self, original: GeneratedRecord) -> GeneratedRecord:
        """Create an exact replay classified as a duplicate."""
        return replace(original, injection_kind=InjectionKind.DUPLICATE, invalid_kind=None)

    def create_invalid(
        self,
        event_type: EventType,
        invalid_kind: InvalidKind,
    ) -> GeneratedRecord:
        """Create a routed event and then apply one schema-invalid mutation."""
        if invalid_kind not in self.invalid_kinds_for(event_type):
            raise ValueError(f"{invalid_kind.value} is not valid for {event_type.value}")
        valid_record = self.create_normal(event_type)
        value = json.loads(valid_record.value)
        self._mutate_invalid(value, invalid_kind)
        return replace(
            valid_record,
            value=self._encode(value),
            injection_kind=InjectionKind.INVALID,
            invalid_kind=invalid_kind,
        )

    def build_event(self, event_type: EventType, event_time: datetime) -> PlatformEvent:
        """Build one concrete event model for the requested event type."""
        event_id = self._uuid()
        if event_type is EventType.ORDER_CREATED:
            return OrderCreatedEvent(
                event_id=event_id,
                event_time=event_time,
                source="order-api",
                payload=self._order_created_payload(),
            )
        if event_type is EventType.ORDER_PAID:
            return OrderPaidEvent(
                event_id=event_id,
                event_time=event_time,
                source="payment-api",
                payload=self._order_paid_payload(),
            )
        if event_type is EventType.ORDER_CANCELLED:
            return OrderCancelledEvent(
                event_id=event_id,
                event_time=event_time,
                source="order-api",
                payload=self._order_cancelled_payload(),
            )
        if event_type is EventType.PAYMENT_FAILED:
            return PaymentFailedEvent(
                event_id=event_id,
                event_time=event_time,
                source="payment-api",
                payload=self._payment_failed_payload(),
            )
        service = self._random.choice(LOG_SERVICE_CANDIDATES)
        if event_type is EventType.API_ACCESS_LOG:
            return ApiAccessLogEvent(
                event_id=event_id,
                event_time=event_time,
                source=service,
                payload=self._api_access_payload(service),
            )
        return ApplicationErrorLogEvent(
            event_id=event_id,
            event_time=event_time,
            source=service,
            payload=self._application_error_payload(service),
        )

    @staticmethod
    def invalid_kinds_for(event_type: EventType) -> tuple[InvalidKind, ...]:
        """Return only invalid mutations applicable to an event type."""
        common: tuple[InvalidKind, ...] = (
            InvalidKind.MISSING_REQUIRED_FIELD,
            InvalidKind.UNSUPPORTED_EVENT_TYPE,
            InvalidKind.MALFORMED_PAYLOAD,
        )
        if event_type in {
            EventType.ORDER_CREATED,
            EventType.ORDER_PAID,
            EventType.PAYMENT_FAILED,
        }:
            common += (InvalidKind.NEGATIVE_AMOUNT, InvalidKind.INVALID_CURRENCY)
        if event_type is EventType.ORDER_CREATED:
            common += (InvalidKind.INVALID_CHANNEL,)
        if event_type is EventType.API_ACCESS_LOG:
            common += (InvalidKind.INVALID_HTTP_METHOD, InvalidKind.INVALID_STATUS_CODE)
        return common

    def _route(self, event: PlatformEvent, injection_kind: InjectionKind) -> GeneratedRecord:
        if isinstance(
            event,
            (OrderCreatedEvent, OrderPaidEvent, OrderCancelledEvent, PaymentFailedEvent),
        ):
            topic = self._settings.KAFKA_ORDER_TOPIC
            key = event.payload.order_id.encode()
            family: EventFamily = "order"
        else:
            topic = self._settings.KAFKA_LOG_TOPIC
            key = event.payload.service.encode()
            family = "log"
        return GeneratedRecord(
            topic=topic,
            key=key,
            value=self._encode(event.model_dump(mode="json")),
            event_id=event.event_id,
            event_type=event.event_type,
            family=family,
            injection_kind=injection_kind,
        )

    @staticmethod
    def _mutate_invalid(value: dict[str, object], invalid_kind: InvalidKind) -> None:
        payload = value.get("payload")
        if invalid_kind is InvalidKind.UNSUPPORTED_EVENT_TYPE:
            value["event_type"] = "unsupported_event_type"
            return
        if invalid_kind is InvalidKind.MALFORMED_PAYLOAD:
            value["payload"] = ["malformed"]
            return
        if not isinstance(payload, dict):
            raise ValueError("valid event payload must be an object before invalid mutation")
        if invalid_kind is InvalidKind.MISSING_REQUIRED_FIELD:
            field = "order_id" if "order_id" in payload else "request_id"
            payload.pop(field)
        elif invalid_kind is InvalidKind.NEGATIVE_AMOUNT:
            payload["amount"] = "-1.00"
        elif invalid_kind is InvalidKind.INVALID_CURRENCY:
            payload["currency"] = "EUR"
        elif invalid_kind is InvalidKind.INVALID_CHANNEL:
            payload["channel"] = "store"
        elif invalid_kind is InvalidKind.INVALID_HTTP_METHOD:
            payload["http_method"] = "TRACE"
        elif invalid_kind is InvalidKind.INVALID_STATUS_CODE:
            payload["status_code"] = 700

    def _choose_family(self, order_ratio: float) -> EventFamily:
        return "order" if self._random.random() < order_ratio else "log"

    def _choose_event_type(self, family: EventFamily) -> EventType:
        if family == "order":
            return self._random.choice(
                (
                    EventType.ORDER_CREATED,
                    EventType.ORDER_PAID,
                    EventType.ORDER_CANCELLED,
                    EventType.PAYMENT_FAILED,
                )
            )
        return self._random.choice((EventType.API_ACCESS_LOG, EventType.APPLICATION_ERROR_LOG))

    def _order_created_payload(self) -> OrderCreatedPayload:
        return OrderCreatedPayload(
            order_id=self._id("ORD", 1_000_000),
            user_id=self._id("USR", 100_000),
            product_id=self._id("PRD", 10_000),
            quantity=self._random.randint(1, 5),
            amount=self._amount(),
            currency=self._random.choice(tuple(Currency)),
            channel=self._random.choice(tuple(OrderChannel)),
        )

    def _order_paid_payload(self) -> OrderPaidPayload:
        return OrderPaidPayload(
            order_id=self._id("ORD", 1_000_000),
            user_id=self._id("USR", 100_000),
            payment_id=self._id("PAY", 1_000_000),
            amount=self._amount(),
            currency=self._random.choice(tuple(Currency)),
            payment_method=self._random.choice(("credit_card", "bank_transfer", "wallet")),
        )

    def _order_cancelled_payload(self) -> OrderCancelledPayload:
        return OrderCancelledPayload(
            order_id=self._id("ORD", 1_000_000),
            user_id=self._id("USR", 100_000),
            cancellation_reason=self._random.choice(
                ("customer_request", "inventory_unavailable", "payment_timeout")
            ),
        )

    def _payment_failed_payload(self) -> PaymentFailedPayload:
        code, reason = self._random.choice(
            (("DECLINED", "card declined"), ("TIMEOUT", "gateway timeout"))
        )
        return PaymentFailedPayload(
            order_id=self._id("ORD", 1_000_000),
            user_id=self._id("USR", 100_000),
            payment_id=self._id("PAY", 1_000_000),
            amount=self._amount(),
            currency=self._random.choice(tuple(Currency)),
            failure_code=code,
            failure_reason=reason,
        )

    def _api_access_payload(self, service: str) -> ApiAccessLogPayload:
        endpoint = self._random.choice(("/orders", "/payments", "/inventory"))
        return ApiAccessLogPayload(
            request_id=self._id("REQ", 10_000_000),
            service=service,
            endpoint=endpoint,
            http_method=self._random.choice(tuple(HttpMethod)),
            status_code=self._random.choice((200, 201, 400, 404, 500, 503)),
            response_time_ms=self._random.randint(1, 2_000),
            client_ip=f"10.0.{self._random.randint(0, 255)}.{self._random.randint(1, 254)}",
        )

    def _application_error_payload(self, service: str) -> ApplicationErrorLogPayload:
        error_type, error_message = self._random.choice(
            (("TimeoutError", "upstream timeout"), ("RuntimeError", "unexpected failure"))
        )
        return ApplicationErrorLogPayload(
            request_id=self._id("REQ", 10_000_000),
            service=service,
            error_type=error_type,
            error_message=error_message,
            endpoint=self._random.choice(("/orders", "/payments", None)),
            trace_id=self._id("TRACE", 10_000_000),
        )

    def _uuid(self) -> UUID:
        return UUID(int=self._random.getrandbits(128), version=4)

    def _id(self, prefix: str, maximum: int) -> str:
        return f"{prefix}-{self._random.randint(1, maximum):08d}"

    def _amount(self) -> Decimal:
        return Decimal(self._random.randint(100, 500_000)) / Decimal(100)

    @staticmethod
    def _encode(value: object) -> bytes:
        return json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
