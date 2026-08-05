from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

from streaming_platform.database.models import ValidOrder
from streaming_platform.database.order_repository import KafkaRecordMetadata, OrderRepository
from streaming_platform.models import OrderCreatedEvent, OrderCreatedPayload


class ScalarResult:
    def __init__(self, value):
        self.value = value

    def scalar_one_or_none(self):
        return self.value


class FakeSession:
    def __init__(self, inserted_event_id):
        self.inserted_event_id = inserted_event_id
        self.added = []
        self.flush_calls = 0

    def execute(self, _statement):
        return ScalarResult(self.inserted_event_id)

    def add(self, value):
        self.added.append(value)

    def flush(self):
        self.flush_calls += 1


def order_event() -> OrderCreatedEvent:
    return OrderCreatedEvent(
        event_id=uuid4(),
        event_time=datetime.now(UTC),
        source="order-api",
        payload=OrderCreatedPayload(
            order_id="ORD-1",
            user_id="USR-1",
            product_id="PRD-1",
            quantity=2,
            amount=Decimal("1800.00"),
            currency="TWD",
            channel="web",
        ),
    )


def test_duplicate_marker_skips_business_insert_and_flush() -> None:
    session = FakeSession(None)

    inserted = OrderRepository().persist(
        session, order_event(), KafkaRecordMetadata("orders.v1", 1, 10), "group-v1"
    )

    assert inserted is False
    assert session.added == []
    assert session.flush_calls == 0


def test_new_event_adds_decimal_order_without_committing() -> None:
    event = order_event()
    session = FakeSession(event.event_id)

    inserted = OrderRepository().persist(
        session, event, KafkaRecordMetadata("orders.v1", 1, 10), "group-v1"
    )

    assert inserted is True
    assert len(session.added) == 1
    stored = session.added[0]
    assert isinstance(stored, ValidOrder)
    assert stored.amount == Decimal("1800.00")
    assert stored.event_time.tzinfo is not None
    assert session.flush_calls == 1
    assert not hasattr(session, "commit")
