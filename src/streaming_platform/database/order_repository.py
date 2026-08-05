"""Transactional persistence for validated order events."""

from dataclasses import dataclass

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from streaming_platform.database.models import ProcessedEvent, ValidOrder
from streaming_platform.models import OrderCreatedEvent, OrderEvent


@dataclass(frozen=True, slots=True)
class KafkaRecordMetadata:
    """Kafka coordinates stored with a processed business event."""

    topic: str
    partition: int
    offset: int


class OrderRepository:
    """Write idempotency and order rows without owning transaction commits."""

    def persist(
        self,
        session: Session,
        event: OrderEvent,
        metadata: KafkaRecordMetadata,
        consumer_group: str,
    ) -> bool:
        """Persist one event and return false when its idempotency marker exists."""
        marker = (
            insert(ProcessedEvent)
            .values(
                consumer_group=consumer_group,
                event_id=event.event_id,
                topic=metadata.topic,
                partition_id=metadata.partition,
                offset_id=metadata.offset,
            )
            .on_conflict_do_nothing(index_elements=["consumer_group", "event_id"])
            .returning(ProcessedEvent.event_id)
        )
        inserted_event_id = session.execute(marker).scalar_one_or_none()
        if inserted_event_id is None:
            return False

        payload = event.payload
        if isinstance(event, OrderCreatedEvent):
            product_id = event.payload.product_id
            quantity = event.payload.quantity
            channel = event.payload.channel.value
        else:
            product_id = None
            quantity = None
            channel = None
        session.add(
            ValidOrder(
                event_id=event.event_id,
                order_id=payload.order_id,
                event_type=event.event_type.value,
                user_id=payload.user_id,
                product_id=product_id,
                quantity=quantity,
                amount=getattr(payload, "amount", None),
                currency=(payload.currency.value if hasattr(payload, "currency") else None),
                channel=channel,
                event_time=event.event_time,
                kafka_topic=metadata.topic,
                kafka_partition=metadata.partition,
                kafka_offset=metadata.offset,
            )
        )
        session.flush()
        return True
