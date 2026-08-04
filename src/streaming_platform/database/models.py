"""SQLAlchemy models for the platform's PostgreSQL schema."""

from datetime import datetime
from decimal import Decimal
from uuid import UUID

from sqlalchemy import BigInteger, DateTime, Integer, Numeric, String, func
from sqlalchemy.dialects.postgresql import UUID as PostgreSQLUUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Declarative base for all database models."""


class ValidOrder(Base):
    """A validated order event persisted by the future order consumer."""

    __tablename__ = "valid_orders"

    event_id: Mapped[UUID] = mapped_column(PostgreSQLUUID(as_uuid=True), primary_key=True)
    order_id: Mapped[str] = mapped_column(String(64), index=True)
    event_type: Mapped[str] = mapped_column(String(50))
    user_id: Mapped[str] = mapped_column(String(64))
    product_id: Mapped[str | None] = mapped_column(String(64))
    quantity: Mapped[int | None] = mapped_column(Integer)
    amount: Mapped[Decimal | None] = mapped_column(Numeric(18, 2))
    currency: Mapped[str | None] = mapped_column(String(3))
    channel: Mapped[str | None] = mapped_column(String(20))
    event_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    kafka_topic: Mapped[str] = mapped_column(String(255))
    kafka_partition: Mapped[int] = mapped_column(Integer)
    kafka_offset: Mapped[int] = mapped_column(BigInteger)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class ProcessedEvent(Base):
    """Idempotency marker scoped to a Kafka consumer group."""

    __tablename__ = "processed_events"

    consumer_group: Mapped[str] = mapped_column(String(100), primary_key=True)
    event_id: Mapped[UUID] = mapped_column(PostgreSQLUUID(as_uuid=True), primary_key=True)
    topic: Mapped[str] = mapped_column(String(255))
    partition_id: Mapped[int] = mapped_column(Integer)
    offset_id: Mapped[int] = mapped_column(BigInteger)
    processed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class LogMetricMinute(Base):
    """Per-minute application access-log aggregates."""

    __tablename__ = "log_metrics_minute"

    metric_minute: Mapped[datetime] = mapped_column(DateTime(timezone=True), primary_key=True)
    service: Mapped[str] = mapped_column(String(100), primary_key=True)
    endpoint: Mapped[str] = mapped_column(String(255), primary_key=True)
    request_count: Mapped[int] = mapped_column(BigInteger)
    success_count: Mapped[int] = mapped_column(BigInteger)
    client_error_count: Mapped[int] = mapped_column(BigInteger)
    server_error_count: Mapped[int] = mapped_column(BigInteger)
    response_time_sum_ms: Mapped[int] = mapped_column(BigInteger)
    max_response_time_ms: Mapped[int] = mapped_column(Integer)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )
