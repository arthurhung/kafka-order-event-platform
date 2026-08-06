"""Deterministic Phase 6 fixtures delivered through the existing Kafka Core."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from time import monotonic, sleep
from typing import Literal
from uuid import NAMESPACE_URL, UUID, uuid5

from confluent_kafka import Producer
from sqlalchemy import func, select

from streaming_platform.config import Settings
from streaming_platform.database.models import LogMetricMinute, ProcessedEvent, ValidOrder
from streaming_platform.database.session import create_database_engine, create_session_factory
from streaming_platform.models import (
    ApiAccessLogEvent,
    ApiAccessLogPayload,
    OrderCancelledEvent,
    OrderCancelledPayload,
    OrderCreatedEvent,
    OrderCreatedPayload,
    OrderPaidEvent,
    OrderPaidPayload,
    PaymentFailedEvent,
    PaymentFailedPayload,
)


@dataclass(frozen=True, slots=True)
class FixtureRecord:
    """One serialized fixture record and its expected consumer group."""

    event_id: UUID
    topic: str
    key: bytes
    value: bytes
    consumer_group: str
    family: Literal["order", "log"]


@dataclass(frozen=True, slots=True)
class FixtureBundle:
    """All deterministic records for one fixture run."""

    run_id: str
    generated_at: datetime
    records: tuple[FixtureRecord, ...]

    @property
    def order_event_ids(self) -> set[UUID]:
        """Return order event IDs in the bundle."""
        return {
            record.event_id
            for record in self.records
            if record.family == "order"
        }

    @property
    def log_event_ids(self) -> set[UUID]:
        """Return access-log event IDs in the bundle."""
        return {
            record.event_id
            for record in self.records
            if record.family == "log"
        }


def _identifier(run_id: str, name: str) -> UUID:
    return uuid5(NAMESPACE_URL, f"kafka-order-event-platform/phase6/{run_id}/{name}")


def _order_record(
    settings: Settings,
    event: OrderCreatedEvent | OrderPaidEvent | PaymentFailedEvent | OrderCancelledEvent,
) -> FixtureRecord:
    return FixtureRecord(
        event_id=event.event_id,
        topic=settings.KAFKA_ORDER_TOPIC,
        key=event.payload.order_id.encode(),
        value=event.model_dump_json().encode(),
        consumer_group=settings.KAFKA_ORDER_CONSUMER_GROUP,
        family="order",
    )


def _log_record(settings: Settings, event: ApiAccessLogEvent) -> FixtureRecord:
    return FixtureRecord(
        event_id=event.event_id,
        topic=settings.KAFKA_LOG_TOPIC,
        key=event.payload.service.encode(),
        value=event.model_dump_json().encode(),
        consumer_group=settings.KAFKA_LOG_CONSUMER_GROUP,
        family="log",
    )


def build_fixture_bundle(settings: Settings, run_id: str, now: datetime) -> FixtureBundle:
    """Build reproducible lifecycle and service-health records for one run ID."""
    if now.tzinfo is None:
        raise ValueError("fixture timestamp must be timezone-aware")
    base = now.astimezone(UTC).replace(second=0, microsecond=0)
    records: list[FixtureRecord] = []
    scenarios = (
        ("paid", "TWD", "web", ("created", "paid")),
        ("retry", "USD", "ios", ("created", "failed", "paid")),
        ("cancelled", "TWD", "android", ("created", "cancelled")),
        ("paid-cancelled", "USD", "web", ("created", "paid", "cancelled")),
    )
    sequence = 0
    for scenario, currency, channel, lifecycle in scenarios:
        order_id = f"DP6-{run_id}-{scenario}"[:64]
        user_id = f"DP6-USR-{scenario}"[:64]
        amount = Decimal("1200.00") if currency == "TWD" else Decimal("42.50")
        for state in lifecycle:
            event_time = base + timedelta(seconds=sequence)
            name = f"{scenario}-{state}-{sequence}"
            order_event: (
                OrderCreatedEvent
                | OrderPaidEvent
                | PaymentFailedEvent
                | OrderCancelledEvent
            )
            if state == "created":
                order_event = OrderCreatedEvent(
                    event_id=_identifier(run_id, name),
                    event_time=event_time,
                    source="phase6-fixture",
                    payload=OrderCreatedPayload(
                        order_id=order_id,
                        user_id=user_id,
                        product_id=f"DP6-PRD-{scenario}"[:64],
                        quantity=sequence % 3 + 1,
                        amount=amount,
                        currency=currency,
                        channel=channel,
                    ),
                )
            elif state == "paid":
                order_event = OrderPaidEvent(
                    event_id=_identifier(run_id, name),
                    event_time=event_time,
                    source="phase6-fixture",
                    payload=OrderPaidPayload(
                        order_id=order_id,
                        user_id=user_id,
                        payment_id=f"DP6-PAY-{scenario}-{sequence}",
                        amount=amount,
                        currency=currency,
                        payment_method="fixture",
                    ),
                )
            elif state == "failed":
                order_event = PaymentFailedEvent(
                    event_id=_identifier(run_id, name),
                    event_time=event_time,
                    source="phase6-fixture",
                    payload=PaymentFailedPayload(
                        order_id=order_id,
                        user_id=user_id,
                        payment_id=f"DP6-PAY-{scenario}-{sequence}",
                        amount=amount,
                        currency=currency,
                        failure_code="FIXTURE_RETRY",
                        failure_reason="deterministic fixture",
                    ),
                )
            else:
                order_event = OrderCancelledEvent(
                    event_id=_identifier(run_id, name),
                    event_time=event_time,
                    source="phase6-fixture",
                    payload=OrderCancelledPayload(
                        order_id=order_id,
                        user_id=user_id,
                        cancellation_reason="deterministic fixture",
                    ),
                )
            records.append(_order_record(settings, order_event))
            sequence += 1

    log_cases = (
        ("order-api", "/orders", 200, 100),
        ("order-api", "/orders", 404, 200),
        ("order-api", "/orders/detail", 201, 50),
        ("payment-api", "/payments", 500, 300),
        ("payment-api", "/payments", 200, 50),
    )
    for index, (service, endpoint, status, response_ms) in enumerate(log_cases):
        log_event = ApiAccessLogEvent(
            event_id=_identifier(run_id, f"log-{index}"),
            event_time=base + timedelta(seconds=30 + index),
            source="phase6-fixture",
            payload=ApiAccessLogPayload(
                request_id=f"DP6-REQ-{run_id}-{index}",
                service=service,
                endpoint=endpoint,
                http_method="GET",
                status_code=status,
                response_time_ms=response_ms,
                client_ip="127.0.0.1",
            ),
        )
        records.append(_log_record(settings, log_event))
    return FixtureBundle(run_id=run_id, generated_at=base, records=tuple(records))


def _start_consumers() -> list[subprocess.Popen[bytes]]:
    environment = os.environ.copy()
    environment["LOG_CONSUMER_FLUSH_INTERVAL_SECONDS"] = "0.5"
    return [
        subprocess.Popen(  # noqa: S603 - fixed interpreter and allowlisted modules
            [sys.executable, "-m", module],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.STDOUT,
            env=environment,
        )
        for module in ("apps.order_consumer", "apps.log_consumer")
    ]


def _publish(settings: Settings, bundle: FixtureBundle) -> None:
    errors: list[str] = []

    def delivered(error: object, _message: object) -> None:
        if error is not None:
            errors.append(str(error))

    producer = Producer({"bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS})
    for record in bundle.records:
        producer.produce(
            record.topic,
            key=record.key,
            value=record.value,
            on_delivery=delivered,
        )
        producer.poll(0)
    remaining = producer.flush(15)
    if remaining or errors:
        raise RuntimeError(errors[0] if errors else f"{remaining} fixture records undelivered")


def _pending_bundle(settings: Settings, bundle: FixtureBundle) -> FixtureBundle:
    """Return only records not already persisted for their consumer group."""
    engine = create_database_engine(settings)
    try:
        with create_session_factory(engine)() as session:
            existing = set(
                session.execute(
                    select(ProcessedEvent.consumer_group, ProcessedEvent.event_id).where(
                        ProcessedEvent.event_id.in_(
                            record.event_id for record in bundle.records
                        )
                    )
                ).tuples()
            )
    finally:
        engine.dispose()
    return FixtureBundle(
        run_id=bundle.run_id,
        generated_at=bundle.generated_at,
        records=tuple(
            record
            for record in bundle.records
            if (record.consumer_group, record.event_id) not in existing
        ),
    )


def _wait_for_persistence(settings: Settings, bundle: FixtureBundle, timeout: float = 30) -> None:
    expected = {record.event_id for record in bundle.records}
    engine = create_database_engine(settings)
    deadline = monotonic() + timeout
    try:
        while monotonic() < deadline:
            with create_session_factory(engine)() as session:
                observed = set(
                    session.scalars(
                        select(ProcessedEvent.event_id).where(ProcessedEvent.event_id.in_(expected))
                    )
                )
            if observed == expected:
                return
            sleep(0.2)
    finally:
        engine.dispose()
    raise TimeoutError(f"only {len(observed)} of {len(expected)} fixture events persisted")


def _stop_consumers(processes: list[subprocess.Popen[bytes]]) -> None:
    for process in processes:
        if process.poll() is None:
            process.terminate()
    for process in processes:
        try:
            process.wait(timeout=15)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)


def load_fixtures(settings: Settings, run_id: str, report_path: Path) -> dict[str, object]:
    """Publish one fixture bundle, wait for durable writes, and write a summary."""
    bundle = build_fixture_bundle(settings, run_id, datetime.now(UTC))
    pending = _pending_bundle(settings, bundle)
    if pending.records:
        processes = _start_consumers()
        try:
            _publish(settings, pending)
            _wait_for_persistence(settings, bundle)
        finally:
            _stop_consumers(processes)

    engine = create_database_engine(settings)
    try:
        with create_session_factory(engine)() as session:
            counts = {
                "valid_orders": session.scalar(select(func.count()).select_from(ValidOrder)) or 0,
                "processed_events": (
                    session.scalar(select(func.count()).select_from(ProcessedEvent)) or 0
                ),
                "log_metrics_minute": (
                    session.scalar(select(func.count()).select_from(LogMetricMinute)) or 0
                ),
            }
    finally:
        engine.dispose()
    summary: dict[str, object] = {
        "report_type": "phase6_fixtures",
        "schema_version": 1,
        "run_id": run_id,
        "generated_at": bundle.generated_at.isoformat().replace("+00:00", "Z"),
        "order_event_count": len(bundle.order_event_ids),
        "log_event_count": len(bundle.log_event_ids),
        "published_event_count": len(pending.records),
        "existing_event_count": len(bundle.records) - len(pending.records),
        "source_row_counts": counts,
        "request_count_zero_fixture": "dbt_unit_test_only",
        "status": "passed",
    }
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return summary
