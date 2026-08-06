"""Executable Phase 4 flow using real Kafka and PostgreSQL services."""

import json
import os
import signal
import subprocess
import sys
from collections.abc import Callable
from datetime import UTC, datetime
from time import monotonic, sleep
from uuid import uuid4

import pytest
from confluent_kafka import Consumer, TopicPartition
from sqlalchemy import delete, select

from streaming_platform.config import Settings, get_settings
from streaming_platform.database.models import LogMetricMinute, ProcessedEvent
from streaming_platform.database.session import create_database_engine, create_session_factory
from streaming_platform.models import ApiAccessLogEvent, ApiAccessLogPayload
from tests.integration.kafka_helpers import ensure_test_infrastructure, ensure_test_topics_ready
from tests.integration.test_log_consumer import produce, tail_dlq

pytestmark = pytest.mark.e2e


def wait_until(predicate: Callable[[], bool], description: str, timeout: float = 20) -> None:
    deadline = monotonic() + timeout
    while monotonic() < deadline:
        if predicate():
            return
        sleep(0.2)
    raise TimeoutError(f"timed out waiting for {description}")


def reset_group_to_tail(settings: Settings) -> None:
    consumer = Consumer(
        {
            "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
            "group.id": settings.KAFKA_LOG_CONSUMER_GROUP,
            "enable.auto.commit": False,
        }
    )
    try:
        metadata = consumer.list_topics(settings.KAFKA_LOG_TOPIC, timeout=10)
        positions = []
        for partition_id in metadata.topics[settings.KAFKA_LOG_TOPIC].partitions:
            partition = TopicPartition(settings.KAFKA_LOG_TOPIC, partition_id)
            _low, high = consumer.get_watermark_offsets(partition, timeout=10)
            positions.append(TopicPartition(settings.KAFKA_LOG_TOPIC, partition_id, high))
        consumer.assign(positions)
        consumer.commit(offsets=positions, asynchronous=False)
    finally:
        consumer.close()


def access_event(
    *,
    event_id=None,
    event_time: datetime,
    status: int = 200,
    response_ms: int = 10,
) -> ApiAccessLogEvent:
    return ApiAccessLogEvent(
        event_id=event_id or uuid4(),
        event_time=event_time,
        source="order-api",
        payload=ApiAccessLogPayload(
            request_id=f"REQ-{uuid4()}",
            service="order-api",
            endpoint="/orders",
            http_method="GET",
            status_code=status,
            response_time_ms=response_ms,
            client_ip="10.0.0.1",
        ),
    )


def start_consumer(settings: Settings, flush_interval: float) -> subprocess.Popen[str]:
    environment = os.environ.copy()
    environment["LOG_CONSUMER_FLUSH_INTERVAL_SECONDS"] = str(flush_interval)
    environment["KAFKA_LOG_TOPIC"] = settings.KAFKA_LOG_TOPIC
    environment["KAFKA_DLQ_TOPIC"] = settings.KAFKA_DLQ_TOPIC
    environment["KAFKA_LOG_CONSUMER_GROUP"] = settings.KAFKA_LOG_CONSUMER_GROUP
    return subprocess.Popen(
        [sys.executable, "-m", "apps.log_consumer"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        env=environment,
    )


def stop_consumer(process: subprocess.Popen[str]) -> str:
    process.send_signal(signal.SIGTERM)
    output, _ = process.communicate(timeout=20)
    assert process.returncode == 143, output
    return output


def rows(engine) -> list[LogMetricMinute]:
    with create_session_factory(engine)() as session:
        return list(session.scalars(select(LogMetricMinute)).all())


def committed_offset(settings: Settings, position: TopicPartition) -> int:
    observer = Consumer(
        {
            "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
            "group.id": settings.KAFKA_LOG_CONSUMER_GROUP,
            "enable.auto.commit": False,
        }
    )
    try:
        return observer.committed([position], timeout=10)[0].offset
    finally:
        observer.close()


def test_log_consumer_shutdown_flush_restart_and_continued_processing() -> None:
    ensure_test_infrastructure()
    get_settings.cache_clear()
    suffix = uuid4().hex
    settings = get_settings().model_copy(
        update={
            "KAFKA_ORDER_TOPIC": f"order-e2e-unused.{suffix}.v1",
            "KAFKA_LOG_TOPIC": f"log-e2e.{suffix}.v1",
            "KAFKA_DLQ_TOPIC": f"dlq-log-e2e.{suffix}.v1",
            "KAFKA_LOG_CONSUMER_GROUP": f"log-e2e-{suffix}",
        }
    )
    ensure_test_topics_ready(settings)
    reset_group_to_tail(settings)
    engine = create_database_engine(settings)
    with engine.begin() as connection:
        connection.execute(delete(LogMetricMinute))
        connection.execute(
            delete(ProcessedEvent).where(
                ProcessedEvent.consumer_group == settings.KAFKA_LOG_CONSUMER_GROUP
            )
        )

    current_minute = datetime(2026, 8, 5, 10, 1, 30, tzinfo=UTC)
    late_minute = datetime(2026, 8, 5, 9, 55, 45, tzinfo=UTC)
    normal = access_event(event_time=current_minute, status=200, response_ms=10)
    server_error = access_event(event_time=current_minute, status=500, response_ms=30)
    late = access_event(event_time=late_minute, response_ms=5)
    invalid = json.loads(normal.model_dump_json())
    invalid["event_id"] = str(uuid4())
    invalid_event_id = invalid["event_id"]
    invalid["payload"]["status_code"] = 700
    dlq_observer = tail_dlq(settings)
    process = start_consumer(settings, flush_interval=60)
    positions = []
    try:
        for value in (
            normal.model_dump_json().encode(),
            server_error.model_dump_json().encode(),
            late.model_dump_json().encode(),
            normal.model_dump_json().encode(),
            json.dumps(invalid).encode(),
        ):
            positions.append(produce(settings, value, partition=0))

        dlq_body = None
        deadline = monotonic() + 15
        while dlq_body is None and monotonic() < deadline:
            message = dlq_observer.poll(0.25)
            if message is not None and message.error() is None:
                candidate = json.loads(message.value())
                payload = candidate.get("original_payload")
                if (
                    candidate.get("original_topic") == positions[-1].topic
                    and candidate.get("original_partition") == positions[-1].partition
                    and candidate.get("original_offset") == positions[-1].offset
                    and isinstance(payload, dict)
                    and payload.get("event_id") == invalid_event_id
                ):
                    dlq_body = candidate
        assert dlq_body is not None
        assert rows(engine) == []
        stop_consumer(process)
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=10)
        dlq_observer.close()

    persisted = rows(engine)
    assert len(persisted) == 2
    current = next(row for row in persisted if row.metric_minute.hour == 10)
    late_row = next(row for row in persisted if row.metric_minute.hour == 9)
    assert (current.request_count, current.success_count, current.server_error_count) == (2, 1, 1)
    assert (current.response_time_sum_ms, current.max_response_time_ms) == (40, 30)
    assert late_row.request_count == 1
    assert committed_offset(settings, positions[-1]) == positions[-1].offset + 1

    follow_up = access_event(event_time=current_minute, status=404, response_ms=20)
    restarted = start_consumer(settings, flush_interval=0.5)
    try:
        follow_up_position = produce(settings, follow_up.model_dump_json().encode(), partition=0)
        wait_until(
            lambda: next(row for row in rows(engine) if row.metric_minute.hour == 10).request_count
            == 3,
            "follow-up log aggregation",
        )
        wait_until(
            lambda: committed_offset(settings, follow_up_position) == follow_up_position.offset + 1,
            "follow-up offset commit",
        )
        stop_consumer(restarted)
    finally:
        if restarted.poll() is None:
            restarted.kill()
            restarted.wait(timeout=10)
        engine.dispose()
