import logging
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from sqlalchemy.exc import OperationalError

from streaming_platform.consumer.log import LogFlushService
from streaming_platform.consumer.offsets import ContiguousOffsetTracker
from streaming_platform.consumer.retry import RetryPolicy
from streaming_platform.database.log_repository import LogMetricRepository
from streaming_platform.metrics.log_aggregation import KafkaCoordinate, LogAggregationBuffer
from streaming_platform.models import ApiAccessLogEvent, ApiAccessLogPayload


def event() -> ApiAccessLogEvent:
    return ApiAccessLogEvent(
        event_id=uuid4(),
        event_time=datetime(2026, 8, 5, 10, 1, tzinfo=UTC),
        source="order-api",
        payload=ApiAccessLogPayload(
            request_id="REQ-1",
            service="order-api",
            endpoint="/orders",
            http_method="GET",
            status_code=200,
            response_time_ms=10,
            client_ip="10.0.0.1",
        ),
    )


class TransactionContext:
    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False


class FakeSession:
    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def begin(self):
        return TransactionContext()


class FailingRepository(LogMetricRepository):
    def persist_snapshot(self, _session, _snapshot, _consumer_group):
        raise OperationalError("forced", {}, TimeoutError("offline"))


class NoCommitConsumer:
    def __init__(self):
        self.commits = []

    def commit_offsets(self, offsets):
        self.commits.append(offsets)


def test_database_flush_failure_restores_buffer_and_never_commits() -> None:
    buffer = LogAggregationBuffer()
    coordinate = KafkaCoordinate("logs.v1", 0, 10)
    buffer.add(event(), coordinate)
    offsets = ContiguousOffsetTracker()
    offsets.observe(coordinate)
    consumer = NoCommitConsumer()
    service = LogFlushService(
        buffer,
        offsets,
        lambda: FakeSession(),
        FailingRepository(),
        consumer,
        "log-group",
        RetryPolicy(max_retries=0),
        logging.LoggerAdapter(logging.getLogger("log-repository-test"), {}),
    )

    with pytest.raises(Exception, match="failed after 1 attempts"):
        service.flush()

    assert len(buffer) == 1
    assert consumer.commits == []
    assert offsets.has_pending() is True
