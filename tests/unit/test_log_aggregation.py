from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal
from uuid import UUID, uuid4

import pytest

from streaming_platform.metrics.log_aggregation import (
    AggregationKey,
    KafkaCoordinate,
    LogAggregationBuffer,
    contribution_for,
    utc_minute,
)
from streaming_platform.models import ApiAccessLogEvent, ApiAccessLogPayload


def access_event(
    *,
    event_id: UUID | None = None,
    event_time: datetime | None = None,
    service: str = "order-api",
    endpoint: str = "/orders",
    status: int = 200,
    response_ms: int = 10,
) -> ApiAccessLogEvent:
    return ApiAccessLogEvent(
        event_id=event_id or uuid4(),
        event_time=event_time or datetime(2026, 8, 5, 10, 1, 30, 123, tzinfo=UTC),
        source=service,
        payload=ApiAccessLogPayload(
            request_id=f"REQ-{uuid4()}",
            service=service,
            endpoint=endpoint,
            http_method="GET",
            status_code=status,
            response_time_ms=response_ms,
            client_ip="10.0.0.1",
        ),
    )


def test_utc_minute_normalizes_and_truncates() -> None:
    local = datetime(2026, 8, 5, 18, 1, 59, 999, tzinfo=timezone(timedelta(hours=8)))
    assert utc_minute(local) == datetime(2026, 8, 5, 10, 1, tzinfo=UTC)
    with pytest.raises(ValueError, match="timezone-aware"):
        utc_minute(datetime(2026, 8, 5, 10, 1))


@pytest.mark.parametrize(
    "status, expected",
    [
        (100, (1, 0, 0)),
        (399, (1, 0, 0)),
        (400, (0, 1, 0)),
        (499, (0, 1, 0)),
        (500, (0, 0, 1)),
        (599, (0, 0, 1)),
    ],
)
def test_http_status_categories(status: int, expected: tuple[int, int, int]) -> None:
    _key, metric = contribution_for(access_event(status=status, response_ms=135))
    assert (
        metric.success_count,
        metric.client_error_count,
        metric.server_error_count,
    ) == expected
    assert metric.request_count == 1
    assert metric.response_time_sum_ms == metric.max_response_time_ms == 135
    assert metric.average_response_time_ms == Decimal(135)


def test_multiple_events_aggregate_and_separate_keys_and_late_minute() -> None:
    buffer = LogAggregationBuffer()
    first = access_event(status=200, response_ms=10)
    second = access_event(status=500, response_ms=30)
    other = access_event(service="payment-api", endpoint="/payments", response_ms=7)
    late = access_event(event_time=datetime(2026, 8, 5, 9, 55, 45, tzinfo=UTC))
    for offset, event in enumerate((first, second, other, late)):
        buffer.add(event, KafkaCoordinate("logs.v1", 0, offset))

    aggregates = buffer.take_snapshot().aggregate()
    current_key = AggregationKey(datetime(2026, 8, 5, 10, 1, tzinfo=UTC), "order-api", "/orders")
    current = aggregates[current_key]
    assert current.request_count == 2
    assert current.success_count == 1
    assert current.server_error_count == 1
    assert current.response_time_sum_ms == 40
    assert current.max_response_time_ms == 30
    assert current.average_response_time_ms == Decimal(20)
    assert len(aggregates) == 3
    assert AggregationKey(
        datetime(2026, 8, 5, 9, 55, tzinfo=UTC), "order-api", "/orders"
    ) in aggregates


def test_duplicate_event_is_counted_once_but_retains_both_offsets() -> None:
    buffer = LogAggregationBuffer()
    event = access_event()
    assert buffer.add(event, KafkaCoordinate("logs.v1", 0, 10)) is True
    assert buffer.add(event, KafkaCoordinate("logs.v1", 0, 11)) is False
    snapshot = buffer.take_snapshot()

    assert snapshot.aggregate()[contribution_for(event)[0]].request_count == 1
    assert {coordinate.offset for coordinate in snapshot.coordinates} == {10, 11}


def test_snapshot_swap_and_restore_preserve_new_events() -> None:
    buffer = LogAggregationBuffer()
    first = access_event()
    second = access_event(endpoint="/payments")
    buffer.add(first, KafkaCoordinate("logs.v1", 0, 1))
    failed_snapshot = buffer.take_snapshot()
    buffer.add(second, KafkaCoordinate("logs.v1", 1, 2))
    buffer.restore(failed_snapshot)

    restored = buffer.take_snapshot()
    assert restored.event_ids == {first.event_id, second.event_id}
    assert len(buffer) == 0
