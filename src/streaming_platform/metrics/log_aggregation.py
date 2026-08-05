"""Thread-safe event-time minute aggregation for API access logs."""

from dataclasses import dataclass, replace
from datetime import UTC, datetime
from decimal import Decimal
from threading import Lock
from uuid import UUID

from streaming_platform.models import ApiAccessLogEvent


@dataclass(frozen=True, slots=True)
class KafkaCoordinate:
    """The source coordinate of one Kafka record."""

    topic: str
    partition: int
    offset: int


@dataclass(frozen=True, slots=True)
class AggregationKey:
    """Database key for one service endpoint and UTC event-time minute."""

    metric_minute: datetime
    service: str
    endpoint: str


@dataclass(frozen=True, slots=True)
class MetricAggregate:
    """Additive metrics for one aggregation key."""

    request_count: int = 0
    success_count: int = 0
    client_error_count: int = 0
    server_error_count: int = 0
    response_time_sum_ms: int = 0
    max_response_time_ms: int = 0

    @property
    def average_response_time_ms(self) -> Decimal:
        """Return the exact derived average without a database column."""
        if self.request_count == 0:
            return Decimal(0)
        return Decimal(self.response_time_sum_ms) / Decimal(self.request_count)

    def combine(self, other: MetricAggregate) -> MetricAggregate:
        """Return the additive combination of two metric deltas."""
        return MetricAggregate(
            request_count=self.request_count + other.request_count,
            success_count=self.success_count + other.success_count,
            client_error_count=self.client_error_count + other.client_error_count,
            server_error_count=self.server_error_count + other.server_error_count,
            response_time_sum_ms=self.response_time_sum_ms + other.response_time_sum_ms,
            max_response_time_ms=max(
                self.max_response_time_ms, other.max_response_time_ms
            ),
        )


@dataclass(frozen=True, slots=True)
class BufferedLogEvent:
    """One unique event contribution with every source replay coordinate."""

    event_id: UUID
    key: AggregationKey
    delta: MetricAggregate
    coordinates: tuple[KafkaCoordinate, ...]


@dataclass(frozen=True, slots=True)
class AggregationSnapshot:
    """An immutable group of buffered unique events awaiting durable flush."""

    events: tuple[BufferedLogEvent, ...] = ()

    @property
    def event_ids(self) -> set[UUID]:
        """Return unique event identifiers in this snapshot."""
        return {event.event_id for event in self.events}

    @property
    def coordinates(self) -> tuple[KafkaCoordinate, ...]:
        """Return all source offsets represented by the snapshot."""
        return tuple(coordinate for event in self.events for coordinate in event.coordinates)

    def aggregate(
        self, included_event_ids: set[UUID] | None = None
    ) -> dict[AggregationKey, MetricAggregate]:
        """Aggregate all events or only identifiers inserted by the DB transaction."""
        result: dict[AggregationKey, MetricAggregate] = {}
        for event in self.events:
            if included_event_ids is not None and event.event_id not in included_event_ids:
                continue
            current = result.get(event.key, MetricAggregate())
            result[event.key] = current.combine(event.delta)
        return result


def utc_minute(value: datetime) -> datetime:
    """Normalize an aware datetime to UTC and truncate seconds and microseconds."""
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("metric timestamp must be timezone-aware")
    return value.astimezone(UTC).replace(second=0, microsecond=0)


def contribution_for(event: ApiAccessLogEvent) -> tuple[AggregationKey, MetricAggregate]:
    """Build an event-time key and one-request HTTP status contribution."""
    status = event.payload.status_code
    key = AggregationKey(
        metric_minute=utc_minute(event.event_time),
        service=event.payload.service,
        endpoint=event.payload.endpoint,
    )
    return key, MetricAggregate(
        request_count=1,
        success_count=int(100 <= status <= 399),
        client_error_count=int(400 <= status <= 499),
        server_error_count=int(500 <= status <= 599),
        response_time_sum_ms=event.payload.response_time_ms,
        max_response_time_ms=event.payload.response_time_ms,
    )


class LogAggregationBuffer:
    """Deduplicate and snapshot access-log contributions under a small lock."""

    def __init__(self) -> None:
        """Initialize an empty active event map."""
        self._lock = Lock()
        self._events: dict[UUID, BufferedLogEvent] = {}

    def add(self, event: ApiAccessLogEvent, coordinate: KafkaCoordinate) -> bool:
        """Add a contribution; return false for a duplicate event identifier."""
        key, delta = contribution_for(event)
        incoming = BufferedLogEvent(event.event_id, key, delta, (coordinate,))
        with self._lock:
            existing = self._events.get(event.event_id)
            if existing is None:
                self._events[event.event_id] = incoming
                return True
            self._events[event.event_id] = self._merge_event(existing, incoming)
            return False

    def take_snapshot(self) -> AggregationSnapshot:
        """Atomically swap the active map and return the previous contents."""
        with self._lock:
            previous, self._events = self._events, {}
        return AggregationSnapshot(tuple(previous.values()))

    def restore(self, snapshot: AggregationSnapshot) -> None:
        """Merge a failed snapshot back without losing events added meanwhile."""
        with self._lock:
            for incoming in snapshot.events:
                existing = self._events.get(incoming.event_id)
                self._events[incoming.event_id] = (
                    incoming if existing is None else self._merge_event(existing, incoming)
                )

    def __len__(self) -> int:
        """Return the number of unique event identifiers currently buffered."""
        with self._lock:
            return len(self._events)

    @staticmethod
    def _merge_event(first: BufferedLogEvent, second: BufferedLogEvent) -> BufferedLogEvent:
        if first.key != second.key or first.delta != second.delta:
            raise ValueError(f"event_id {first.event_id} was reused with different metric data")
        coordinates = tuple(dict.fromkeys(first.coordinates + second.coordinates))
        return replace(first, coordinates=coordinates)
