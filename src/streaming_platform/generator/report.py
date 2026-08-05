"""Thread-safe producer counters and JSON report output."""

from bisect import bisect_left
from collections import Counter
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from threading import Lock
from time import monotonic
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from streaming_platform.generator.factory import GeneratedRecord, InjectionKind
from streaming_platform.metrics.statistics import average, nearest_rank_percentile


class DeliveredOffsetRange(BaseModel):
    """One exact contiguous range of broker offsets delivered by a run."""

    model_config = ConfigDict(extra="forbid")

    topic: str
    partition: int
    start_offset: int
    end_offset_exclusive: int


class ProducerReport(BaseModel):
    """Measured output from one generator run."""

    model_config = ConfigDict(extra="forbid")

    report_version: int = 1
    started_at: datetime
    finished_at: datetime
    target_events_per_second: float
    duration_seconds: float
    elapsed_seconds: float
    actual_events_per_second: float
    attempted: int
    delivered: int
    failed: int
    order_events_attempted: int
    log_events_attempted: int
    invalid_events_injected: int
    stale_events_injected: int
    duplicate_events_injected: int
    duplicate_events_delivered: int = 0
    invalid_events_by_type: dict[str, int]
    seed: int
    stale_hours: float
    producer_delivery_latency_sample_count: int = 0
    producer_delivery_latency_average_ms: float | None = None
    producer_delivery_latency_p95_ms: float | None = None
    producer_delivery_latency_p99_ms: float | None = None
    delivered_offset_ranges: list[DeliveredOffsetRange] = Field(default_factory=list)


class DeliveryTracker:
    """Collect attempted and asynchronous delivery results safely."""

    def __init__(self, clock: Callable[[], float] = monotonic) -> None:
        """Initialize empty counters protected for delivery callbacks."""
        self._lock = Lock()
        self._attempted = 0
        self._delivered = 0
        self._failed = 0
        self._pending: set[int] = set()
        self._started: dict[int, float] = {}
        self._token_injection: dict[int, str] = {}
        self._next_token = 0
        self._families: Counter[str] = Counter()
        self._injections: Counter[str] = Counter()
        self._invalid_types: Counter[str] = Counter()
        self._delivered_injections: Counter[str] = Counter()
        self._latencies_ms: list[float] = []
        self._offset_ranges: dict[tuple[str, int], list[tuple[int, int]]] = {}
        self._clock = clock

    def begin_attempt(self, record: GeneratedRecord) -> int:
        """Register an attempted record and return its delivery token."""
        with self._lock:
            token = self._next_token
            self._next_token += 1
            self._attempted += 1
            self._pending.add(token)
            self._started[token] = self._clock()
            self._token_injection[token] = record.injection_kind.value
            self._families[record.family] += 1
            self._injections[record.injection_kind.value] += 1
            if record.invalid_kind is not None:
                self._invalid_types[record.invalid_kind.value] += 1
            return token

    def delivered(
        self,
        token: int,
        *,
        topic: str | None = None,
        partition: int | None = None,
        offset: int | None = None,
    ) -> None:
        """Mark one queued record as delivered."""
        with self._lock:
            if token in self._pending:
                self._pending.remove(token)
                self._delivered += 1
                started = self._started.pop(token)
                self._latencies_ms.append(max((self._clock() - started) * 1000, 0.0))
                injection = self._token_injection.pop(token)
                self._delivered_injections[injection] += 1
                if topic is not None and partition is not None and offset is not None:
                    self._insert_offset((topic, partition), offset)

    def failed(self, token: int) -> None:
        """Mark one attempted record as failed."""
        with self._lock:
            if token in self._pending:
                self._pending.remove(token)
                self._started.pop(token, None)
                self._token_injection.pop(token, None)
                self._failed += 1

    def fail_pending(self) -> int:
        """Convert all records left after flush into delivery failures."""
        with self._lock:
            pending = len(self._pending)
            self._failed += pending
            self._pending.clear()
            self._started.clear()
            self._token_injection.clear()
            return pending

    def snapshot(self) -> dict[str, Any]:
        """Return a consistent copy of the current counters."""
        with self._lock:
            return {
                "attempted": self._attempted,
                "delivered": self._delivered,
                "failed": self._failed,
                "pending": len(self._pending),
                "order_events_attempted": self._families["order"],
                "log_events_attempted": self._families["log"],
                "invalid_events_injected": self._injections[InjectionKind.INVALID.value],
                "stale_events_injected": self._injections[InjectionKind.STALE.value],
                "duplicate_events_injected": self._injections[InjectionKind.DUPLICATE.value],
                "duplicate_events_delivered": self._delivered_injections[
                    InjectionKind.DUPLICATE.value
                ],
                "invalid_events_by_type": dict(self._invalid_types),
                "producer_delivery_latency_sample_count": len(self._latencies_ms),
                "producer_delivery_latency_average_ms": average(self._latencies_ms),
                "producer_delivery_latency_p95_ms": nearest_rank_percentile(
                    self._latencies_ms, 0.95
                ),
                "producer_delivery_latency_p99_ms": nearest_rank_percentile(
                    self._latencies_ms, 0.99
                ),
                "delivered_offset_ranges": [
                    {
                        "topic": topic,
                        "partition": partition,
                        "start_offset": start,
                        "end_offset_exclusive": end,
                    }
                    for (topic, partition), ranges in sorted(self._offset_ranges.items())
                    for start, end in ranges
                ],
            }

    def _insert_offset(self, key: tuple[str, int], offset: int) -> None:
        ranges = self._offset_ranges.setdefault(key, [])
        index = bisect_left(ranges, (offset, offset + 1))
        start, end = offset, offset + 1
        if index and ranges[index - 1][1] >= start:
            index -= 1
            start = min(start, ranges[index][0])
            end = max(end, ranges[index][1])
            ranges.pop(index)
        while index < len(ranges) and ranges[index][0] <= end:
            start = min(start, ranges[index][0])
            end = max(end, ranges[index][1])
            ranges.pop(index)
        ranges.insert(index, (start, end))


def write_report(report: ProducerReport, path: Path) -> None:
    """Write a measured report as indented UTF-8 JSON."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(report.model_dump_json(indent=2) + "\n", encoding="utf-8")
