"""Thread-safe producer counters and JSON report output."""

from collections import Counter
from datetime import datetime
from pathlib import Path
from threading import Lock
from typing import Any

from pydantic import BaseModel, ConfigDict

from streaming_platform.generator.factory import GeneratedRecord, InjectionKind


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
    invalid_events_by_type: dict[str, int]
    seed: int
    stale_hours: float


class DeliveryTracker:
    """Collect attempted and asynchronous delivery results safely."""

    def __init__(self) -> None:
        """Initialize empty counters protected for delivery callbacks."""
        self._lock = Lock()
        self._attempted = 0
        self._delivered = 0
        self._failed = 0
        self._pending: set[int] = set()
        self._next_token = 0
        self._families: Counter[str] = Counter()
        self._injections: Counter[str] = Counter()
        self._invalid_types: Counter[str] = Counter()

    def begin_attempt(self, record: GeneratedRecord) -> int:
        """Register an attempted record and return its delivery token."""
        with self._lock:
            token = self._next_token
            self._next_token += 1
            self._attempted += 1
            self._pending.add(token)
            self._families[record.family] += 1
            self._injections[record.injection_kind.value] += 1
            if record.invalid_kind is not None:
                self._invalid_types[record.invalid_kind.value] += 1
            return token

    def delivered(self, token: int) -> None:
        """Mark one queued record as delivered."""
        with self._lock:
            if token in self._pending:
                self._pending.remove(token)
                self._delivered += 1

    def failed(self, token: int) -> None:
        """Mark one attempted record as failed."""
        with self._lock:
            if token in self._pending:
                self._pending.remove(token)
                self._failed += 1

    def fail_pending(self) -> int:
        """Convert all records left after flush into delivery failures."""
        with self._lock:
            pending = len(self._pending)
            self._failed += pending
            self._pending.clear()
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
                "invalid_events_by_type": dict(self._invalid_types),
            }


def write_report(report: ProducerReport, path: Path) -> None:
    """Write a measured report as indented UTF-8 JSON."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(report.model_dump_json(indent=2) + "\n", encoding="utf-8")
