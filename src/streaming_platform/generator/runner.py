"""Paced generator orchestration independent from the command-line interface."""

import logging
from collections.abc import Callable
from datetime import UTC, datetime
from time import monotonic, sleep

from streaming_platform.generator.factory import EventFactory
from streaming_platform.generator.options import GeneratorOptions
from streaming_platform.generator.report import DeliveryTracker, ProducerReport, write_report
from streaming_platform.kafka.producer import TrackedKafkaProducer


class GeneratorRunner:
    """Generate a configured number of records on a monotonic schedule."""

    def __init__(
        self,
        options: GeneratorOptions,
        factory: EventFactory,
        producer: TrackedKafkaProducer,
        tracker: DeliveryTracker,
        logger: logging.LoggerAdapter[logging.Logger],
        clock: Callable[[], datetime] | None = None,
        monotonic_clock: Callable[[], float] = monotonic,
        sleeper: Callable[[float], None] = sleep,
    ) -> None:
        """Store orchestration dependencies and injectable clocks."""
        self._options = options
        self._factory = factory
        self._producer = producer
        self._tracker = tracker
        self._logger = logger
        self._clock = clock or (lambda: datetime.now(UTC))
        self._monotonic = monotonic_clock
        self._sleeper = sleeper

    def run(self) -> ProducerReport:
        """Run generation, always flush, and write the measured JSON report."""
        started_at = self._clock()
        started_monotonic = self._monotonic()
        next_progress = started_monotonic + 5.0
        try:
            for index in range(self._options.planned_events):
                scheduled = started_monotonic + ((index + 1) / self._options.events_per_second)
                delay = scheduled - self._monotonic()
                if delay > 0:
                    self._sleeper(delay)
                self._producer.send(self._factory.next_record(self._options))
                if self._monotonic() >= next_progress:
                    self._log_progress()
                    next_progress += 5.0
        finally:
            self._producer.flush()
            finished_at = self._clock()
            elapsed = max(self._monotonic() - started_monotonic, 1e-9)
            report = self._build_report(started_at, finished_at, elapsed)
            write_report(report, self._options.report_path)
        return report

    def _log_progress(self) -> None:
        snapshot = self._tracker.snapshot()
        self._logger.info(
            "event generation progress",
            extra={
                "attempted": snapshot["attempted"],
                "delivered": snapshot["delivered"],
                "failed": snapshot["failed"],
            },
        )

    def _build_report(
        self,
        started_at: datetime,
        finished_at: datetime,
        elapsed: float,
    ) -> ProducerReport:
        snapshot = self._tracker.snapshot()
        delivered = int(snapshot["delivered"])
        return ProducerReport(
            started_at=started_at,
            finished_at=finished_at,
            target_events_per_second=self._options.events_per_second,
            duration_seconds=self._options.duration_seconds,
            elapsed_seconds=elapsed,
            actual_events_per_second=delivered / elapsed,
            attempted=int(snapshot["attempted"]),
            delivered=delivered,
            failed=int(snapshot["failed"]),
            order_events_attempted=int(snapshot["order_events_attempted"]),
            log_events_attempted=int(snapshot["log_events_attempted"]),
            invalid_events_injected=int(snapshot["invalid_events_injected"]),
            stale_events_injected=int(snapshot["stale_events_injected"]),
            duplicate_events_injected=int(snapshot["duplicate_events_injected"]),
            duplicate_events_delivered=int(snapshot["duplicate_events_delivered"]),
            invalid_events_by_type=dict(snapshot["invalid_events_by_type"]),
            seed=self._options.seed,
            stale_hours=self._options.stale_hours,
            producer_delivery_latency_sample_count=int(
                snapshot["producer_delivery_latency_sample_count"]
            ),
            producer_delivery_latency_average_ms=snapshot[
                "producer_delivery_latency_average_ms"
            ],
            producer_delivery_latency_p95_ms=snapshot["producer_delivery_latency_p95_ms"],
            producer_delivery_latency_p99_ms=snapshot["producer_delivery_latency_p99_ms"],
            delivered_offset_ranges=snapshot["delivered_offset_ranges"],
        )
