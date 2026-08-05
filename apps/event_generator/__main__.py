"""Command-line entry point for Phase 2 event generation."""

import argparse
import logging
from pathlib import Path

from streaming_platform.config import get_settings
from streaming_platform.generator.factory import EventFactory
from streaming_platform.generator.options import GeneratorOptions
from streaming_platform.generator.report import DeliveryTracker, ProducerReport
from streaming_platform.generator.runner import GeneratorRunner
from streaming_platform.kafka.producer import TrackedKafkaProducer
from streaming_platform.logging import configure_logging


def parse_args() -> argparse.Namespace:
    """Parse supported generator CLI options."""
    parser = argparse.ArgumentParser(description="Generate order and application-log events")
    parser.add_argument("--events-per-second", type=float, default=100.0)
    parser.add_argument("--duration-seconds", type=float, default=60.0)
    parser.add_argument("--order-ratio", type=float, default=0.2)
    parser.add_argument("--log-ratio", type=float, default=0.8)
    parser.add_argument("--application-error-ratio", type=float, default=0.5)
    parser.add_argument("--invalid-rate", type=float, default=0.0)
    parser.add_argument("--stale-rate", type=float, default=0.0)
    parser.add_argument("--stale-hours", type=float, default=168.0)
    parser.add_argument("--duplicate-rate", type=float, default=0.0)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--report-path", type=Path, default=Path("reports/latest.json"))
    return parser.parse_args()


def _run_with_graceful_interrupt(
    runner: GeneratorRunner,
    logger: logging.LoggerAdapter[logging.Logger],
    report_path: Path,
) -> ProducerReport:
    try:
        return runner.run()
    except KeyboardInterrupt:
        logger.info(
            "event generation interrupted",
            extra={"report_path": str(report_path)},
        )
        raise SystemExit(130) from None


def main() -> None:
    """Build services, run generation, and fail if any delivery failed."""
    arguments = parse_args()
    options = GeneratorOptions(**vars(arguments))
    settings = get_settings()
    logger = configure_logging(settings, "event-generator")
    tracker = DeliveryTracker()
    factory = EventFactory(settings, seed=options.seed)
    producer = TrackedKafkaProducer(settings, tracker, logger)
    runner = GeneratorRunner(options, factory, producer, tracker, logger)
    report = _run_with_graceful_interrupt(runner, logger, options.report_path)
    logger.info(
        "event generation completed",
        extra={
            "attempted": report.attempted,
            "delivered": report.delivered,
            "failed": report.failed,
            "actual_events_per_second": report.actual_events_per_second,
            "report_path": str(options.report_path),
        },
    )
    if report.failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
