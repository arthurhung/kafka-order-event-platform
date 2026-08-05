"""Command-line entry point for measured Phase 5 workloads."""

import argparse

from streaming_platform.benchmark.config import (
    BenchmarkConfig,
    BenchmarkEnvironment,
    BenchmarkProfile,
)
from streaming_platform.benchmark.runner import BenchmarkRunner
from streaming_platform.config import get_settings
from streaming_platform.logging import configure_logging


def parse_args(environment: BenchmarkEnvironment | None = None) -> argparse.Namespace:
    """Parse CLI values, using validated environment settings as defaults."""
    defaults = environment or BenchmarkEnvironment()
    parser = argparse.ArgumentParser(description="Run a measured Kafka benchmark")
    parser.add_argument("--profile", choices=tuple(BenchmarkProfile), default=defaults.profile)
    parser.add_argument("--events-per-second", type=float, default=defaults.target_eps)
    parser.add_argument("--duration-seconds", type=float, default=defaults.duration_seconds)
    parser.add_argument("--order-ratio", type=float, default=defaults.order_ratio)
    parser.add_argument("--log-ratio", type=float, default=defaults.log_ratio)
    parser.add_argument(
        "--application-error-ratio",
        type=float,
        default=defaults.application_error_ratio,
    )
    parser.add_argument("--invalid-rate", type=float, default=defaults.invalid_rate)
    parser.add_argument("--duplicate-rate", type=float, default=defaults.duplicate_rate)
    parser.add_argument("--seed", type=int, default=defaults.seed)
    parser.add_argument("--timeout", type=float, default=defaults.timeout_seconds)
    parser.add_argument(
        "--poll-interval", type=float, default=defaults.poll_interval_seconds
    )
    parser.add_argument("--report-directory", default=defaults.report_directory)
    return parser.parse_args()


def config_from_args(arguments: argparse.Namespace) -> BenchmarkConfig:
    """Resolve named profile defaults beneath CLI/environment overrides."""
    overrides = {
        "target_eps": arguments.events_per_second,
        "duration_seconds": arguments.duration_seconds,
        "order_ratio": arguments.order_ratio,
        "log_ratio": arguments.log_ratio,
        "application_error_ratio": arguments.application_error_ratio,
        "invalid_rate": arguments.invalid_rate,
        "duplicate_rate": arguments.duplicate_rate,
        "seed": arguments.seed,
        "timeout_seconds": arguments.timeout,
        "poll_interval_seconds": arguments.poll_interval,
        "report_directory": arguments.report_directory,
    }
    return BenchmarkConfig.from_profile(
        BenchmarkProfile(arguments.profile), **overrides
    )


def main() -> None:
    """Run the requested profile and return its measured status to the shell."""
    config = config_from_args(parse_args())
    settings = get_settings()
    logger = configure_logging(settings, "benchmark")
    report, path = BenchmarkRunner(settings, config, logger).run()
    logger.info(
        "benchmark completed",
        extra={
            "run_id": report.run_id,
            "status": report.status.value,
            "exit_code": report.exit_code,
            "report_path": str(path),
        },
    )
    raise SystemExit(report.exit_code)


if __name__ == "__main__":
    main()
