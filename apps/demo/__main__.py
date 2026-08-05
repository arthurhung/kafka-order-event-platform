"""Run the small mixed, lag recovery, and replay demonstration."""

from streaming_platform.config import get_settings
from streaming_platform.demo import DemoRunner
from streaming_platform.logging import configure_logging


def main() -> None:
    """Run the demo and expose its report path and exit status."""
    settings = get_settings()
    logger = configure_logging(settings, "demo")
    exit_code, path = DemoRunner(settings, logger).run()
    logger.info(
        "demo completed",
        extra={"exit_code": exit_code, "report_path": str(path)},
    )
    raise SystemExit(exit_code)


if __name__ == "__main__":
    main()
