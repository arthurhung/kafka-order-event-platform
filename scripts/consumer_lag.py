"""Print real Kafka consumer-group lag for the platform groups."""

import argparse
import sys

from streaming_platform.config import get_settings
from streaming_platform.kafka.lag import LagInspector, TopicMissingError, format_lag_table


def main() -> None:
    """Inspect lag in human or machine-readable JSON form."""
    parser = argparse.ArgumentParser(description="Inspect platform Kafka consumer lag")
    parser.add_argument("--format", choices=("table", "json"), default="table")
    parser.add_argument("--timeout", type=float, default=10.0)
    arguments = parser.parse_args()
    try:
        snapshot = LagInspector(
            get_settings(), timeout_seconds=arguments.timeout
        ).inspect()
    except TopicMissingError as error:
        print(f"consumer-lag failed: {error}", file=sys.stderr)
        raise SystemExit(2) from None
    except Exception as error:
        print(f"consumer-lag failed: {type(error).__name__}: {error}", file=sys.stderr)
        raise SystemExit(1) from None
    if arguments.format == "json":
        print(snapshot.model_dump_json(indent=2))
    else:
        print(format_lag_table(snapshot))


if __name__ == "__main__":
    main()
