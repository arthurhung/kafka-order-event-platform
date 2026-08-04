"""Wait until local Kafka and PostgreSQL accept connections."""

import argparse
import socket
import time

from streaming_platform.config import get_settings


def wait_for_tcp(host: str, port: int, timeout_seconds: float) -> None:
    """Wait for a TCP endpoint or raise TimeoutError."""
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=1):
                return
        except OSError:
            time.sleep(1)
    raise TimeoutError(f"Timed out waiting for {host}:{port}")


def main() -> None:
    """Wait for Phase 1 infrastructure endpoints."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--timeout", type=float, default=120.0)
    args = parser.parse_args()
    settings = get_settings()
    kafka_host, kafka_port = settings.KAFKA_BOOTSTRAP_SERVERS.rsplit(":", maxsplit=1)
    wait_for_tcp(kafka_host, int(kafka_port), args.timeout)
    wait_for_tcp(settings.POSTGRES_HOST, settings.POSTGRES_PORT, args.timeout)
    wait_for_tcp("localhost", settings.KAFKA_UI_PORT, args.timeout)


if __name__ == "__main__":
    main()
