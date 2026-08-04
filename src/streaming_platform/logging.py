"""Structured JSON logging configuration."""

import logging
import sys
from datetime import UTC, datetime
from typing import Any

from pythonjsonlogger.json import JsonFormatter

from streaming_platform.config import Settings


class PlatformJsonFormatter(JsonFormatter):
    """Emit the platform's required base fields with UTC timestamps."""

    def add_fields(
        self,
        log_record: dict[str, Any],
        record: logging.LogRecord,
        message_dict: dict[str, Any],
    ) -> None:
        """Normalize standard log fields before JSON serialization."""
        super().add_fields(log_record, record, message_dict)
        log_record["timestamp"] = (
            datetime.fromtimestamp(record.created, tz=UTC)
            .isoformat(timespec="milliseconds")
            .replace("+00:00", "Z")
        )
        log_record["level"] = record.levelname
        log_record["service"] = getattr(record, "service", "unknown")
        log_record["message"] = record.getMessage()


def configure_logging(settings: Settings, service: str) -> logging.LoggerAdapter[logging.Logger]:
    """Configure root JSON logging and return a service-scoped logger."""
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(PlatformJsonFormatter())

    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.addHandler(handler)
    root_logger.setLevel(settings.LOG_LEVEL)

    logger = logging.getLogger(service)
    return logging.LoggerAdapter(logger, {"service": service}, merge_extra=True)
