"""Tests for structured JSON logging."""

import json
import logging

from streaming_platform.config import Settings
from streaming_platform.logging import configure_logging


def test_configure_logging_emits_required_base_fields(capsys, settings: Settings) -> None:
    logger = configure_logging(settings, "phase-one-test")

    logger.info("service ready", extra={"topic": "example.v1"})
    record = json.loads(capsys.readouterr().out)

    assert record["level"] == "INFO"
    assert record["service"] == "phase-one-test"
    assert record["message"] == "service ready"
    assert record["topic"] == "example.v1"
    assert record["timestamp"].endswith("Z")

    logging.getLogger().handlers.clear()
