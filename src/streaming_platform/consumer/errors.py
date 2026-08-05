"""Shared permanent-message classification and DLQ construction."""

import base64
import json
from datetime import UTC, datetime
from json import JSONDecodeError
from typing import cast
from uuid import UUID

from pydantic import JsonValue, ValidationError
from sqlalchemy.exc import DBAPIError, InterfaceError, OperationalError

from streaming_platform.kafka.consumer import KafkaMessage
from streaming_platform.models.dlq import DlqMessage


class PermanentMessageError(ValueError):
    """A decoding, validation, or unsupported-type failure routed to the DLQ."""

    def __init__(
        self,
        error_type: str,
        safe_message: str,
        decoded_payload: JsonValue | None = None,
    ) -> None:
        """Store a stable category, safe description, and optional decoded JSON."""
        super().__init__(safe_message)
        self.error_type = error_type
        self.safe_message = safe_message
        self.decoded_payload = decoded_payload


def decode_json_value(raw_value: bytes) -> JsonValue:
    """Decode a Kafka value as UTF-8 JSON or raise a permanent error."""
    try:
        text = raw_value.decode("utf-8")
    except UnicodeDecodeError as error:
        raise PermanentMessageError("JSONDecodeError", "payload is not valid UTF-8") from error
    try:
        return cast(JsonValue, json.loads(text))
    except JSONDecodeError as error:
        safe_message = f"{error.msg} at line {error.lineno} column {error.colno}"
        raise PermanentMessageError("JSONDecodeError", safe_message) from error


def safe_validation_message(error: ValidationError, fallback: str) -> str:
    """Render Pydantic errors without including potentially sensitive input values."""
    details = []
    for item in error.errors(include_input=False, include_url=False):
        location = ".".join(str(part) for part in item["loc"])
        details.append(f"{location}: {item['msg']}")
    return "; ".join(details)[:1000] or fallback


def is_retryable_database_error(error: Exception) -> bool:
    """Classify temporary PostgreSQL connection and transaction failures."""
    if isinstance(error, (OperationalError, InterfaceError)):
        return True
    if not isinstance(error, DBAPIError):
        return False
    if error.connection_invalidated:
        return True
    original = error.orig
    sqlstate = getattr(original, "sqlstate", None) or getattr(original, "pgcode", None)
    return bool(
        isinstance(sqlstate, str)
        and (sqlstate.startswith("08") or sqlstate in {"40001", "40P01", "55P03"})
    )


def build_dlq_message(
    message: KafkaMessage,
    error: PermanentMessageError,
    consumer_group: str,
    failed_at: datetime,
) -> tuple[str, DlqMessage]:
    """Build a loss-aware DLQ body and stable Kafka key."""
    raw_value = message.value() or b""
    if error.decoded_payload is not None:
        original_payload = error.decoded_payload
        encoding = "json"
    else:
        try:
            original_payload = raw_value.decode("utf-8")
            encoding = "utf-8"
        except UnicodeDecodeError:
            original_payload = base64.b64encode(raw_value).decode("ascii")
            encoding = "base64"

    raw_key = message.key() or b""
    try:
        original_key = raw_key.decode("utf-8")
    except UnicodeDecodeError:
        original_key = base64.b64encode(raw_key).decode("ascii")

    dlq_key = _extract_event_id(error.decoded_payload)
    if dlq_key is None:
        dlq_key = f"{message.topic()}:{message.partition()}:{message.offset()}"

    return dlq_key, DlqMessage(
        failed_at=failed_at.astimezone(UTC),
        error_type=error.error_type,
        error_message=error.safe_message,
        original_topic=message.topic(),
        original_partition=message.partition(),
        original_offset=message.offset(),
        consumer_group=consumer_group,
        original_key=original_key,
        original_payload=original_payload,
        original_payload_encoding=encoding,
    )


def _extract_event_id(payload: JsonValue | None) -> str | None:
    if not isinstance(payload, dict):
        return None
    value = payload.get("event_id")
    try:
        return str(UUID(str(value)))
    except (TypeError, ValueError, AttributeError):
        return None
