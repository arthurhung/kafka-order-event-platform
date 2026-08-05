"""Common event envelope types and validation."""

from datetime import UTC, datetime
from enum import StrEnum
from typing import Annotated, Literal
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, StringConstraints, field_validator

NonEmptyString = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]


class EventType(StrEnum):
    """Supported version-one event types."""

    ORDER_CREATED = "order_created"
    ORDER_PAID = "order_paid"
    ORDER_CANCELLED = "order_cancelled"
    PAYMENT_FAILED = "payment_failed"
    API_ACCESS_LOG = "api_access_log"
    APPLICATION_ERROR_LOG = "application_error_log"


class BaseEventEnvelope[PayloadT: BaseModel](BaseModel):
    """Fields shared by every platform event."""

    model_config = ConfigDict(extra="forbid")

    event_id: UUID
    event_type: EventType
    event_version: Literal[1] = 1
    event_time: AwareDatetime
    source: NonEmptyString
    payload: PayloadT

    @field_validator("event_time")
    @classmethod
    def normalize_event_time(cls, value: datetime) -> datetime:
        """Normalize an already timezone-aware event timestamp to UTC."""
        return value.astimezone(UTC)
