"""Application log payloads and discriminated event models."""

from enum import StrEnum
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, TypeAdapter

from streaming_platform.models.event import BaseEventEnvelope, EventType, NonEmptyString

Endpoint = Annotated[str, StringConstraints(pattern=r"^/")]


class HttpMethod(StrEnum):
    """HTTP methods accepted by API access logs."""

    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    PATCH = "PATCH"
    DELETE = "DELETE"
    HEAD = "HEAD"
    OPTIONS = "OPTIONS"


class ApiAccessLogPayload(BaseModel):
    """Fields required for an API access log."""

    model_config = ConfigDict(extra="forbid")

    request_id: NonEmptyString
    service: NonEmptyString
    endpoint: Endpoint
    http_method: HttpMethod
    status_code: Annotated[int, Field(ge=100, le=599)]
    response_time_ms: Annotated[int, Field(ge=0)]
    client_ip: NonEmptyString


class ApplicationErrorLogPayload(BaseModel):
    """Fields required and optionally accepted for an application error log."""

    model_config = ConfigDict(extra="forbid")

    request_id: NonEmptyString
    service: NonEmptyString
    error_type: NonEmptyString
    error_message: NonEmptyString
    endpoint: Endpoint | None = None
    stack_trace: str | None = None
    trace_id: NonEmptyString | None = None


class ApiAccessLogEvent(BaseEventEnvelope[ApiAccessLogPayload]):
    """An API access log event."""

    event_type: Literal[EventType.API_ACCESS_LOG] = EventType.API_ACCESS_LOG


class ApplicationErrorLogEvent(BaseEventEnvelope[ApplicationErrorLogPayload]):
    """An application error log event."""

    event_type: Literal[EventType.APPLICATION_ERROR_LOG] = EventType.APPLICATION_ERROR_LOG


API_ACCESS_LOG_ADAPTER: TypeAdapter[ApiAccessLogEvent] = TypeAdapter(ApiAccessLogEvent)
