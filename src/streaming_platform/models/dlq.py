"""Dead-letter message model for permanently invalid Kafka records."""

from typing import Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, JsonValue


class DlqMessage(BaseModel):
    """A safe, JSON-serializable record preserving source coordinates and payload."""

    model_config = ConfigDict(extra="forbid")

    failed_at: AwareDatetime
    error_type: str
    error_message: str
    original_topic: str
    original_partition: int
    original_offset: int
    consumer_group: str
    original_key: str
    original_payload: JsonValue
    original_payload_encoding: Literal["json", "utf-8", "base64"]

    def encoded(self) -> bytes:
        """Serialize the DLQ record as compact UTF-8 JSON."""
        return self.model_dump_json().encode("utf-8")
