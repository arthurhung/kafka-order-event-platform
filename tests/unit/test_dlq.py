import base64
import json
from datetime import UTC, datetime
from typing import cast
from uuid import uuid4

import pytest
from confluent_kafka import KafkaError, KafkaException, Message

from streaming_platform.consumer.order import PermanentMessageError, build_dlq_message
from streaming_platform.kafka.dlq import DlqProducer


class FakeMessage:
    def __init__(self, value=b"{}", key=b"ORD-1"):
        self._value = value
        self._key = key

    def topic(self):
        return "ecommerce.orders.raw.v1"

    def partition(self):
        return 2

    def offset(self):
        return 1035

    def key(self):
        return self._key

    def value(self):
        return self._value


class FakeProducer:
    def __init__(self, delivery_error=None):
        self.delivery_error = delivery_error
        self.callback = None
        self.arguments = None

    def produce(self, topic, **arguments):
        self.arguments = {"topic": topic, **arguments}
        self.callback = arguments["on_delivery"]

    def poll(self, _timeout):
        callback, self.callback = self.callback, None
        callback(self.delivery_error, cast(Message, object()))
        return 1

    def flush(self, _timeout):
        return 0


def test_dlq_message_preserves_validation_context_and_event_id() -> None:
    event_id = uuid4()
    decoded = {"event_id": str(event_id), "payload": {"amount": "-1.00"}}
    key, message = build_dlq_message(
        FakeMessage(value=json.dumps(decoded).encode()),
        PermanentMessageError("ValidationError", "payload.amount: greater than 0", decoded),
        "order-processing-group-v1",
        datetime(2026, 8, 5, tzinfo=UTC),
    )

    assert key == str(event_id)
    assert message.original_topic == "ecommerce.orders.raw.v1"
    assert message.original_partition == 2
    assert message.original_offset == 1035
    assert message.original_payload == decoded
    assert message.original_payload_encoding == "json"
    assert message.failed_at.tzinfo is not None


def test_dlq_message_uses_source_coordinate_key_and_base64_for_invalid_utf8() -> None:
    raw = b"\xff\xfe"
    key, message = build_dlq_message(
        FakeMessage(value=raw, key=b"\xff"),
        PermanentMessageError("JSONDecodeError", "payload is not valid UTF-8"),
        "order-processing-group-v1",
        datetime.now(UTC),
    )

    assert key == "ecommerce.orders.raw.v1:2:1035"
    assert message.original_payload == base64.b64encode(raw).decode()
    assert message.original_payload_encoding == "base64"


def test_dlq_producer_waits_for_successful_delivery(settings) -> None:
    fake = FakeProducer()
    producer = DlqProducer(settings, producer=fake)
    _key, message = build_dlq_message(
        FakeMessage(value=b"not-json"),
        PermanentMessageError("JSONDecodeError", "invalid JSON"),
        settings.KAFKA_ORDER_CONSUMER_GROUP,
        datetime.now(UTC),
    )

    producer.publish("fallback-key", message)

    assert fake.arguments["topic"] == settings.KAFKA_DLQ_TOPIC
    assert fake.arguments["key"] == b"fallback-key"


def test_dlq_delivery_error_is_not_reported_as_success(settings) -> None:
    error = KafkaError(KafkaError._MSG_TIMED_OUT)
    producer = DlqProducer(settings, producer=FakeProducer(error))
    _key, message = build_dlq_message(
        FakeMessage(),
        PermanentMessageError("JSONDecodeError", "invalid JSON"),
        settings.KAFKA_ORDER_CONSUMER_GROUP,
        datetime.now(UTC),
    )

    with pytest.raises(KafkaException):
        producer.publish("fallback-key", message)
