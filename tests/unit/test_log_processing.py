import json
import logging
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from confluent_kafka import TopicPartition

from streaming_platform.consumer.errors import PermanentMessageError
from streaming_platform.consumer.log import (
    LogConsumerRunner,
    LogMessageProcessor,
    LogProcessingResult,
    PartitionLostError,
    decode_access_log_event,
)
from streaming_platform.consumer.offsets import ContiguousOffsetTracker
from streaming_platform.consumer.retry import RetryPolicy
from streaming_platform.metrics.log_aggregation import KafkaCoordinate, LogAggregationBuffer
from streaming_platform.models import ApiAccessLogEvent, ApiAccessLogPayload


class FakeMessage:
    def __init__(self, value: bytes, offset: int = 7):
        self._value = value
        self._offset = offset

    def topic(self):
        return "logs.v1"

    def partition(self):
        return 0

    def offset(self):
        return self._offset

    def key(self):
        return b"order-api"

    def value(self):
        return self._value


class FakeDlq:
    def __init__(self):
        self.messages = []

    def publish(self, key, message):
        self.messages.append((key, message))


def valid_event() -> ApiAccessLogEvent:
    return ApiAccessLogEvent(
        event_id=uuid4(),
        event_time=datetime(2026, 8, 5, 10, 1, 30, tzinfo=UTC),
        source="order-api",
        payload=ApiAccessLogPayload(
            request_id="REQ-1",
            service="order-api",
            endpoint="/orders",
            http_method="GET",
            status_code=200,
            response_time_ms=12,
            client_ip="10.0.0.1",
        ),
    )


def logger():
    return logging.LoggerAdapter(logging.getLogger("log-processing-test"), {})


@pytest.mark.parametrize(
    "value, error_type",
    [
        (b"{", "JSONDecodeError"),
        (json.dumps({"event_type": "application_error_log"}).encode(), "UnsupportedEventType"),
        (json.dumps({"event_type": "order_created"}).encode(), "UnsupportedEventType"),
    ],
)
def test_decode_classifies_permanent_errors(value: bytes, error_type: str) -> None:
    with pytest.raises(PermanentMessageError) as raised:
        decode_access_log_event(value)
    assert raised.value.error_type == error_type


def test_validation_error_is_permanent() -> None:
    value = json.loads(valid_event().model_dump_json())
    value["payload"]["endpoint"] = "orders"
    with pytest.raises(PermanentMessageError) as raised:
        decode_access_log_event(json.dumps(value).encode())
    assert raised.value.error_type == "ValidationError"


def test_valid_event_waits_in_buffer_and_invalid_dlq_completes_offset() -> None:
    buffer = LogAggregationBuffer()
    offsets = ContiguousOffsetTracker()
    dlq = FakeDlq()
    processor = LogMessageProcessor(
        buffer, offsets, dlq, "log-group", RetryPolicy(max_retries=0), logger()
    )
    valid_message = FakeMessage(valid_event().model_dump_json().encode(), offset=10)
    invalid_message = FakeMessage(b"{", offset=11)
    for message in (valid_message, invalid_message):
        offsets.observe(KafkaCoordinate(message.topic(), message.partition(), message.offset()))

    assert processor.process(valid_message) is LogProcessingResult.BUFFERED
    assert processor.process(invalid_message) is LogProcessingResult.DLQED
    assert len(buffer) == 1
    assert len(dlq.messages) == 1
    assert offsets.committable() == {}


class EmptyConsumer:
    def __init__(self):
        self.polls = 0

    def poll(self, _timeout):
        self.polls += 1
        return None


class RecordingFlush:
    def __init__(self):
        self.flushes = 0
        self.calls = []

    def flush(self, *args, **kwargs):
        self.flushes += 1
        self.calls.append((args, kwargs))
        return 0

    def commit_ready(self, *_args):
        return None


def test_graceful_shutdown_flushes_before_return() -> None:
    consumer = EmptyConsumer()
    flush = RecordingFlush()
    runner = LogConsumerRunner(
        consumer,
        object(),
        flush,
        ContiguousOffsetTracker(),
        RetryPolicy(max_retries=0),
        0.01,
        10,
        logger(),
    )
    runner.request_shutdown()
    runner.run()
    runner.shutdown()
    assert consumer.polls == 0
    assert flush.flushes == 1


def test_rebalance_revoke_flushes_before_forgetting_partition() -> None:
    flush = RecordingFlush()
    offsets = ContiguousOffsetTracker()
    runner = LogConsumerRunner(
        EmptyConsumer(), object(), flush, offsets, RetryPolicy(max_retries=0), 0.01, 10, logger()
    )
    runner.on_revoke([TopicPartition("logs.v1", 2)])
    assert flush.calls == [(({("logs.v1", 2)},), {})]


def test_assignment_lost_flushes_without_committing_and_stops() -> None:
    flush = RecordingFlush()
    runner = LogConsumerRunner(
        EmptyConsumer(),
        object(),
        flush,
        ContiguousOffsetTracker(),
        RetryPolicy(max_retries=0),
        0.01,
        10,
        logger(),
    )
    with pytest.raises(PartitionLostError):
        runner.on_lost([TopicPartition("logs.v1", 3)])
    assert flush.calls == [(({("logs.v1", 3)},), {"commit": False})]
