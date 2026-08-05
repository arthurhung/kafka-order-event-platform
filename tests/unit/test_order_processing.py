import json
import logging
from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import pytest

from streaming_platform.consumer.order import (
    OrderConsumerRunner,
    PermanentMessageError,
    ProcessingResult,
    decode_order_event,
)
from streaming_platform.consumer.retry import RetryPolicy
from streaming_platform.models import OrderPaidEvent, OrderPaidPayload


class FakeMessage:
    def __init__(self, value: bytes):
        self._value = value

    def topic(self):
        return "orders.v1"

    def partition(self):
        return 0

    def offset(self):
        return 7

    def key(self):
        return b"ORD-1"

    def value(self):
        return self._value


def valid_value() -> bytes:
    event = OrderPaidEvent(
        event_id=uuid4(),
        event_time=datetime.now(UTC),
        source="payment-api",
        payload=OrderPaidPayload(
            order_id="ORD-1",
            user_id="USR-1",
            payment_id="PAY-1",
            amount=Decimal("10.00"),
            currency="TWD",
            payment_method="card",
        ),
    )
    return event.model_dump_json().encode()


def test_decode_classifies_json_and_validation_errors() -> None:
    with pytest.raises(PermanentMessageError) as malformed:
        decode_order_event(b"{")
    assert malformed.value.error_type == "JSONDecodeError"

    invalid = json.loads(valid_value())
    invalid["payload"]["amount"] = "-1.00"
    with pytest.raises(PermanentMessageError) as validation:
        decode_order_event(json.dumps(invalid).encode())
    assert validation.value.error_type == "ValidationError"
    assert "greater than 0" in validation.value.safe_message


def test_decode_rejects_log_event_as_unsupported_order_event() -> None:
    value = json.loads(valid_value())
    value["event_type"] = "api_access_log"
    with pytest.raises(PermanentMessageError) as raised:
        decode_order_event(json.dumps(value).encode())
    assert raised.value.error_type == "ValidationError"


class FakeConsumer:
    def __init__(self, message):
        self.message = message
        self.committed = []
        self.polls = 0

    def poll(self, _timeout):
        self.polls += 1
        return self.message

    def commit(self, message):
        self.committed.append(message)


class SuccessfulProcessor:
    def __init__(self, result):
        self.result = result

    def process(self, _message):
        return self.result


class FailingProcessor:
    def process(self, _message):
        raise RuntimeError("database failed")


def logger():
    return logging.LoggerAdapter(logging.getLogger("order-processing-test"), {})


@pytest.mark.parametrize("result", list(ProcessingResult))
def test_runner_commits_every_completed_processing_result(result) -> None:
    message = FakeMessage(valid_value())
    consumer = FakeConsumer(message)
    runner = OrderConsumerRunner(
        consumer,
        SuccessfulProcessor(result),
        RetryPolicy(max_retries=0),
        0.01,
        logger(),
    )
    original_commit = consumer.commit

    def commit_and_stop(committed_message):
        original_commit(committed_message)
        runner.request_shutdown()

    consumer.commit = commit_and_stop
    runner.run()

    assert consumer.committed == [message]


def test_runner_does_not_commit_failed_processing() -> None:
    message = FakeMessage(valid_value())
    consumer = FakeConsumer(message)
    runner = OrderConsumerRunner(
        consumer,
        FailingProcessor(),
        RetryPolicy(max_retries=0),
        0.01,
        logger(),
    )

    with pytest.raises(RuntimeError, match="database failed"):
        runner.run()

    assert consumer.committed == []


def test_shutdown_request_stops_before_polling() -> None:
    consumer = FakeConsumer(FakeMessage(valid_value()))
    runner = OrderConsumerRunner(
        consumer,
        SuccessfulProcessor(ProcessingResult.PROCESSED),
        RetryPolicy(),
        0.01,
        logger(),
    )
    runner.request_shutdown()
    runner.run()
    assert consumer.polls == 0
