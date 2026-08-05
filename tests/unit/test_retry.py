import pytest
from sqlalchemy.exc import OperationalError

from streaming_platform.consumer.order import is_retryable_database_error
from streaming_platform.consumer.retry import RetriesExhaustedError, RetryPolicy, run_with_retry


def test_retry_uses_bounded_exponential_backoff() -> None:
    attempts = []
    delays = []

    def operation() -> str:
        attempts.append(1)
        if len(attempts) < 4:
            raise TimeoutError("temporary")
        return "ok"

    result = run_with_retry(
        operation,
        operation_name="test operation",
        policy=RetryPolicy(max_retries=3, base_seconds=1, max_seconds=4),
        is_retryable=lambda error: isinstance(error, TimeoutError),
        sleeper=delays.append,
    )

    assert result == "ok"
    assert len(attempts) == 4
    assert delays == [1, 2, 4]


def test_retry_delay_is_capped() -> None:
    policy = RetryPolicy(base_seconds=2, max_seconds=5)
    assert [policy.delay(index) for index in range(4)] == [2, 4, 5, 5]


def test_non_retryable_error_is_preserved_without_sleep() -> None:
    delays = []

    with pytest.raises(ValueError, match="permanent"):
        run_with_retry(
            lambda: (_ for _ in ()).throw(ValueError("permanent")),
            operation_name="test operation",
            policy=RetryPolicy(),
            is_retryable=lambda _error: False,
            sleeper=delays.append,
        )

    assert delays == []


def test_retries_exhausted_keeps_final_error_as_cause() -> None:
    with pytest.raises(RetriesExhaustedError) as raised:
        run_with_retry(
            lambda: (_ for _ in ()).throw(TimeoutError("offline")),
            operation_name="database",
            policy=RetryPolicy(max_retries=2, base_seconds=1, max_seconds=2),
            is_retryable=lambda error: isinstance(error, TimeoutError),
            sleeper=lambda _delay: None,
        )

    assert raised.value.attempts == 3
    assert isinstance(raised.value.__cause__, TimeoutError)


def test_database_operational_error_is_retryable() -> None:
    error = OperationalError("statement", {}, TimeoutError("offline"))
    assert is_retryable_database_error(error) is True
    assert is_retryable_database_error(ValueError("bad data")) is False
