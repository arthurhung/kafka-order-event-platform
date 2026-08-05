"""Bounded retry helpers shared by consumer database and Kafka operations."""

from collections.abc import Callable
from dataclasses import dataclass
from time import sleep


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """Configurable exponential backoff with a fixed retry limit and delay cap."""

    max_retries: int = 3
    base_seconds: float = 1.0
    max_seconds: float = 4.0

    def delay(self, retry_index: int) -> float:
        """Return the capped delay for a zero-based retry index."""
        if retry_index < 0:
            raise ValueError("retry_index must be non-negative")
        return float(min(self.base_seconds * (2**retry_index), self.max_seconds))


class RetriesExhaustedError(RuntimeError):
    """Raised after a retryable operation consumes its configured retries."""

    def __init__(self, operation: str, attempts: int, cause: Exception) -> None:
        """Describe the exhausted operation while chaining its final cause."""
        super().__init__(f"{operation} failed after {attempts} attempts: {cause}")
        self.operation = operation
        self.attempts = attempts


def run_with_retry[ResultT](
    operation: Callable[[], ResultT],
    *,
    operation_name: str,
    policy: RetryPolicy,
    is_retryable: Callable[[Exception], bool],
    sleeper: Callable[[float], None] = sleep,
    on_retry: Callable[[Exception, int, float], None] | None = None,
) -> ResultT:
    """Run an operation with bounded retry and preserve non-retryable failures."""
    for attempt in range(policy.max_retries + 1):
        try:
            return operation()
        except Exception as error:
            if not is_retryable(error):
                raise
            if attempt == policy.max_retries:
                raise RetriesExhaustedError(operation_name, attempt + 1, error) from error
            delay = policy.delay(attempt)
            if on_retry is not None:
                on_retry(error, attempt + 1, delay)
            sleeper(delay)
    raise AssertionError("retry loop must return or raise")
