"""Tests for generator CLI option validation."""

import pytest
from pydantic import ValidationError

from streaming_platform.generator.options import GeneratorOptions


def make_options(**overrides) -> GeneratorOptions:
    values = {
        "events_per_second": 10,
        "duration_seconds": 2,
        "order_ratio": 0.2,
        "log_ratio": 0.8,
    }
    values.update(overrides)
    return GeneratorOptions(**values)


def test_options_calculate_planned_events() -> None:
    assert make_options().planned_events == 20


@pytest.mark.parametrize(
    "overrides, message",
    [
        ({"order_ratio": 0.3}, "sum to 1"),
        (
            {"invalid_rate": 0.5, "stale_rate": 0.3, "duplicate_rate": 0.3},
            "sum to at most 1",
        ),
        ({"stale_hours": 0}, "greater than 0"),
    ],
)
def test_options_reject_invalid_ratios(overrides, message) -> None:
    with pytest.raises(ValidationError, match=message):
        make_options(**overrides)
