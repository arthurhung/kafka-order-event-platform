from datetime import UTC, datetime

from streaming_platform.benchmark.runner import (
    benchmark_lag,
    consumed_from_ranges,
    coordinate_in_ranges,
)
from streaming_platform.generator.report import DeliveredOffsetRange
from streaming_platform.kafka.lag import LagGroup, LagRow, LagSnapshot


def snapshot(current: int | None) -> LagSnapshot:
    row = LagRow(
        consumer_group="group",
        topic="orders.v1",
        partition=0,
        current_offset=current,
        log_end_offset=20,
        lag=None if current is None else 20 - current,
        status="offset_not_available" if current is None else "measured",
    )
    return LagSnapshot(
        observed_at=datetime.now(UTC),
        groups=[LagGroup(consumer_group="group", topic="orders.v1", status="measured", rows=[row])],
    )


def test_consumed_count_uses_committed_next_offset() -> None:
    ranges = [
        DeliveredOffsetRange(
            topic="orders.v1", partition=0, start_offset=10, end_offset_exclusive=15
        )
    ]
    assert consumed_from_ranges(ranges, snapshot(12)) == 2
    assert consumed_from_ranges(ranges, snapshot(20)) == 5
    assert consumed_from_ranges(ranges, snapshot(None)) == 0


def test_coordinate_matching_is_exact() -> None:
    ranges = [
        DeliveredOffsetRange(
            topic="orders.v1", partition=1, start_offset=10, end_offset_exclusive=12
        ),
        DeliveredOffsetRange(
            topic="orders.v1", partition=1, start_offset=14, end_offset_exclusive=15
        ),
    ]
    assert coordinate_in_ranges(("orders.v1", 1, 10), ranges) is True
    assert coordinate_in_ranges(("orders.v1", 1, 13), ranges) is False
    assert coordinate_in_ranges(("orders.v1", 2, 10), ranges) is False


def test_benchmark_lag_only_treats_provably_empty_missing_offset_as_zero() -> None:
    empty = snapshot(None)
    empty.groups[0].rows[0].log_end_offset = 0
    assert benchmark_lag(empty) == 0
    unavailable_with_data = snapshot(None)
    assert benchmark_lag(unavailable_with_data) is None
