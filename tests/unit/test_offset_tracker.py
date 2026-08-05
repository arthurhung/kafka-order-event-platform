from streaming_platform.consumer.offsets import ContiguousOffsetTracker
from streaming_platform.metrics.log_aggregation import KafkaCoordinate


def coordinate(partition: int, offset: int) -> KafkaCoordinate:
    return KafkaCoordinate("logs.v1", partition, offset)


def test_contiguous_offset_does_not_cross_pending_valid_event() -> None:
    tracker = ContiguousOffsetTracker()
    tracker.observe(coordinate(0, 10))
    tracker.observe(coordinate(0, 11))
    tracker.complete(coordinate(0, 11))
    assert tracker.committable() == {}

    tracker.complete(coordinate(0, 10))
    assert tracker.committable() == {("logs.v1", 0): 12}
    tracker.acknowledge_commit(("logs.v1", 0), 12)
    assert tracker.committable() == {}


def test_partitions_advance_independently() -> None:
    tracker = ContiguousOffsetTracker()
    for item in (coordinate(0, 3), coordinate(1, 8)):
        tracker.observe(item)
    tracker.complete(coordinate(1, 8))

    assert tracker.committable() == {("logs.v1", 1): 9}
    assert tracker.has_pending({("logs.v1", 0)}) is True
    assert tracker.has_pending({("logs.v1", 1)}) is False


def test_commit_failure_can_retry_same_frontier() -> None:
    tracker = ContiguousOffsetTracker()
    tracker.observe(coordinate(2, 20))
    tracker.complete(coordinate(2, 20))
    assert tracker.committable() == {("logs.v1", 2): 21}
    assert tracker.committable() == {("logs.v1", 2): 21}
