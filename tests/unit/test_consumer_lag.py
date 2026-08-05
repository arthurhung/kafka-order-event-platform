from datetime import UTC, datetime

from streaming_platform.kafka.lag import (
    LagGroup,
    LagInspector,
    LagRow,
    LagSnapshot,
    format_lag_table,
)


class FakeTopic:
    def __init__(self) -> None:
        self.partitions = {0: object(), 1: object()}
        self.error = None


class FakeCluster:
    def __init__(self, settings) -> None:
        self.topics = {
            settings.KAFKA_ORDER_TOPIC: FakeTopic(),
            settings.KAFKA_LOG_TOPIC: FakeTopic(),
        }


class FakeGroupResult:
    def __init__(self) -> None:
        self.valid = []
        self.errors = []


class FakeFuture:
    def result(self, _timeout):
        return FakeGroupResult()


class FakeAdmin:
    def __init__(self, settings) -> None:
        self.cluster = FakeCluster(settings)

    def list_topics(self, timeout):
        return self.cluster

    def list_consumer_groups(self, request_timeout):
        return FakeFuture()


def test_lag_total_and_human_output() -> None:
    rows = [
        LagRow(
            consumer_group="group-v1",
            topic="orders.v1",
            partition=0,
            current_offset=8,
            log_end_offset=10,
            lag=2,
        ),
        LagRow(
            consumer_group="group-v1",
            topic="orders.v1",
            partition=1,
            current_offset=3,
            log_end_offset=4,
            lag=1,
        ),
    ]
    snapshot = LagSnapshot(
        observed_at=datetime.now(UTC),
        groups=[
            LagGroup(
                consumer_group="group-v1",
                topic="orders.v1",
                status="measured",
                rows=rows,
                total_lag=3,
            )
        ],
    )
    output = format_lag_table(snapshot)
    assert "CURRENT OFFSET" in output
    assert "TOTAL group-v1 / orders.v1: 3" in output
    assert snapshot.combined_total_lag == 3


def test_missing_group_representation_does_not_fake_offsets() -> None:
    row = LagRow(
        consumer_group="missing",
        topic="orders.v1",
        partition=0,
        current_offset=None,
        log_end_offset=None,
        lag=None,
        status="group_not_established",
    )
    snapshot = LagSnapshot(
        observed_at=datetime.now(UTC),
        groups=[
            LagGroup(
                consumer_group="missing",
                topic="orders.v1",
                status="group_not_established",
                rows=[row],
            )
        ],
    )
    assert snapshot.combined_total_lag is None
    assert "not_available" in format_lag_table(snapshot)


def test_inspector_handles_missing_groups_without_creating_consumers(settings) -> None:
    def unexpected_consumer(_group):
        raise AssertionError("missing groups must not create offset consumers")

    snapshot = LagInspector(
        settings,
        admin=FakeAdmin(settings),
        consumer_factory=unexpected_consumer,
    ).inspect()

    assert [group.status for group in snapshot.groups] == [
        "group_not_established",
        "group_not_established",
    ]
    assert all(row.current_offset is None for group in snapshot.groups for row in group.rows)
