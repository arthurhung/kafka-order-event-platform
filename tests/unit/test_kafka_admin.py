"""Tests for immutable Kafka topic definitions."""

from dataclasses import dataclass

from streaming_platform.config import Settings
from streaming_platform.kafka.admin import TopicSpec, topic_specs, topic_topology_mismatch


@dataclass
class FakePartitionMetadata:
    replicas: list[int]


@dataclass
class FakeTopicMetadata:
    partitions: dict[int, FakePartitionMetadata]


def test_topic_specs_match_contract(settings: Settings) -> None:
    specs = topic_specs(settings)

    assert [(spec.name, spec.partitions, spec.replication_factor) for spec in specs] == [
        ("ecommerce.orders.raw.v1", 6, 1),
        ("ecommerce.application-logs.raw.v1", 6, 1),
        ("ecommerce.dlq.v1", 3, 1),
    ]


def test_topic_topology_accepts_expected_shape() -> None:
    metadata = FakeTopicMetadata(
        partitions={index: FakePartitionMetadata([1]) for index in range(6)}
    )

    assert topic_topology_mismatch(TopicSpec("orders", 6, 1), metadata) is None


def test_topic_topology_reports_partition_mismatch() -> None:
    metadata = FakeTopicMetadata(
        partitions={index: FakePartitionMetadata([1]) for index in range(3)}
    )

    assert topic_topology_mismatch(TopicSpec("orders", 6, 1), metadata) == (
        "orders has 3 partitions; expected 6"
    )


def test_topic_topology_reports_replication_factor_mismatch() -> None:
    metadata = FakeTopicMetadata(
        partitions={index: FakePartitionMetadata([1, 2]) for index in range(3)}
    )

    assert topic_topology_mismatch(TopicSpec("dlq", 3, 1), metadata) == (
        "dlq has replication factor(s) 2; expected 1"
    )
