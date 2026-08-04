"""Kafka topic definitions and idempotent bootstrap operations."""

from dataclasses import dataclass
from typing import Protocol, cast

from confluent_kafka.admin import AdminClient, NewTopic  # type: ignore[attr-defined]

from streaming_platform.config import Settings


@dataclass(frozen=True, slots=True)
class TopicSpec:
    """Declarative topic configuration."""

    name: str
    partitions: int
    replication_factor: int = 1


class _PartitionMetadata(Protocol):
    replicas: list[int]


class _TopicMetadata(Protocol):
    partitions: dict[int, _PartitionMetadata]


def topic_specs(settings: Settings) -> tuple[TopicSpec, ...]:
    """Return the immutable Phase 1 topic topology."""
    return (
        TopicSpec(settings.KAFKA_ORDER_TOPIC, partitions=6),
        TopicSpec(settings.KAFKA_LOG_TOPIC, partitions=6),
        TopicSpec(settings.KAFKA_DLQ_TOPIC, partitions=3),
    )


def ensure_topics(settings: Settings, timeout_seconds: float = 30.0) -> list[str]:
    """Create missing topics and reject incompatible existing topology."""
    client = AdminClient({"bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS})
    metadata = client.list_topics(timeout=timeout_seconds)
    specs = topic_specs(settings)
    missing = [spec for spec in specs if spec.name not in metadata.topics]
    mismatches = [
        mismatch
        for spec in specs
        if spec.name in metadata.topics
        and (
            mismatch := topic_topology_mismatch(
                spec, cast(_TopicMetadata, metadata.topics[spec.name])
            )
        )
    ]
    if mismatches:
        raise RuntimeError("Existing Kafka topic topology mismatch: " + "; ".join(mismatches))
    if not missing:
        return []

    futures = client.create_topics(
        [
            NewTopic(
                topic=spec.name,
                num_partitions=spec.partitions,
                replication_factor=spec.replication_factor,
            )
            for spec in missing
        ]
    )
    for _topic_name, future in futures.items():
        future.result(timeout=timeout_seconds)
    return [spec.name for spec in missing]


def topic_topology_mismatch(spec: TopicSpec, metadata: _TopicMetadata) -> str | None:
    """Describe a partition or replication mismatch for one existing topic."""
    actual_partitions = len(metadata.partitions)
    replication_factors = {len(partition.replicas) for partition in metadata.partitions.values()}
    if actual_partitions != spec.partitions:
        return f"{spec.name} has {actual_partitions} partitions; expected {spec.partitions}"
    if replication_factors != {spec.replication_factor}:
        actual = ",".join(str(value) for value in sorted(replication_factors)) or "none"
        return f"{spec.name} has replication factor(s) {actual}; expected {spec.replication_factor}"
    return None
