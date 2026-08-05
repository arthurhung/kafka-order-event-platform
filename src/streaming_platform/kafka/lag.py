"""Real Kafka consumer-group lag inspection with human and JSON output."""

from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any, Protocol, cast

from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

from streaming_platform.config import Settings

INVALID_OFFSET = -1001


class TopicMissingError(RuntimeError):
    """Raised when a configured source topic does not exist."""


class LagRow(BaseModel):
    """One partition's committed and log-end offsets."""

    model_config = ConfigDict(extra="forbid")

    consumer_group: str
    topic: str
    partition: int
    current_offset: int | None
    log_end_offset: int | None
    lag: int | None
    status: str = "measured"


class LagGroup(BaseModel):
    """Lag rows and total for one configured consumer group and topic."""

    model_config = ConfigDict(extra="forbid")

    consumer_group: str
    topic: str
    status: str
    rows: list[LagRow] = Field(default_factory=list)
    total_lag: int | None = None


class LagSnapshot(BaseModel):
    """Point-in-time lag for both platform consumer groups."""

    model_config = ConfigDict(extra="forbid")

    observed_at: AwareDatetime
    groups: list[LagGroup]

    @property
    def combined_total_lag(self) -> int | None:
        """Return the sum only when every configured group has a numeric total."""
        totals = [group.total_lag for group in self.groups]
        if any(value is None for value in totals):
            return None
        return sum(cast(int, value) for value in totals)


class _ConsumerClient(Protocol):
    def committed(
        self, partitions: list[TopicPartition], timeout: float
    ) -> list[TopicPartition]: ...

    def get_watermark_offsets(
        self, partition: TopicPartition, timeout: float
    ) -> tuple[int, int]: ...

    def close(self) -> None: ...


class LagInspector:
    """Inspect configured group offsets without mutating them."""

    def __init__(
        self,
        settings: Settings,
        *,
        admin: object | None = None,
        consumer_factory: Callable[[str], _ConsumerClient] | None = None,
        timeout_seconds: float = 10.0,
    ) -> None:
        """Create Kafka clients or accept deterministic test doubles."""
        self._settings = settings
        self._admin: Any = admin or AdminClient(
            {"bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS}
        )
        self._consumer_factory = consumer_factory or self._create_consumer
        self._timeout = timeout_seconds

    def inspect(self) -> LagSnapshot:
        """Return one real-offset snapshot for both configured groups."""
        metadata = self._admin.list_topics(timeout=self._timeout)
        pairs = (
            (self._settings.KAFKA_ORDER_CONSUMER_GROUP, self._settings.KAFKA_ORDER_TOPIC),
            (self._settings.KAFKA_LOG_CONSUMER_GROUP, self._settings.KAFKA_LOG_TOPIC),
        )
        for _group, topic in pairs:
            if topic not in metadata.topics:
                raise TopicMissingError(f"configured Kafka topic does not exist: {topic}")
            error = getattr(metadata.topics[topic], "error", None)
            if error is not None:
                raise TopicMissingError(f"Kafka topic metadata failed for {topic}: {error}")

        group_result = self._admin.list_consumer_groups(
            request_timeout=self._timeout
        ).result(self._timeout + 1)
        if group_result.errors:
            raise RuntimeError(f"Kafka group listing failed: {group_result.errors[0]}")
        existing = {listing.group_id for listing in group_result.valid}
        groups = [
            self._inspect_group(group, topic, metadata.topics[topic], group in existing)
            for group, topic in pairs
        ]
        return LagSnapshot(observed_at=datetime.now(UTC), groups=groups)

    def _inspect_group(
        self,
        group: str,
        topic: str,
        topic_metadata: object,
        exists: bool,
    ) -> LagGroup:
        metadata = cast(Any, topic_metadata)
        partition_ids = sorted(metadata.partitions)
        if not exists:
            rows = [
                LagRow(
                    consumer_group=group,
                    topic=topic,
                    partition=partition,
                    current_offset=None,
                    log_end_offset=None,
                    lag=None,
                    status="group_not_established",
                )
                for partition in partition_ids
            ]
            return LagGroup(
                consumer_group=group,
                topic=topic,
                status="group_not_established",
                rows=rows,
            )

        consumer = self._consumer_factory(group)
        requested = [TopicPartition(topic, partition) for partition in partition_ids]
        try:
            committed = consumer.committed(requested, timeout=self._timeout)
            committed_by_partition = {item.partition: item.offset for item in committed}
            rows = [
                self._row(
                    consumer,
                    group,
                    topic,
                    partition,
                    committed_by_partition.get(partition, INVALID_OFFSET),
                )
                for partition in partition_ids
            ]
        finally:
            consumer.close()
        total = sum(cast(int, row.lag) for row in rows) if all(
            row.lag is not None for row in rows
        ) else None
        status = "measured" if total is not None else "partial"
        return LagGroup(
            consumer_group=group,
            topic=topic,
            status=status,
            rows=rows,
            total_lag=total,
        )

    def _row(
        self,
        consumer: _ConsumerClient,
        group: str,
        topic: str,
        partition: int,
        current: int,
    ) -> LagRow:
        _low, high = consumer.get_watermark_offsets(
            TopicPartition(topic, partition), timeout=self._timeout
        )
        if current == INVALID_OFFSET or current < 0:
            return LagRow(
                consumer_group=group,
                topic=topic,
                partition=partition,
                current_offset=None,
                log_end_offset=high,
                lag=None,
                status="offset_not_available",
            )
        lag = high - current
        if lag < 0:
            return LagRow(
                consumer_group=group,
                topic=topic,
                partition=partition,
                current_offset=current,
                log_end_offset=high,
                lag=None,
                status="invalid_negative_lag",
            )
        return LagRow(
            consumer_group=group,
            topic=topic,
            partition=partition,
            current_offset=current,
            log_end_offset=high,
            lag=lag,
        )

    def _create_consumer(self, group: str) -> _ConsumerClient:
        return cast(
            _ConsumerClient,
            Consumer(
                {
                    "bootstrap.servers": self._settings.KAFKA_BOOTSTRAP_SERVERS,
                    "group.id": group,
                    "enable.auto.commit": False,
                }
            ),
        )


def format_lag_table(snapshot: LagSnapshot) -> str:
    """Render a stable human-readable lag table and group totals."""
    headings = (
        "CONSUMER GROUP",
        "TOPIC",
        "PARTITION",
        "CURRENT OFFSET",
        "LOG END OFFSET",
        "LAG",
        "STATUS",
    )
    rows = [
        (
            row.consumer_group,
            row.topic,
            str(row.partition),
            _display(row.current_offset),
            _display(row.log_end_offset),
            _display(row.lag),
            row.status,
        )
        for group in snapshot.groups
        for row in group.rows
    ]
    widths = [
        max(len(headings[index]), *(len(row[index]) for row in rows))
        for index in range(len(headings))
    ]
    lines = ["  ".join(value.ljust(widths[index]) for index, value in enumerate(headings))]
    lines.extend(
        "  ".join(value.ljust(widths[index]) for index, value in enumerate(row))
        for row in rows
    )
    lines.append("")
    lines.extend(
        f"TOTAL {group.consumer_group} / {group.topic}: "
        f"{_display(group.total_lag)} ({group.status})"
        for group in snapshot.groups
    )
    return "\n".join(lines)


def _display(value: int | None) -> str:
    return "not_available" if value is None else str(value)
