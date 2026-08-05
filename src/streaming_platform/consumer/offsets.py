"""Per-partition contiguous manual-offset tracking."""

from dataclasses import dataclass, field
from threading import Lock

from streaming_platform.metrics.log_aggregation import KafkaCoordinate

PartitionKey = tuple[str, int]


@dataclass(slots=True)
class _PartitionState:
    committed_next: int
    offsets: dict[int, bool] = field(default_factory=dict)


class ContiguousOffsetTracker:
    """Advance commits only across consecutive durably completed source offsets."""

    def __init__(self) -> None:
        """Initialize empty per-partition state."""
        self._lock = Lock()
        self._states: dict[PartitionKey, _PartitionState] = {}

    def observe(self, coordinate: KafkaCoordinate) -> None:
        """Register a polled offset as pending before downstream processing."""
        key = (coordinate.topic, coordinate.partition)
        with self._lock:
            state = self._states.setdefault(key, _PartitionState(coordinate.offset))
            if coordinate.offset >= state.committed_next:
                state.offsets.setdefault(coordinate.offset, False)

    def complete(self, coordinate: KafkaCoordinate) -> None:
        """Mark one observed offset durable without crossing earlier pending work."""
        key = (coordinate.topic, coordinate.partition)
        with self._lock:
            state = self._states.get(key)
            if state is None or coordinate.offset not in state.offsets:
                raise ValueError(f"offset was not observed: {coordinate}")
            state.offsets[coordinate.offset] = True

    def complete_many(self, coordinates: tuple[KafkaCoordinate, ...]) -> None:
        """Mark every coordinate from a successful snapshot transaction durable."""
        for coordinate in coordinates:
            self.complete(coordinate)

    def committable(self, partitions: set[PartitionKey] | None = None) -> dict[PartitionKey, int]:
        """Return each partition's highest consecutive completed next offset."""
        result: dict[PartitionKey, int] = {}
        with self._lock:
            for key, state in self._states.items():
                if partitions is not None and key not in partitions:
                    continue
                candidate = state.committed_next
                while state.offsets.get(candidate) is True:
                    candidate += 1
                if candidate > state.committed_next:
                    result[key] = candidate
        return result

    def acknowledge_commit(self, key: PartitionKey, next_offset: int) -> None:
        """Discard tracking state only after Kafka confirms an explicit commit."""
        with self._lock:
            state = self._states[key]
            if next_offset < state.committed_next:
                raise ValueError("committed offset cannot move backwards")
            for offset in tuple(state.offsets):
                if offset < next_offset:
                    del state.offsets[offset]
            state.committed_next = next_offset

    def forget(self, partitions: set[PartitionKey]) -> None:
        """Remove successfully revoked partition state after its safe commit."""
        with self._lock:
            for key in partitions:
                self._states.pop(key, None)

    def has_pending(self, partitions: set[PartitionKey] | None = None) -> bool:
        """Return whether selected partitions contain non-durable offsets."""
        with self._lock:
            return any(
                not completed
                for key, state in self._states.items()
                if partitions is None or key in partitions
                for completed in state.offsets.values()
            )
