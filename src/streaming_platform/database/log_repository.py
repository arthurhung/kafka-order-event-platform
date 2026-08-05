"""Transactional idempotency and additive minute-metric persistence."""

from uuid import UUID

from sqlalchemy import func
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from streaming_platform.database.models import LogMetricMinute, ProcessedEvent
from streaming_platform.metrics.log_aggregation import AggregationSnapshot


class LogMetricRepository:
    """Persist idempotency markers and metric deltas without committing."""

    def persist_snapshot(
        self,
        session: Session,
        snapshot: AggregationSnapshot,
        consumer_group: str,
    ) -> set[UUID]:
        """Persist new event IDs and aggregate only those IDs in one transaction."""
        if not snapshot.events:
            return set()
        marker_values = []
        for event in snapshot.events:
            coordinate = event.coordinates[0]
            marker_values.append(
                {
                    "consumer_group": consumer_group,
                    "event_id": event.event_id,
                    "topic": coordinate.topic,
                    "partition_id": coordinate.partition,
                    "offset_id": coordinate.offset,
                }
            )
        marker = (
            insert(ProcessedEvent)
            .values(marker_values)
            .on_conflict_do_nothing(index_elements=["consumer_group", "event_id"])
            .returning(ProcessedEvent.event_id)
        )
        inserted_ids = set(session.execute(marker).scalars().all())
        aggregates = snapshot.aggregate(inserted_ids)
        if not aggregates:
            return inserted_ids

        rows = [
            {
                "metric_minute": key.metric_minute,
                "service": key.service,
                "endpoint": key.endpoint,
                "request_count": metric.request_count,
                "success_count": metric.success_count,
                "client_error_count": metric.client_error_count,
                "server_error_count": metric.server_error_count,
                "response_time_sum_ms": metric.response_time_sum_ms,
                "max_response_time_ms": metric.max_response_time_ms,
            }
            for key, metric in aggregates.items()
        ]
        statement = insert(LogMetricMinute).values(rows)
        excluded = statement.excluded
        upsert = statement.on_conflict_do_update(
            index_elements=["metric_minute", "service", "endpoint"],
            set_={
                "request_count": LogMetricMinute.request_count + excluded.request_count,
                "success_count": LogMetricMinute.success_count + excluded.success_count,
                "client_error_count": (
                    LogMetricMinute.client_error_count + excluded.client_error_count
                ),
                "server_error_count": (
                    LogMetricMinute.server_error_count + excluded.server_error_count
                ),
                "response_time_sum_ms": (
                    LogMetricMinute.response_time_sum_ms + excluded.response_time_sum_ms
                ),
                "max_response_time_ms": func.greatest(
                    LogMetricMinute.max_response_time_ms,
                    excluded.max_response_time_ms,
                ),
                "updated_at": func.now(),
            },
        )
        session.execute(upsert)
        session.flush()
        return inserted_ids
