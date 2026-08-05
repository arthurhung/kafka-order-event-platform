"""End-to-end benchmark orchestration over real Kafka and PostgreSQL."""

import json
import logging
import os
import subprocess
import sys
import tempfile
import zlib
from datetime import UTC, datetime
from pathlib import Path
from time import monotonic, sleep
from typing import Any, cast
from uuid import uuid4

from confluent_kafka import Consumer, TopicPartition
from sqlalchemy import and_, func, or_, select

from streaming_platform.benchmark.config import BenchmarkConfig
from streaming_platform.benchmark.environment import collect_environment
from streaming_platform.benchmark.identity import create_run_id, report_path
from streaming_platform.benchmark.processes import ManagedProcess, ProcessManager
from streaming_platform.benchmark.report import (
    BenchmarkReport,
    LatencyMetric,
    status_for_run,
    write_benchmark_report,
)
from streaming_platform.config import Settings
from streaming_platform.database.models import LogMetricMinute, ProcessedEvent, ValidOrder
from streaming_platform.database.session import create_database_engine, create_session_factory
from streaming_platform.generator.report import DeliveredOffsetRange, ProducerReport
from streaming_platform.kafka.lag import LagInspector, LagSnapshot


class BenchmarkStageError(RuntimeError):
    """A stage-specific failure that should still produce a JSON report."""


class BenchmarkRunner:
    """Run one workload, wait for durable consumption, and write evidence."""

    def __init__(
        self,
        settings: Settings,
        config: BenchmarkConfig,
        logger: logging.LoggerAdapter[logging.Logger],
    ) -> None:
        """Store validated settings and workload configuration."""
        self._settings = settings
        self._config = config
        self._logger = logger
        self._effective_seed = 0

    def run(self) -> tuple[BenchmarkReport, Path]:
        """Execute all bounded stages and always attempt to write a final report."""
        run_id = create_run_id()
        self._effective_seed = (
            self._config.seed
            if self._config.seed is not None
            else zlib.crc32(run_id.encode("utf-8"))
        )
        output_path = report_path(
            self._config.report_directory, run_id, self._config.profile.value
        )
        started_at = datetime.now(UTC)
        started_monotonic = monotonic()
        workload_started: float | None = None
        durable_finished: float | None = None
        stage = "initialization"
        errors: list[str] = []
        limitations = [
            "producer_delivery_latency_ms is not end-to-end latency",
            "end_to_end_latency_ms is not implemented for the current schemas",
            "observed max lag is limited by the configured polling interval",
            "local single-node results do not represent production capacity",
        ]
        producer_data: dict[str, Any] = {}
        consumer_data: dict[str, Any] = {}
        database_data: dict[str, Any] = {}
        ranges: list[DeliveredOffsetRange] = []
        lag_samples: list[LagSnapshot] = []
        failed = False
        unavailable_required = False
        exit_code = 0

        with tempfile.TemporaryDirectory(prefix=f"kafka-benchmark-{run_id[:20]}-") as temp:
            temp_directory = Path(temp)
            manager = ProcessManager(temp_directory)
            producer_report_path = temp_directory / "producer.json"
            dlq_start: dict[int, int] = {}
            try:
                stage = "infrastructure"
                self._prepare_infrastructure()
                stage = "consumer_start"
                order = manager.start(
                    "order-consumer", [sys.executable, "-m", "apps.order_consumer"]
                )
                log_environment = os.environ.copy()
                log_environment["LOG_CONSUMER_FLUSH_INTERVAL_SECONDS"] = str(
                    min(1.0, self._settings.LOG_CONSUMER_FLUSH_INTERVAL_SECONDS)
                )
                log = manager.start(
                    "log-consumer",
                    [sys.executable, "-m", "apps.log_consumer"],
                    environment=log_environment,
                )
                stage = "consumer_readiness"
                self._wait_for_baseline(order, log, lag_samples)
                dlq_start = self._topic_high_watermarks(self._settings.KAFKA_DLQ_TOPIC)

                stage = "producer"
                workload_started = monotonic()
                generator = manager.start(
                    "event-generator",
                    self._generator_arguments(producer_report_path),
                )
                self._wait_for_process(generator, lag_samples)
                manager.stop(generator)
                if generator.process.returncode != 0:
                    raise BenchmarkStageError(
                        f"event generator exited with code {generator.process.returncode}"
                    )
                producer_report = ProducerReport.model_validate_json(
                    producer_report_path.read_text(encoding="utf-8")
                )
                producer_data = self._producer_data(producer_report)
                ranges = producer_report.delivered_offset_ranges
                if sum(
                    item.end_offset_exclusive - item.start_offset for item in ranges
                ) != producer_report.delivered:
                    raise BenchmarkStageError(
                        "producer broker coordinates do not cover every delivered record"
                    )

                stage = "durable_processing"
                final_snapshot, consumed_count = self._wait_for_consumption(
                    ranges, producer_report.delivered, order, log, lag_samples
                )
                durable_finished = monotonic()
                stage = "dlq_observation"
                dlq_count = self._count_run_dlq(dlq_start, ranges)
                stage = "database_observation"
                database_data = self._database_counts(ranges)
                numeric_lag = numeric_benchmark_lag_samples(lag_samples)
                final_lag = benchmark_lag(final_snapshot)
                unavailable_required = not numeric_lag or final_lag is None
                consumer_data = {
                    "consumed_count": consumed_count,
                    "duplicate_count": producer_report.duplicate_events_delivered,
                    "dlq_count": dlq_count,
                    "observed_max_lag": max(numeric_lag) if numeric_lag else None,
                    "final_lag": final_lag,
                    "lag_scope": (
                        "all partitions with committed offsets plus empty uncommitted partitions"
                    ),
                    "lag_poll_interval_seconds": self._config.poll_interval_seconds,
                    "lag_sample_count": len(lag_samples),
                    "final_groups": [
                        group.model_dump(mode="json") for group in final_snapshot.groups
                    ],
                    "restart_recovery_time_seconds": None,
                    "restart_recovery_status": "not_measured_by_benchmark",
                }
                if producer_report.failed:
                    failed = True
                    errors.append(f"producer reported {producer_report.failed} failed deliveries")
                    exit_code = 1
            except Exception as error:
                failed = True
                exit_code = 1
                errors.append(f"{type(error).__name__}: {error}")
                self._logger.exception("benchmark stage failed", extra={"stage": stage})
            finally:
                forced = manager.stop_all()
                if forced:
                    failed = True
                    exit_code = 1
                    errors.append("forced process termination: " + ", ".join(forced))

        finished_at = datetime.now(UTC)
        if not consumer_data and lag_samples:
            final_snapshot = lag_samples[-1]
            numeric_lag = numeric_benchmark_lag_samples(lag_samples)
            consumer_data = {
                "consumed_count": consumed_from_ranges(ranges, final_snapshot),
                "duplicate_count": producer_data.get("duplicate_events_delivered"),
                "dlq_count": None,
                "dlq_count_status": "not_available_after_failed_stage",
                "observed_max_lag": max(numeric_lag) if numeric_lag else None,
                "final_lag": benchmark_lag(final_snapshot),
                "lag_scope": (
                    "all partitions with committed offsets plus empty uncommitted partitions"
                ),
                "lag_poll_interval_seconds": self._config.poll_interval_seconds,
                "lag_sample_count": len(lag_samples),
                "final_groups": [
                    group.model_dump(mode="json") for group in final_snapshot.groups
                ],
                "restart_recovery_time_seconds": None,
                "restart_recovery_status": "not_measured_by_benchmark",
            }
            unavailable_required = True
        status = status_for_run(
            failed=failed, unavailable_required_metric=unavailable_required
        )
        latency = self._latency_data(producer_data)
        report = BenchmarkReport(
            run_id=run_id,
            profile=self._config.profile.value,
            started_at=started_at,
            finished_at=finished_at,
            status=status,
            failure_stage=stage if failed else None,
            exit_code=exit_code,
            environment=collect_environment(),
            configuration=self._configuration_data(),
            producer=producer_data,
            consumer=consumer_data,
            latency=latency,
            runtime={
                "total_runtime_seconds": monotonic() - started_monotonic,
                "workload_runtime_seconds": (
                    durable_finished - workload_started
                    if workload_started is not None and durable_finished is not None
                    else None
                ),
                "definition": "generation start through durable source-offset completion",
            },
            database=database_data,
            run_scope={
                "method": "exact delivered Kafka topic/partition/offset intervals",
                "database_counts": "current_run",
                "delivered_offset_ranges": [
                    item.model_dump(mode="json") for item in ranges
                ],
            },
            errors=errors,
            limitations=limitations,
            artifacts={"report_path": str(output_path)},
        )
        write_benchmark_report(report, output_path)
        return report, output_path

    def _prepare_infrastructure(self) -> None:
        commands = (
            (["docker", "compose", "up", "-d", "kafka", "postgres", "kafka-ui"], 180),
            ([sys.executable, "scripts/wait_for_services.py", "--timeout", "120"], 130),
            ([sys.executable, "scripts/create_topics.py"], 60),
            ([sys.executable, "-m", "alembic", "upgrade", "head"], 60),
        )
        for command, timeout in commands:
            try:
                subprocess.run(command, check=True, timeout=timeout)  # noqa: S603
            except subprocess.SubprocessError as error:
                raise BenchmarkStageError(
                    f"command failed: {' '.join(command)}: {error}"
                ) from error

    def _wait_for_baseline(
        self,
        order: ManagedProcess,
        log: ManagedProcess,
        samples: list[LagSnapshot],
    ) -> None:
        deadline = monotonic() + min(60.0, self._config.timeout_seconds / 3)
        while monotonic() < deadline:
            self._raise_if_child_failed(order, log)
            snapshot = LagInspector(self._settings).inspect()
            samples.append(snapshot)
            numeric_rows = [
                row.lag for group in snapshot.groups for row in group.rows if row.lag is not None
            ]
            if all(value == 0 for value in numeric_rows) and all(
                group.status != "group_not_established" for group in snapshot.groups
            ):
                return
            sleep(self._config.poll_interval_seconds)
        raise TimeoutError("timed out waiting for consumers to drain existing backlog")

    def _wait_for_process(
        self, process: ManagedProcess, samples: list[LagSnapshot]
    ) -> None:
        deadline = (
            monotonic()
            + self._config.duration_seconds
            + self._config.timeout_seconds
        )
        while process.process.poll() is None:
            if monotonic() >= deadline:
                raise TimeoutError("event generator exceeded benchmark timeout")
            samples.append(LagInspector(self._settings).inspect())
            sleep(self._config.poll_interval_seconds)

    def _wait_for_consumption(
        self,
        ranges: list[DeliveredOffsetRange],
        delivered: int,
        order: ManagedProcess,
        log: ManagedProcess,
        samples: list[LagSnapshot],
    ) -> tuple[LagSnapshot, int]:
        deadline = monotonic() + self._config.timeout_seconds
        final: LagSnapshot | None = None
        consumed = 0
        while monotonic() < deadline:
            self._raise_if_child_failed(order, log)
            final = LagInspector(self._settings).inspect()
            samples.append(final)
            consumed = consumed_from_ranges(ranges, final)
            if consumed == delivered:
                return final, consumed
            sleep(self._config.poll_interval_seconds)
        raise TimeoutError(f"durable processing timed out at {consumed}/{delivered} records")

    @staticmethod
    def _raise_if_child_failed(*processes: ManagedProcess) -> None:
        for managed in processes:
            code = managed.process.poll()
            if code is not None:
                raise BenchmarkStageError(f"{managed.name} exited unexpectedly with code {code}")

    def _generator_arguments(self, report: Path) -> list[str]:
        return [
            sys.executable,
            "-m",
            "apps.event_generator",
            "--events-per-second",
            str(self._config.target_eps),
            "--duration-seconds",
            str(self._config.duration_seconds),
            "--order-ratio",
            str(self._config.order_ratio),
            "--log-ratio",
            str(self._config.log_ratio),
            "--application-error-ratio",
            str(self._config.application_error_ratio),
            "--invalid-rate",
            str(self._config.invalid_rate),
            "--duplicate-rate",
            str(self._config.duplicate_rate),
            "--seed",
            str(self._effective_seed),
            "--report-path",
            str(report),
        ]

    def _topic_high_watermarks(self, topic: str) -> dict[int, int]:
        consumer = Consumer(
            {
                "bootstrap.servers": self._settings.KAFKA_BOOTSTRAP_SERVERS,
                "group.id": f"benchmark-watermark-{uuid4()}",
                "enable.auto.commit": False,
            }
        )
        try:
            metadata = consumer.list_topics(topic, timeout=10)
            return {
                partition: consumer.get_watermark_offsets(
                    TopicPartition(topic, partition), timeout=10
                )[1]
                for partition in metadata.topics[topic].partitions
            }
        finally:
            consumer.close()

    def _count_run_dlq(
        self,
        starts: dict[int, int],
        ranges: list[DeliveredOffsetRange],
    ) -> int:
        ends = self._topic_high_watermarks(self._settings.KAFKA_DLQ_TOPIC)
        expected = sum(max(ends[partition] - start, 0) for partition, start in starts.items())
        if expected == 0:
            return 0
        consumer = Consumer(
            {
                "bootstrap.servers": self._settings.KAFKA_BOOTSTRAP_SERVERS,
                "group.id": f"benchmark-dlq-{uuid4()}",
                "enable.auto.commit": False,
            }
        )
        consumer.assign(
            [
                TopicPartition(self._settings.KAFKA_DLQ_TOPIC, partition, start)
                for partition, start in starts.items()
            ]
        )
        seen = 0
        matched = 0
        deadline = monotonic() + min(120.0, self._config.timeout_seconds)
        try:
            while seen < expected and monotonic() < deadline:
                message = consumer.poll(0.25)
                if message is None:
                    continue
                if message.error() is not None:
                    raise BenchmarkStageError(f"DLQ observation failed: {message.error()}")
                partition = message.partition()
                offset = message.offset()
                value = message.value()
                if partition is None or offset is None:
                    raise BenchmarkStageError("DLQ record did not expose broker coordinates")
                if offset >= ends[partition]:
                    continue
                seen += 1
                if value is None:
                    continue
                try:
                    body = json.loads(value)
                except (json.JSONDecodeError, UnicodeDecodeError, TypeError):
                    continue
                coordinate = (
                    body.get("original_topic"),
                    body.get("original_partition"),
                    body.get("original_offset"),
                )
                if coordinate_in_ranges(coordinate, ranges):
                    matched += 1
        finally:
            consumer.close()
        if seen != expected:
            raise TimeoutError(f"read {seen}/{expected} new DLQ records")
        return matched

    def _database_counts(self, ranges: list[DeliveredOffsetRange]) -> dict[str, Any]:
        engine = create_database_engine(self._settings)
        processed_conditions = [
            and_(
                ProcessedEvent.topic == item.topic,
                ProcessedEvent.partition_id == item.partition,
                ProcessedEvent.offset_id >= item.start_offset,
                ProcessedEvent.offset_id < item.end_offset_exclusive,
            )
            for item in ranges
        ]
        order_conditions = [
            and_(
                ValidOrder.kafka_topic == item.topic,
                ValidOrder.kafka_partition == item.partition,
                ValidOrder.kafka_offset >= item.start_offset,
                ValidOrder.kafka_offset < item.end_offset_exclusive,
            )
            for item in ranges
            if item.topic == self._settings.KAFKA_ORDER_TOPIC
        ]
        try:
            with create_session_factory(engine)() as session:
                processed = session.scalar(
                    select(func.count()).select_from(ProcessedEvent).where(
                        or_(*processed_conditions)
                    )
                ) if processed_conditions else 0
                valid_orders = session.scalar(
                    select(func.count()).select_from(ValidOrder).where(
                        or_(*order_conditions)
                    )
                ) if order_conditions else 0
                log_processed = session.scalar(
                    select(func.count()).select_from(ProcessedEvent).where(
                        ProcessedEvent.consumer_group
                        == self._settings.KAFKA_LOG_CONSUMER_GROUP,
                        or_(*processed_conditions),
                    )
                ) if processed_conditions else 0
                metric_rows = session.scalar(
                    select(func.count()).select_from(LogMetricMinute)
                )
        finally:
            engine.dispose()
        return {
            "scope": "current_run_source_coordinates",
            "processed_events_count": int(processed or 0),
            "valid_orders_count": int(valid_orders or 0),
            "log_processed_events_count": int(log_processed or 0),
            "log_metric_rows_total": int(metric_rows or 0),
            "log_metric_rows_total_scope": "whole_table",
        }

    def _producer_data(self, report: ProducerReport) -> dict[str, Any]:
        return {
            "attempted": report.attempted,
            "delivered": report.delivered,
            "failed": report.failed,
            "actual_eps": report.actual_events_per_second,
            "elapsed_seconds": report.elapsed_seconds,
            "order_events_attempted": report.order_events_attempted,
            "log_events_attempted": report.log_events_attempted,
            "invalid_events_injected": report.invalid_events_injected,
            "duplicate_events_attempted": report.duplicate_events_injected,
            "duplicate_events_delivered": report.duplicate_events_delivered,
            "delivery_latency_sample_count": report.producer_delivery_latency_sample_count,
            "delivery_latency_average_ms": report.producer_delivery_latency_average_ms,
            "delivery_latency_p95_ms": report.producer_delivery_latency_p95_ms,
            "delivery_latency_p99_ms": report.producer_delivery_latency_p99_ms,
        }

    def _latency_data(self, producer: dict[str, Any]) -> dict[str, LatencyMetric]:
        samples = int(producer.get("delivery_latency_sample_count", 0))
        return {
            "producer_delivery_latency_ms": LatencyMetric(
                status="measured" if samples else "not_available",
                definition=(
                    "monotonic time from first local produce attempt to successful "
                    "Kafka delivery callback"
                ),
                sample_count=samples,
                average_ms=cast(float | None, producer.get("delivery_latency_average_ms")),
                p95_ms=cast(float | None, producer.get("delivery_latency_p95_ms")),
                p99_ms=cast(float | None, producer.get("delivery_latency_p99_ms")),
                reason=None if samples else "no successful delivery samples",
            ),
            "end_to_end_latency_ms": LatencyMetric(
                status="not_implemented",
                definition="event creation through durable downstream completion",
                sample_count=0,
                average_ms=None,
                p95_ms=None,
                p99_ms=None,
                reason="current schemas do not expose one reliable endpoint for all outcomes",
            ),
        }

    def _configuration_data(self) -> dict[str, Any]:
        return {
            "profile_target": {
                "target_eps": self._config.profile_target_eps,
                "duration_seconds": self._config.profile_duration_seconds,
            },
            "effective": {
                "target_eps": self._config.target_eps,
                "duration_seconds": self._config.duration_seconds,
                "order_ratio": self._config.order_ratio,
                "log_ratio": self._config.log_ratio,
                "application_error_ratio": self._config.application_error_ratio,
                "invalid_rate": self._config.invalid_rate,
                "duplicate_rate": self._config.duplicate_rate,
                "seed": self._effective_seed,
                "seed_source": "explicit" if self._config.seed is not None else "run_id",
                "producer_timeout_seconds": (
                    self._config.duration_seconds + self._config.timeout_seconds
                ),
                "drain_timeout_seconds": self._config.timeout_seconds,
            },
            "adjusted_from_profile": self._config.adjusted,
            "topic_partitions": {
                self._settings.KAFKA_ORDER_TOPIC: 6,
                self._settings.KAFKA_LOG_TOPIC: 6,
                self._settings.KAFKA_DLQ_TOPIC: 3,
            },
        }


def consumed_from_ranges(
    ranges: list[DeliveredOffsetRange], snapshot: LagSnapshot
) -> int:
    """Count run records below the real committed next offset for their group."""
    current = {
        (row.topic, row.partition): row.current_offset
        for group in snapshot.groups
        for row in group.rows
    }
    consumed = 0
    for item in ranges:
        committed = current.get((item.topic, item.partition))
        if committed is None:
            continue
        consumed += max(
            min(committed, item.end_offset_exclusive) - item.start_offset,
            0,
        )
    return consumed


def benchmark_lag(snapshot: LagSnapshot) -> int | None:
    """Sum numeric lag while treating only provably empty uncommitted partitions as zero."""
    rows = [row for group in snapshot.groups for row in group.rows]
    if any(row.lag is None and row.log_end_offset not in (0,) for row in rows):
        return None
    return sum(row.lag or 0 for row in rows)


def numeric_benchmark_lag_samples(samples: list[LagSnapshot]) -> list[int]:
    """Return only benchmark lag samples whose total is measurable."""
    numeric: list[int] = []
    for sample in samples:
        value = benchmark_lag(sample)
        if value is not None:
            numeric.append(value)
    return numeric


def coordinate_in_ranges(
    coordinate: tuple[Any, Any, Any], ranges: list[DeliveredOffsetRange]
) -> bool:
    """Return whether a DLQ original coordinate belongs to this run."""
    topic, partition, offset = coordinate
    if not isinstance(topic, str) or not isinstance(partition, int) or not isinstance(offset, int):
        return False
    return any(
        item.topic == topic
        and item.partition == partition
        and item.start_offset <= offset < item.end_offset_exclusive
        for item in ranges
    )
