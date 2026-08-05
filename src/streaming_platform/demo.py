"""Small mixed-flow and real consumer-restart demonstration."""

import logging
import os
import sys
import tempfile
from pathlib import Path
from time import monotonic, sleep
from typing import cast
from uuid import uuid4

from confluent_kafka import Consumer, Message, Producer, TopicPartition
from sqlalchemy import Engine, func, select

from streaming_platform.benchmark.config import BenchmarkConfig, BenchmarkProfile
from streaming_platform.benchmark.processes import ProcessManager
from streaming_platform.benchmark.report import BenchmarkStatus, write_benchmark_report
from streaming_platform.benchmark.runner import BenchmarkRunner, consumed_from_ranges
from streaming_platform.config import Settings
from streaming_platform.consumer.order import OrderProcessor, ProcessingResult
from streaming_platform.consumer.retry import RetryPolicy
from streaming_platform.database.models import ValidOrder
from streaming_platform.database.order_repository import OrderRepository
from streaming_platform.database.session import create_database_engine, create_session_factory
from streaming_platform.generator.factory import EventFactory
from streaming_platform.generator.report import ProducerReport
from streaming_platform.kafka.consumer import ConsumerClient, OrderKafkaConsumer
from streaming_platform.kafka.dlq import DlqProducer
from streaming_platform.kafka.lag import LagInspector
from streaming_platform.models import EventType


class DemoRunner:
    """Run the mixed benchmark path plus bounded restart/replay scenarios."""

    def __init__(
        self,
        settings: Settings,
        logger: logging.LoggerAdapter[logging.Logger],
    ) -> None:
        """Store application dependencies."""
        self._settings = settings
        self._logger = logger
        self._seed = uuid4().int & 0x7FFFFFFF

    def run(self) -> tuple[int, Path]:
        """Run a small mixed demo and augment its report with recovery evidence."""
        config = BenchmarkConfig.from_profile(
            BenchmarkProfile.CUSTOM,
            target_eps=20,
            duration_seconds=5,
            order_ratio=0.5,
            log_ratio=0.5,
            invalid_rate=0.1,
            duplicate_rate=0.2,
            timeout_seconds=120,
            poll_interval_seconds=0.25,
            seed=self._seed,
        )
        report, path = BenchmarkRunner(self._settings, config, self._logger).run()
        if report.exit_code:
            return report.exit_code, path
        try:
            recovery = self._run_stop_and_recovery()
            replay = self._run_uncommitted_replay()
            report.consumer["failure_scenario"] = recovery
            report.consumer["restart_recovery_time_seconds"] = recovery[
                "restart_recovery_time_seconds"
            ]
            report.consumer["restart_recovery_status"] = "measured"
            report.consumer["uncommitted_replay"] = replay
            if (
                int(report.database.get("valid_orders_count", 0)) < 1
                or int(report.database.get("log_processed_events_count", 0)) < 1
                or int(report.consumer.get("dlq_count", 0)) < 1
            ):
                raise RuntimeError("mixed demo did not produce every required durable outcome")
        except Exception as error:
            report.status = BenchmarkStatus.FAILED
            report.exit_code = 1
            report.failure_stage = "failure_demo"
            report.errors.append(f"{type(error).__name__}: {error}")
            self._logger.exception("failure demo failed")
        write_benchmark_report(report, path)
        return report.exit_code, path

    def _run_stop_and_recovery(self) -> dict[str, object]:
        with tempfile.TemporaryDirectory(prefix="kafka-demo-recovery-") as temporary:
            directory = Path(temporary)
            manager = ProcessManager(directory)
            producer_path = directory / "producer.json"
            try:
                order = manager.start(
                    "order-consumer", [sys.executable, "-m", "apps.order_consumer"]
                )
                environment = os.environ.copy()
                environment["LOG_CONSUMER_FLUSH_INTERVAL_SECONDS"] = "0.5"
                log = manager.start(
                    "log-consumer",
                    [sys.executable, "-m", "apps.log_consumer"],
                    environment=environment,
                )
                self._wait_lag_zero(30)
                manager.stop(order)
                manager.stop(log)

                generator = manager.start(
                    "producer-while-stopped",
                    [
                        sys.executable,
                        "-m",
                        "apps.event_generator",
                        "--events-per-second",
                        "20",
                        "--duration-seconds",
                        "3",
                        "--order-ratio",
                        "0.5",
                        "--log-ratio",
                        "0.5",
                        "--application-error-ratio",
                        "0.5",
                        "--invalid-rate",
                        "0.1",
                        "--duplicate-rate",
                        "0.2",
                        "--seed",
                        str(self._seed + 1),
                        "--report-path",
                        str(producer_path),
                    ],
                )
                generator.process.wait(timeout=30)
                manager.stop(generator)
                if generator.process.returncode:
                    raise RuntimeError(
                        f"producer while stopped exited {generator.process.returncode}"
                    )
                produced = ProducerReport.model_validate_json(
                    producer_path.read_text(encoding="utf-8")
                )
                raised_snapshot = LagInspector(self._settings).inspect()
                observed_lag = sum(
                    row.lag or 0 for group in raised_snapshot.groups for row in group.rows
                )
                before_consumed = consumed_from_ranges(
                    produced.delivered_offset_ranges, raised_snapshot
                )
                if observed_lag <= 0 or before_consumed >= produced.delivered:
                    raise RuntimeError("consumer lag did not rise while consumers were stopped")

                recovery_started = monotonic()
                manager.start(
                    "order-consumer-restarted",
                    [sys.executable, "-m", "apps.order_consumer"],
                )
                manager.start(
                    "log-consumer-restarted",
                    [sys.executable, "-m", "apps.log_consumer"],
                    environment=environment,
                )
                deadline = monotonic() + 60
                final_consumed = before_consumed
                final_snapshot = raised_snapshot
                while monotonic() < deadline:
                    final_snapshot = LagInspector(self._settings).inspect()
                    final_consumed = consumed_from_ranges(
                        produced.delivered_offset_ranges, final_snapshot
                    )
                    if final_consumed == produced.delivered:
                        break
                    sleep(0.25)
                if final_consumed != produced.delivered:
                    raise TimeoutError("consumer restart did not drain the demo backlog")
                recovery_seconds = monotonic() - recovery_started
                final_available_lag = sum(
                    row.lag or 0 for group in final_snapshot.groups for row in group.rows
                )
                return {
                    "kind": "real_consumer_process_stop_restart",
                    "produced_while_stopped": produced.delivered,
                    "observed_lag_after_stop": observed_lag,
                    "final_available_lag": final_available_lag,
                    "restart_recovery_time_seconds": recovery_seconds,
                    "poll_interval_seconds": 0.25,
                    "timeout_seconds": 60,
                    "success_condition": "all run source offsets durably committed",
                }
            finally:
                forced = manager.stop_all()
                if forced:
                    raise RuntimeError("forced cleanup required: " + ", ".join(forced))

    def _run_uncommitted_replay(self) -> dict[str, object]:
        factory = EventFactory(self._settings, seed=self._seed + 2)
        record = factory.create_normal(EventType.ORDER_CREATED)
        position = self._produce(record.key, record.value)
        raw = Consumer(
            {
                "bootstrap.servers": self._settings.KAFKA_BOOTSTRAP_SERVERS,
                "group.id": self._settings.KAFKA_ORDER_CONSUMER_GROUP,
                "enable.auto.commit": False,
                "enable.auto.offset.store": False,
            }
        )
        wrapped = OrderKafkaConsumer(
            self._settings, consumer=cast(ConsumerClient, raw)
        )
        raw.assign([position])
        message = wrapped.poll(10)
        if message is None:
            wrapped.close()
            raise TimeoutError("uncommitted replay source message was not readable")
        engine = create_database_engine(self._settings)
        processor = OrderProcessor(
            create_session_factory(engine),
            OrderRepository(),
            DlqProducer(self._settings),
            self._settings.KAFKA_ORDER_CONSUMER_GROUP,
            RetryPolicy(max_retries=0),
            self._logger,
        )
        try:
            result = processor.process(message)
            if result is not ProcessingResult.PROCESSED:
                raise RuntimeError(f"expected new event before replay, received {result}")
        finally:
            wrapped.close()  # Deliberately omit commit to reproduce the crash window.
        before = self._order_event_count(engine, record.event_id)
        with tempfile.TemporaryDirectory(prefix="kafka-demo-replay-") as temporary:
            manager = ProcessManager(Path(temporary))
            try:
                manager.start(
                    "order-consumer-replay",
                    [sys.executable, "-m", "apps.order_consumer"],
                )
                deadline = monotonic() + 30
                committed = position.offset
                while monotonic() < deadline:
                    committed = self._committed_offset(position)
                    if committed >= position.offset + 1:
                        break
                    sleep(0.25)
                if committed < position.offset + 1:
                    raise TimeoutError("replayed source offset was not committed")
            finally:
                forced = manager.stop_all()
                if forced:
                    raise RuntimeError("forced replay cleanup required")
        after = self._order_event_count(engine, record.event_id)
        engine.dispose()
        if before != 1 or after != 1:
            raise RuntimeError("uncommitted replay created duplicate business data")
        return {
            "kind": "deterministic_postgres_commit_before_kafka_commit_simulation",
            "database_rows_before_restart": before,
            "database_rows_after_restart": after,
            "final_committed_offset": committed,
            "status": "passed",
        }

    def _wait_lag_zero(self, timeout: float) -> None:
        deadline = monotonic() + timeout
        while monotonic() < deadline:
            snapshot = LagInspector(self._settings).inspect()
            measured = [
                row.lag for group in snapshot.groups for row in group.rows if row.lag is not None
            ]
            if measured and all(value == 0 for value in measured):
                return
            sleep(0.25)
        raise TimeoutError("timed out waiting for zero baseline lag")

    def _produce(self, key: bytes, value: bytes) -> TopicPartition:
        positions: list[TopicPartition] = []
        errors: list[str] = []

        def delivered(error: object | None, message: Message) -> None:
            if error is not None:
                errors.append(str(error))
            else:
                topic = message.topic()
                partition = message.partition()
                offset = message.offset()
                if topic is None or partition is None or offset is None:
                    errors.append("delivery callback omitted broker coordinates")
                    return
                positions.append(
                    TopicPartition(topic, partition, offset)
                )

        producer = Producer({"bootstrap.servers": self._settings.KAFKA_BOOTSTRAP_SERVERS})
        producer.produce(
            self._settings.KAFKA_ORDER_TOPIC,
            key=key,
            value=value,
            on_delivery=delivered,
        )
        remaining = producer.flush(10)
        if remaining or errors or not positions:
            raise RuntimeError(errors[0] if errors else "demo record delivery failed")
        return positions[0]

    def _committed_offset(self, position: TopicPartition) -> int:
        observer = Consumer(
            {
                "bootstrap.servers": self._settings.KAFKA_BOOTSTRAP_SERVERS,
                "group.id": self._settings.KAFKA_ORDER_CONSUMER_GROUP,
                "enable.auto.commit": False,
            }
        )
        try:
            committed = observer.committed(
                [TopicPartition(position.topic, position.partition)], timeout=10
            )[0]
            return committed.offset
        finally:
            observer.close()

    @staticmethod
    def _order_event_count(engine: Engine, event_id: object) -> int:
        factory = create_session_factory(engine)
        with factory() as session:
            value = session.scalar(
                select(func.count()).select_from(ValidOrder).where(
                    ValidOrder.event_id == event_id
                )
            )
        return int(value or 0)
