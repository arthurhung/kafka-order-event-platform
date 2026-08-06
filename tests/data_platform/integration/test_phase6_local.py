import subprocess
from pathlib import Path
from shutil import which
from uuid import uuid4

import pytest
from sqlalchemy import inspect, select

from data_platform.fixtures import load_fixtures
from streaming_platform.database.models import LogMetricMinute, ProcessedEvent, ValidOrder
from streaming_platform.database.session import create_database_engine, create_session_factory


def _source_identity(settings) -> tuple[set, set, set]:
    engine = create_database_engine(settings)
    try:
        with create_session_factory(engine)() as session:
            return (
                set(session.scalars(select(ValidOrder.event_id))),
                set(
                    session.execute(
                        select(ProcessedEvent.consumer_group, ProcessedEvent.event_id)
                    ).tuples()
                ),
                set(
                    session.execute(
                        select(
                            LogMetricMinute.metric_minute,
                            LogMetricMinute.service,
                            LogMetricMinute.endpoint,
                        )
                    ).tuples()
                ),
            )
    finally:
        engine.dispose()


@pytest.mark.data_platform
def test_fixture_loader_is_safe_to_rerun(settings, tmp_path: Path):
    run_id = f"pytest-{uuid4().hex}"
    first = load_fixtures(settings, run_id, tmp_path / "first.json")
    source_after_first = _source_identity(settings)
    second = load_fixtures(settings, run_id, tmp_path / "second.json")

    assert first["published_event_count"] == 15
    assert second["published_event_count"] == 0
    assert second["existing_event_count"] == 15
    assert _source_identity(settings) == source_after_first


@pytest.mark.data_platform
def test_dbt_build_preserves_sources_and_isolates_analytics(settings):
    before = _source_identity(settings)
    dbt_executable = which("dbt")
    assert dbt_executable is not None
    completed = subprocess.run(  # noqa: S603 - resolved dbt executable with fixed arguments
        [
            dbt_executable,
            "build",
            "--project-dir",
            "dbt",
            "--profiles-dir",
            "dbt",
            "--target",
            "local",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
    assert _source_identity(settings) == before

    engine = create_database_engine(settings)
    try:
        inspector = inspect(engine)
        public_relations = set(inspector.get_table_names(schema="public")) | set(
            inspector.get_view_names(schema="public")
        )
        mart_tables = set(inspector.get_table_names(schema="analytics_local_marts"))
    finally:
        engine.dispose()

    assert not {
        "stg_order_events",
        "stg_processed_events",
        "stg_log_metrics_minute",
        "fct_order_events",
        "fct_orders",
        "mart_daily_sales",
        "mart_service_health",
    } & public_relations
    assert {
        "fct_order_events",
        "fct_orders",
        "mart_daily_sales",
        "mart_service_health",
    } <= mart_tables
