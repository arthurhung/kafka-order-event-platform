"""Tests that guard the specified PostgreSQL schema."""

from streaming_platform.database.models import Base


def test_metadata_contains_phase_one_tables() -> None:
    assert set(Base.metadata.tables) == {
        "valid_orders",
        "processed_events",
        "log_metrics_minute",
    }


def test_processed_event_uses_composite_primary_key() -> None:
    table = Base.metadata.tables["processed_events"]

    assert {column.name for column in table.primary_key.columns} == {
        "consumer_group",
        "event_id",
    }
