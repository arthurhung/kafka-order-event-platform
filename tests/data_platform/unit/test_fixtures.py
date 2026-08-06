import json
from datetime import UTC, datetime

import pytest

from data_platform.fixtures import build_fixture_bundle


def test_fixture_bundle_is_deterministic_for_run_id(settings):
    first = build_fixture_bundle(settings, "repeatable", datetime(2026, 8, 6, tzinfo=UTC))
    second = build_fixture_bundle(
        settings,
        "repeatable",
        datetime(2026, 8, 7, tzinfo=UTC),
    )

    assert [record.event_id for record in first.records] == [
        record.event_id for record in second.records
    ]
    assert len(first.order_event_ids) == 10
    assert len(first.log_event_ids) == 5


def test_fixture_bundle_covers_required_order_scenarios(settings):
    bundle = build_fixture_bundle(settings, "coverage", datetime(2026, 8, 6, tzinfo=UTC))
    order_events = [
        json.loads(record.value) for record in bundle.records if record.family == "order"
    ]
    lifecycles: dict[str, list[str]] = {}
    for event in order_events:
        lifecycles.setdefault(event["payload"]["order_id"], []).append(event["event_type"])

    assert sorted(lifecycles.values()) == sorted(
        [
            ["order_created", "order_paid"],
            ["order_created", "payment_failed", "order_paid"],
            ["order_created", "order_cancelled"],
            ["order_created", "order_paid", "order_cancelled"],
        ]
    )
    created = [event for event in order_events if event["event_type"] == "order_created"]
    assert {event["payload"]["currency"] for event in created} == {"TWD", "USD"}
    assert {event["payload"]["channel"] for event in created} == {"web", "ios", "android"}


def test_fixture_bundle_covers_service_status_classes(settings):
    bundle = build_fixture_bundle(settings, "logs", datetime(2026, 8, 6, tzinfo=UTC))
    log_events = [
        json.loads(record.value) for record in bundle.records if record.family == "log"
    ]

    assert {event["payload"]["service"] for event in log_events} == {
        "order-api",
        "payment-api",
    }
    assert {event["payload"]["status_code"] // 100 for event in log_events} == {2, 4, 5}


def test_fixture_timestamp_must_be_timezone_aware(settings):
    with pytest.raises(ValueError, match="timezone-aware"):
        build_fixture_bundle(settings, "naive", datetime(2026, 8, 6))


def test_fixture_timestamp_is_normalized_to_utc_minute(settings):
    timestamp = datetime(2026, 8, 6, 15, 42, 37, 123, tzinfo=UTC)
    bundle = build_fixture_bundle(settings, "utc", timestamp)

    assert bundle.generated_at == datetime(2026, 8, 6, 15, 42, tzinfo=UTC)
