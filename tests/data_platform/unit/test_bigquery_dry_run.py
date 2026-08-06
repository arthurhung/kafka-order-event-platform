from pathlib import Path

import pytest

from data_platform.bigquery_dry_run import (
    DryRunInputError,
    DryRunProvider,
    run_provider,
)

FIXTURE = Path("tests/data_platform/fixtures/bigquery_dry_run/below_warning.json")


def test_local_fixture_is_simulated_without_job_id():
    report = run_provider(DryRunProvider.LOCAL_FIXTURE, FIXTURE)
    assert report["evidence_level"] == "simulated"
    assert report["estimation_method"] == "fixture_estimated"
    assert report["observed_job_id"] is None
    assert report["cloud_execution_status"] == "not_executed"


@pytest.mark.parametrize(
    "provider", [DryRunProvider.BIGQUERY_SANDBOX, DryRunProvider.BIGQUERY_CLOUD]
)
def test_cloud_providers_are_explicitly_unavailable(provider):
    report = run_provider(provider, None)
    assert report["status"] == "not_available"
    assert report["evidence_level"] == "not_available"
    assert report["estimated_bytes"] is None
    assert report["observed_job_id"] is None


def test_local_provider_requires_fixture():
    with pytest.raises(DryRunInputError):
        run_provider(DryRunProvider.LOCAL_FIXTURE, None)
