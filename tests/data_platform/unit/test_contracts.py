from pathlib import Path

from data_platform.contracts import compare_contracts

FIXTURES = Path("tests/data_platform/fixtures/contracts")


def test_removed_published_column_is_blocking_with_downstream_impact():
    report = compare_contracts(
        FIXTURES / "base_manifest.json",
        FIXTURES / "removed_column_manifest.json",
        previous_git_sha="base-sha",
        current_git_sha="current-sha",
    )

    assert report.status == "blocked"
    finding = next(
        item for item in report.findings if item.change_type == "published_column_removed"
    )
    assert finding.model == "mart_orders"
    assert finding.column == "paid_amount"
    assert finding.severity == "blocking"
    assert finding.classification == "breaking"
    assert ("mart_orders", "mart_order_summary") in finding.downstream_impact_paths
    assert report.previous_git_sha == "base-sha"
    assert report.current_git_sha == "current-sha"


def test_identical_contracts_pass():
    report = compare_contracts(
        FIXTURES / "base_manifest.json",
        FIXTURES / "base_manifest.json",
    )

    assert report.status == "passed"
    assert report.blocking_count == 0


def test_missing_previous_state_is_explicit(tmp_path: Path):
    report = compare_contracts(
        tmp_path / "missing.json",
        FIXTURES / "base_manifest.json",
    )

    assert report.status == "previous_state_unavailable"
    assert report.findings == ()
