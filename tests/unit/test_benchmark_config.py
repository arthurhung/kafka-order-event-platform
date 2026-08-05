from pathlib import Path

import pytest
from pydantic import ValidationError

from streaming_platform.benchmark.config import BenchmarkConfig, BenchmarkProfile


def test_named_profiles_and_overrides() -> None:
    smoke = BenchmarkConfig.from_profile(BenchmarkProfile.SMOKE)
    assert (smoke.target_eps, smoke.duration_seconds) == (100, 60)
    assert smoke.adjusted is False

    adjusted = BenchmarkConfig.from_profile(
        BenchmarkProfile.STRESS,
        target_eps=2_000,
        duration_seconds=120,
        report_directory=Path("reports/test"),
    )
    assert adjusted.profile_target_eps == 5_000
    assert adjusted.profile_duration_seconds == 300
    assert adjusted.adjusted is True

    stress = BenchmarkConfig.from_profile(BenchmarkProfile.STRESS)
    assert stress.timeout_seconds == 120


def test_custom_requires_explicit_workload() -> None:
    with pytest.raises(ValueError, match="custom profile requires"):
        BenchmarkConfig.from_profile(BenchmarkProfile.CUSTOM)


@pytest.mark.parametrize(
    "overrides, message",
    [
        ({"order_ratio": 0.4}, "sum to 1"),
        ({"invalid_rate": 0.6, "duplicate_rate": 0.5}, "at most 1"),
        ({"timeout_seconds": 0}, "greater than 0"),
    ],
)
def test_configuration_validation(overrides, message) -> None:
    with pytest.raises(ValidationError, match=message):
        BenchmarkConfig.from_profile(BenchmarkProfile.SMOKE, **overrides)
