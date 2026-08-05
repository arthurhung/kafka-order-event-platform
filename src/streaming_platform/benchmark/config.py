"""Validated benchmark profiles and environment overrides."""

from enum import StrEnum
from pathlib import Path
from typing import Annotated, cast

from pydantic import BaseModel, ConfigDict, Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

Rate = Annotated[float, Field(ge=0.0, le=1.0)]


class BenchmarkProfile(StrEnum):
    """Named Phase 5 workload profiles."""

    SMOKE = "smoke"
    STANDARD = "standard"
    STRESS = "stress"
    CUSTOM = "custom"


PROFILE_TARGETS: dict[BenchmarkProfile, tuple[float, float]] = {
    BenchmarkProfile.SMOKE: (100.0, 60.0),
    BenchmarkProfile.STANDARD: (1_000.0, 300.0),
    BenchmarkProfile.STRESS: (5_000.0, 300.0),
}


class BenchmarkEnvironment(BaseSettings):
    """Optional ``BENCHMARK_*`` environment overrides used by the CLI."""

    model_config = SettingsConfigDict(
        env_prefix="BENCHMARK_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    profile: BenchmarkProfile = BenchmarkProfile.SMOKE
    target_eps: float | None = None
    duration_seconds: float | None = None
    order_ratio: float | None = None
    log_ratio: float | None = None
    application_error_ratio: float | None = None
    invalid_rate: float | None = None
    duplicate_rate: float | None = None
    seed: int | None = None
    timeout_seconds: float | None = None
    poll_interval_seconds: float | None = None
    report_directory: Path | None = None


class BenchmarkConfig(BaseModel):
    """Effective configuration for one benchmark or demo workload."""

    model_config = ConfigDict(extra="forbid")

    profile: BenchmarkProfile
    profile_target_eps: Annotated[float, Field(gt=0)]
    profile_duration_seconds: Annotated[float, Field(gt=0)]
    target_eps: Annotated[float, Field(gt=0)]
    duration_seconds: Annotated[float, Field(gt=0)]
    order_ratio: Rate = 0.2
    log_ratio: Rate = 0.8
    application_error_ratio: Rate = 0.0
    invalid_rate: Rate = 0.0
    duplicate_rate: Rate = 0.0
    seed: int | None = None
    timeout_seconds: Annotated[float, Field(gt=0)] = 600.0
    poll_interval_seconds: Annotated[float, Field(gt=0, le=10)] = 0.5
    report_directory: Path = Path("reports/runs")

    @model_validator(mode="after")
    def validate_ratios(self) -> BenchmarkConfig:
        """Validate mix and mutually exclusive injection ratios."""
        if abs(self.order_ratio + self.log_ratio - 1.0) > 1e-9:
            raise ValueError("order_ratio and log_ratio must sum to 1")
        if self.invalid_rate + self.duplicate_rate > 1.0 + 1e-9:
            raise ValueError("invalid_rate and duplicate_rate must sum to at most 1")
        return self

    @classmethod
    def from_profile(
        cls,
        profile: BenchmarkProfile,
        **overrides: object,
    ) -> BenchmarkConfig:
        """Build an effective configuration from a named profile and overrides."""
        if profile is BenchmarkProfile.CUSTOM:
            target = overrides.get("target_eps")
            duration = overrides.get("duration_seconds")
            if target is None or duration is None:
                raise ValueError("custom profile requires target_eps and duration_seconds")
            profile_target = float(cast(float | int | str, target))
            profile_duration = float(cast(float | int | str, duration))
        else:
            profile_target, profile_duration = PROFILE_TARGETS[profile]
        values = {
            key: value for key, value in overrides.items() if value is not None
        }
        if "timeout_seconds" not in values and profile is BenchmarkProfile.STRESS:
            values["timeout_seconds"] = 120.0
        return cls(
            profile=profile,
            profile_target_eps=profile_target,
            profile_duration_seconds=profile_duration,
            target_eps=values.pop("target_eps", profile_target),
            duration_seconds=values.pop("duration_seconds", profile_duration),
            **values,
        )

    @property
    def adjusted(self) -> bool:
        """Return whether effective throughput or duration differs from the profile."""
        return (
            self.target_eps != self.profile_target_eps
            or self.duration_seconds != self.profile_duration_seconds
        )
