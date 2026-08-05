"""Validated event-generator runtime options."""

from pathlib import Path
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, model_validator

Rate = Annotated[float, Field(ge=0.0, le=1.0)]


class GeneratorOptions(BaseModel):
    """Parameters controlling one event-generator run."""

    model_config = ConfigDict(extra="forbid")

    events_per_second: Annotated[float, Field(gt=0)]
    duration_seconds: Annotated[float, Field(gt=0)]
    order_ratio: Rate
    log_ratio: Rate
    invalid_rate: Rate = 0.0
    stale_rate: Rate = 0.0
    stale_hours: Annotated[float, Field(gt=0)] = 168.0
    duplicate_rate: Rate = 0.0
    seed: int = 42
    report_path: Path = Path("reports/latest.json")

    @model_validator(mode="after")
    def validate_ratios(self) -> GeneratorOptions:
        """Require a complete event mix and non-overlapping injection rates."""
        if abs((self.order_ratio + self.log_ratio) - 1.0) > 1e-9:
            raise ValueError("order_ratio and log_ratio must sum to 1")
        injection_total = self.invalid_rate + self.stale_rate + self.duplicate_rate
        if injection_total > 1.0 + 1e-9:
            raise ValueError("invalid_rate, stale_rate, and duplicate_rate must sum to at most 1")
        return self

    @property
    def planned_events(self) -> int:
        """Return the target number of attempted events for the run."""
        return int(self.events_per_second * self.duration_seconds)
