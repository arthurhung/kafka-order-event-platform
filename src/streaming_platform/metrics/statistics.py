"""Small, deterministic statistical calculations shared by reports."""

from math import ceil
from statistics import fmean


def average(values: list[float]) -> float | None:
    """Return the arithmetic mean, or ``None`` when no sample exists."""
    return fmean(values) if values else None


def nearest_rank_percentile(values: list[float], percentile: float) -> float | None:
    """Return a nearest-rank percentile without mutating the input samples."""
    if not 0 < percentile <= 1:
        raise ValueError("percentile must be greater than 0 and at most 1")
    if not values:
        return None
    ordered = sorted(values)
    index = ceil(percentile * len(ordered)) - 1
    return ordered[index]
