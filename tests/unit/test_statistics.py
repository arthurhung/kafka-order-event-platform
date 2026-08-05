from streaming_platform.metrics.statistics import average, nearest_rank_percentile


def test_average_and_nearest_rank_percentiles() -> None:
    samples = [40.0, 10.0, 30.0, 20.0]
    assert average(samples) == 25.0
    assert nearest_rank_percentile(samples, 0.95) == 40.0
    assert nearest_rank_percentile(samples, 0.99) == 40.0
    assert samples == [40.0, 10.0, 30.0, 20.0]


def test_empty_single_and_small_samples() -> None:
    assert average([]) is None
    assert nearest_rank_percentile([], 0.95) is None
    assert nearest_rank_percentile([7.5], 0.95) == 7.5
    assert nearest_rank_percentile([1.0, 2.0], 0.5) == 1.0
