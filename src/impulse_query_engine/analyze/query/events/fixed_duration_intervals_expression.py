from __future__ import annotations

import numpy as np

from impulse_query_engine.analyze.metadata.tag_expression import TagExpression
from impulse_query_engine.analyze.metadata.time_series_expression import (
    TimeSeriesExpression,
    TimeSeriesSelector,
)
from impulse_query_engine.analyze.query.solvers.series_cache import SeriesCache
from impulse_query_engine.model.series.intervals import Intervals


class FixedDurationIntervalsExpression(TimeSeriesExpression):
    """Virtual periodic mask that yields fixed-duration event intervals.

    Conceptually this behaves like a calculated channel that is ``1`` inside
    each slice and ``0`` at slice boundaries. It returns the interval result
    directly so adjacent periods stay distinct instead of being merged by the
    normal boolean-comparison path.
    """

    def __init__(self, duration_ms: float):
        if duration_ms <= 0:
            raise ValueError("FixedDurationIntervalsExpression duration must be positive.")
        self.duration_ms = duration_ms
        TimeSeriesExpression.__init__(self, is_single_signal=False)

    def __str__(self) -> str:
        return f"FixedDurationIntervalsExpression<duration_ms={self.duration_ms}>"

    def dtype(self):
        return Intervals.empty().dtype()

    def build(self, cache: SeriesCache) -> Intervals:
        pdf = getattr(cache, "pdf", None)
        start_col = getattr(cache, "_ts_col", None)
        end_col = getattr(cache, "_te_col", None)
        if pdf is None or start_col is None or end_col is None or pdf.empty:
            return Intervals.empty()

        start = pdf[start_col].min()
        end = pdf[end_col].max()
        if np.isnan(start) or np.isnan(end):
            return Intervals.empty()

        timestamp_magnitude = max(abs(start), abs(end))
        duration = self._milliseconds_in_cache_units(self.duration_ms, timestamp_magnitude)
        boundary_gap = self._milliseconds_in_cache_units(1, timestamp_magnitude)
        starts = np.arange(start, end, duration, dtype=np.float64)
        ends = np.minimum(starts + duration, end) - boundary_gap
        return Intervals(starts, ends, del_last_empty=True)

    def _milliseconds_in_cache_units(self, milliseconds: float, timestamp_magnitude: float) -> float:
        if timestamp_magnitude >= 1e18:
            return milliseconds * 1_000_000
        if timestamp_magnitude >= 1e15:
            return milliseconds * 1_000
        if timestamp_magnitude < 1e12:
            return milliseconds / 1_000
        return milliseconds

    def get_required_tag_exprs(self) -> set[TagExpression]:
        return set()

    def required_tags(self) -> set[str]:
        return set()

    def get_selectors(self) -> list[TimeSeriesSelector]:
        return []

    def get_selector_expr(self):
        return None
