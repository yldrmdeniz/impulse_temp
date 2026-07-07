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
    """Build fixed-duration intervals over the available sample-series range."""

    def __init__(self, duration: float):
        if duration <= 0:
            raise ValueError("FixedDurationIntervalsExpression duration must be positive.")
        self.duration = duration
        TimeSeriesExpression.__init__(self, is_single_signal=False)

    def __str__(self) -> str:
        return f"FixedDurationIntervalsExpression<duration={self.duration}>"

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

        starts = np.arange(start, end, self.duration, dtype=np.float64)
        ends = np.minimum(starts + self.duration, end)
        return Intervals(starts, ends, del_last_empty=True)

    def get_required_tag_exprs(self) -> set[TagExpression]:
        return set()

    def required_tags(self) -> set[str]:
        return set()

    def get_selectors(self) -> list[TimeSeriesSelector]:
        return []

    def get_selector_expr(self):
        return None
