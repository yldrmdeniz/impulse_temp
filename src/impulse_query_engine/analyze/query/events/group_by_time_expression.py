"""GroupByTimeExpression — yields fixed-duration time-slice Intervals over a container's data span.

This expression is not bound to any channel.  When built against a
:class:`~impulse_query_engine.analyze.query.solvers.series_cache.SeriesCache`
it reads the cache's overall time span (``cache.span()``) and produces a
synthetic boolean mask that is ``True`` for one ``slice_width`` and then
``False`` for a short ``gap`` at every slice boundary.  Comparing the mask to
``0`` yields one :class:`Intervals` entry per slice — exactly the mechanism a
``BasicEvent`` uses to turn a threshold into intervals.  The small ``gap`` keeps
consecutive slices from being merged back together by ``Intervals`` overlap
merging.
"""

from __future__ import annotations

import numpy as np

from impulse_query_engine.analyze.metadata.tag_expression import TagExpression
from impulse_query_engine.analyze.metadata.time_series_expression import (
    TimeSeriesExpression,
    TimeSeriesSelector,
)
from impulse_query_engine.analyze.query.solvers.series_cache import SeriesCache
from impulse_query_engine.model.series.intervals import Intervals
from impulse_query_engine.model.series.sample_series import SampleSeries


class GroupByTimeExpression(TimeSeriesExpression):
    """Produce fixed-duration time-slice ``Intervals`` over a container's data span.

    Parameters
    ----------
    slice_width : float
        Duration of each slice, expressed in the channel-data time unit
        (for example microseconds).  Must be positive.
    gap : float, optional
        Width of the boundary gap between consecutive slices, in the same unit
        as ``slice_width``.  Must be positive so that adjacent slices are not
        merged by ``Intervals`` overlap merging.  Defaults to ``1000.0``
        (1 millisecond when the unit is microseconds).
    alias : str, optional
        Expression alias (typically the owning event's name).
    """

    def __init__(self, slice_width: float, gap: float = 1000.0, alias: str = ""):
        if slice_width <= 0:
            raise ValueError(f"slice_width must be positive, got {slice_width}.")
        if gap <= 0:
            raise ValueError(f"gap must be positive to keep slices separable, got {gap}.")
        self.slice_width = float(slice_width)
        self.gap = float(gap)
        TimeSeriesExpression.__init__(self, alias=alias, is_single_signal=False)

    def dtype(self):
        """Return the Spark data type of the result (same as ``Intervals``)."""
        return Intervals.empty().dtype()

    def __str__(self) -> str:
        """Return a string representation of the expression."""
        return f"GroupByTimeExpression<slice_width={self.slice_width}, gap={self.gap}>"

    def build(self, cache: SeriesCache) -> Intervals:
        """Build the sliced ``Intervals`` for the container held by ``cache``.

        Parameters
        ----------
        cache : SeriesCache
            Cache providing the container's channel data.  Its ``span()`` is
            used as the extent to slice; it must be in the same time unit as
            ``slice_width``.

        Returns
        -------
        Intervals
            One interval per time slice, or an empty ``Intervals`` when the
            cache has no data or a degenerate span.
        """
        span = cache.span()
        if span is None:
            return Intervals.empty()
        t0, t1 = span
        if t1 <= t0:
            return Intervals.empty()
        mask = self._build_mask_series(t0, t1)
        # Regions where the mask is True become one interval per slice.
        return mask > 0

    def _build_mask_series(self, t0: float, t1: float) -> SampleSeries:
        """Construct the alternating True/False mask series spanning ``[t0, t1]``.

        The mask is ``True`` (value ``1.0``) for ``slice_width`` then ``False``
        (value ``0.0``) for ``gap``, repeating until ``t1`` is reached.
        """
        tstarts: list[float] = []
        tends: list[float] = []
        values: list[float] = []
        cursor = t0
        while cursor < t1:
            slice_end = min(cursor + self.slice_width, t1)
            tstarts.append(cursor)
            tends.append(slice_end)
            values.append(1.0)
            cursor = slice_end
            if cursor < t1:
                gap_end = min(cursor + self.gap, t1)
                tstarts.append(cursor)
                tends.append(gap_end)
                values.append(0.0)
                cursor = gap_end
        return SampleSeries(np.array(tstarts), np.array(tends), np.array(values))

    def get_required_tag_exprs(self) -> set[TagExpression]:
        """Return the required tag expressions (none — this expression is channel-agnostic)."""
        return set()

    def required_tags(self) -> set[str]:
        """Return the required tag keys (none — this expression is channel-agnostic)."""
        return set()

    def get_selector_expr(self):
        """Return the selector expression (``None`` — no channels are selected)."""
        return None

    def get_selectors(self) -> list[TimeSeriesSelector]:
        """Return the list of selectors (empty — no channels are selected)."""
        return []
