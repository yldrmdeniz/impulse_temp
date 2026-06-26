"""StatisticType enum for defining supported statistics types."""

from enum import Enum


class StatisticType(Enum):
    """
    Enumeration of supported statistic types for aggregations.

    Attributes
    ----------
    MIN : str
        Minimum value statistic.
    MAX : str
        Maximum value statistic.
    MEAN : str
        Mean (average) value statistic.
    MEDIAN : str
        Median value statistic.
    DIFF_START_END : str
        Difference between the last and first value (last - first).
    """

    MIN = "min"
    MAX = "max"
    MEAN = "mean"
    MEDIAN = "median"
    DIFF_START_END = "diff_start_end"
