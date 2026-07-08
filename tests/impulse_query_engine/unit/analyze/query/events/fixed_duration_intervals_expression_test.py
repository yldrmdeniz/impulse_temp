from types import SimpleNamespace

import pandas as pd

from impulse_query_engine.analyze.query.events.fixed_duration_intervals_expression import (
    FixedDurationIntervalsExpression,
)


def test_build_returns_adjacent_slices_without_merging():
    cache = SimpleNamespace(
        pdf=pd.DataFrame(
            {
                "tstart": [0.0, 10.0, 20.0],
                "tend": [10.0, 20.0, 30.0],
            }
        ),
        _ts_col="tstart",
        _te_col="tend",
    )

    intervals = FixedDurationIntervalsExpression(duration_ms=10_000.0).build(cache)

    assert intervals.get_data() == [
        [0.0, 10.0],
        [10.0, 20.0],
        [20.0, 30.0],
    ]
