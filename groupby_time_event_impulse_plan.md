# GroupByTimeEvent — Implementation Plan for `impulse`

## Summary

Add a new event type `GroupByTimeEvent` that splits a measurement container into fixed-duration time slices. This enables evaluating long measurements (e.g. 2-hour drives) in smaller, consistently-sized fractions instead of treating the entire container as one event.

---

## 1. New File: `impulse_reporting/events/group_by_time_event.py`

**Location:** Same directory as `event.py`, `basic_event.py`, `container_event.py`, `sequence_of_events.py`

**Module-level contents:**

| Component | Description |
|-----------|-------------|
| `TimeUnit` (Enum) | Valid time units: `MILLISECONDS="ms"`, `SECONDS="s"`, `MINUTES="m"`, `HOURS="h"`, `DAYS="d"` |
| `_parse_duration(duration_str) -> int` | Parses `"10m"` → `600000` (ms). Validates unit against `TimeUnit` enum. Raises `ValueError` with helpful message for invalid units like `"10km"`. |
| `GroupByTimeEvent(Event)` | Main class (details below) |

**Imports to adapt (mda → impulse):**

| mda_framework import | impulse equivalent |
|---|---|
| `from mda_query_engine.analyze.query.query_builder import QueryBuilder` | `from impulse_query_engine.analyze.query.query_builder import QueryBuilder` |
| `from mda_query_engine.analyze.query.solvers.query_solver import QuerySolver` | `from impulse_query_engine.analyze.query.solvers.query_solver import QuerySolver` |
| `from mda_reporting.config.config_parser import MeasurementDimensions` | `from impulse_reporting.config.config_parser import MeasurementDimensions` |
| `from mda_reporting.events.event import Event` | `from impulse_reporting.events.event import Event` |
| `from mda_reporting.persist.dimension_schema import EVENT_DIMENSION_SCHEMA` | `from impulse_reporting.persist.dimension_schema import EVENT_DIMENSION_SCHEMA` |
| `from mda_reporting.persist.fact_schema import EVENT_INSTANCE_FACT_SCHEMA` | `from impulse_reporting.persist.fact_schema import EVENT_INSTANCE_FACT_SCHEMA` |
| `from mda_reporting.util.event_instance_util import generate_event_instance_id_column` | `from impulse_reporting.util.event_instance_util import generate_event_instance_id_column` |
| `from mda_reporting.util.report_entity_util import ReportEntityUtil` | `from impulse_reporting.util.report_entity_util import ReportEntityUtil` |

---

## 2. `GroupByTimeEvent` Class Design

**Inherits from:** `Event` (same pattern as `ContainerEvent`)

**Constructor:**

```python
def __init__(self, name: str, group_by_time: str, desc: str | None = None, attributes: Mapping[str, str] | None = None)
```

- `group_by_time`: Duration string validated by `_parse_duration` (e.g. `"10m"`, `"1h"`, `"30s"`)
- Stores `self._group_by_ms` (parsed milliseconds) for computation
- `self.required_channels = None` (no time-series channels needed)
- `attributes` are normalized to `dict[str, str]`

**Abstract method implementations:**

| Method | Behaviour |
|--------|-----------|
| `get_id()` | `zlib.crc32(name) & 0x7FFFFFFF` |
| `get_expression()` | Returns `None` (no time-series expression) |
| `get_event_type_str()` | Returns `"GROUP_BY_TIME_EVENT"` |
| `determine_definition_hash()` | SHA-256 of `f"{name}::{group_by_time}"`, truncated to 8-byte signed int |
| `as_dict()` | Standard 9-field dict; `attributes` includes `"grouped_by": self.group_by_time` |
| `as_spark_row()` | `Row(**self.as_dict())` |

**Class methods:**

### `determine_events(spark, events, *, solved_df, query, solver, pre_filtered_containers_df)` → DataFrame

Returns `EVENT_INSTANCE_FACT_SCHEMA` columns: `(container_id, event_instance_id, event_id, start_ts, end_ts)`

Algorithm:

1. Resolve containers via `solver.filter_container_tags()` → `solver.filter_container_metrics()`
2. Rename columns: `container_id`, `start_ts`, `stop_ts` (cast to `LongType`)
3. Compute `_num_slices = ceil((stop_ts - start_ts) / group_by_ms)`
4. Explode via `f.sequence(0, _num_slices - 1)` to get `_slice_index`
5. Compute `slice_start_ts = start_ts + _slice_index * group_by_ms`
6. Compute `slice_end_ts = least(slice_start_ts + group_by_ms, stop_ts)`
7. Generate `event_instance_id` via `generate_event_instance_id_column(event_type=GroupByTimeEvent)`
8. Generate `event_id` via `ReportEntityUtil.get_event_id_column()`
9. Select fact schema columns

### `determine_events_with_attributes(spark, events, *, query, solver, pre_filtered_containers_df)` → DataFrame

Returns extended columns: `(container_id, event_instance_id, event_id, start_ts, end_ts, order, grouped_by, grouped_by_actual)`

Same algorithm as above, plus:

- `order = _slice_index + 1` (1-based)
- `grouped_by = lit(group_by_time_str)` (e.g. `"10m"`)
- `grouped_by_actual = concat((slice_end_ts - slice_start_ts).cast("string"), lit("ms"))` — real duration of each slice; differs from `grouped_by` only on the last slice

### `determine_metadata_df(spark, events)` → DataFrame

Standard pattern: `spark.createDataFrame([e.as_spark_row() for e in events], schema=EVENT_DIMENSION_SCHEMA)`

---

## 3. Register in `impulse_reporting/events/event_types.py`

```python
from impulse_reporting.events.group_by_time_event import GroupByTimeEvent

class EventType(Enum):
    BASIC_EVENT = BasicEvent
    CONTAINER_EVENT = ContainerEvent
    SEQUENCE_OF_EVENTS = SequenceOfEvents
    GROUP_BY_TIME_EVENT = GroupByTimeEvent  # ← ADD
```

Add `EventType.GROUP_BY_TIME_EVENT` to **all 4 match statements**:

- `get_fact_table_name()` → `"event_instance_fact"`
- `get_fact_schema()` → `EVENT_INSTANCE_FACT_SCHEMA`
- `get_dimension_table_name()` → `"event_dimension"`
- `get_dimension_schema()` → `EVENT_DIMENSION_SCHEMA`

---

## 4. `TimeUnit` Enum — Input Validation

```python
class TimeUnit(Enum):
    MILLISECONDS = "ms"
    SECONDS = "s"
    MINUTES = "m"
    HOURS = "h"
    DAYS = "d"
```

The `_parse_duration` function uses a **two-stage** validation:

1. Regex `r"^(\d+)\s*([a-zA-Z]+)$"` — catches non-numeric or empty input
2. Unit membership check against `{u.value for u in TimeUnit}` — rejects invalid units like `"km"`, `"x"`, `"min"`

Error messages explicitly list valid units with their enum names.

---

## 5. Test File: `tests/unit/impulse_reporting/events/group_by_time_event_test.py`

**Imports to adapt:**

```python
from impulse_query_engine.analyze.query.solvers.basic_narrow_solver import BasicNarrowSolver
from impulse_reporting.events.group_by_time_event import GroupByTimeEvent, TimeUnit, _parse_duration
```

**Test categories (29 tests total):**

| Category | Tests |
|----------|-------|
| `_parse_duration` | `ms`, `s`, `m`, `h`, `d`, whitespace handling, invalid format, invalid unit (`"10km"`, `"5kg"`), empty string, `TimeUnit` enum values |
| Constructor | Required params, optional params |
| `get_id` | Deterministic, unique for different names, positive 32-bit |
| `get_expression` | Returns `None` |
| `get_event_type_str` | Returns `"GROUP_BY_TIME_EVENT"` |
| `as_dict` | Full structure validation including `attributes["grouped_by"]` |
| `as_spark_row` | Row has 9 fields |
| `determine_definition_hash` | Deterministic, differs by `group_by_time`, differs by name, ignores description |
| `determine_events` (Spark) | Valid fact DataFrame, `start_ts < end_ts`, multiple slices per container |
| `determine_events_with_attributes` (Spark) | Has `order`/`grouped_by`/`grouped_by_actual` columns, order is 1-based, last slice actual ≤ configured |
| `determine_metadata_df` (Spark) | Valid dimension DataFrame, `attributes["grouped_by"]` present |

---

## 6. No Schema Changes Required

- Reuses existing `EVENT_INSTANCE_FACT_SCHEMA`: `(container_id, event_instance_id, event_id, start_ts, end_ts)`
- Reuses existing `EVENT_DIMENSION_SCHEMA` (includes `attributes: MapType(StringType, StringType)`)

---

## 7. Example Usage

```python
from impulse_reporting.events.group_by_time_event import GroupByTimeEvent, TimeUnit

# 2-hour measurement split into 10-minute slices → 12 events
event = GroupByTimeEvent(
    name="10min_slices",
    group_by_time=f"10{TimeUnit.MINUTES.value}",  # "10m"
    desc="Split container into 10-minute windows",
)

# Standard fact table (for downstream aggregations)
fact_df = GroupByTimeEvent.determine_events(
    spark, [event], query=db.query, solver=solver
)

# Extended view with ordering metadata
attr_df = GroupByTimeEvent.determine_events_with_attributes(
    spark, [event], query=db.query, solver=solver
)
# attr_df columns: container_id, event_instance_id, event_id, start_ts, end_ts,
#                  order (1..12), grouped_by ("10m"), grouped_by_actual ("600000ms" or less for last)
```

---

## 8. Key Design Decisions

1. **Inherits `Event` directly** (not `BasicEvent`) — same pattern as `ContainerEvent`; no time-series expression is involved.
2. **`TimeUnit` enum for validation** — prevents nonsense units like `"km"`, `"min"`, `"hrs"` at construction time.
3. **Two public class methods for event determination** — `determine_events` for standard pipeline compatibility (returns fact schema), `determine_events_with_attributes` for richer downstream use with ordering info.
4. **Last-slice handling** — `grouped_by_actual` captures the real duration; all non-last slices get exactly `group_by_ms`, last slice gets `stop_ts - last_start_ts`.
5. **1-based `order` attribute** — each slice is explicitly numbered for ordering/filtering.
