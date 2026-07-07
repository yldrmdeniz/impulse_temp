---
sidebar_label: group_by_time_event
title: impulse_reporting.events.group_by_time_event
---

GroupByTimeEvent — an event that splits each measurement container into fixed-duration time slices.


## GroupByTimeEvent

```python
class GroupByTimeEvent(Event)
```

Event that divides a measurement container into consecutive time-based intervals.

Unlike `ContainerEvent`, which produces one event instance for the whole container,
`GroupByTimeEvent` creates one event instance per fixed-duration slice. This is useful
when a report needs repeated windows such as one-minute, ten-minute, hourly, or daily
segments across each matching container.

Supported duration units are:

- `s`: seconds
- `m`: minutes
- `h`: hours
- `d`: days

Examples of valid `group_by_time` values include `"30s"`, `"10m"`, `"1h"`, and `"1d"`.


#### Example Usage

```python
from impulse_reporting.core.report import Report
from impulse_reporting.events.group_by_time_event import GroupByTimeEvent

report = Report(
    name="ten_minute_report",
    spark=spark,
    workspace_client=workspace_client,
    config=config,
)

event = GroupByTimeEvent(
    name="ten_minute_slices",
    group_by_time="10m",
    desc="Split each selected measurement container into 10-minute slices.",
    attributes={"window_type": "fixed"},
)

report.add_event(event)
report.determine_report()
```

Each generated event instance uses the container's original `start_ts` as the first slice
boundary. Subsequent slices are contiguous, and the final slice ends at the container's
`stop_ts` minus the boundary gap used by the event fact table.


#### \_parse\_duration

```python
def _parse_duration(duration_str: str) -> int
```

Parse a duration string into milliseconds.

**Arguments**:

- `duration_str` (`str`): Duration string in `<number><unit>` format, such as `"10m"`, `"1h"`, or `"30s"`.

**Returns**:

`int`: Duration in milliseconds.

**Raises**:

- `ValueError`: If the format is invalid or the unit is not one of `s`, `m`, `h`, or `d`.

#### \_\_init\_\_

```python
def __init__(name: str,
             group_by_time: str,
             desc: str | None = None,
             attributes: Mapping[str, str] | None = None)
```

Initialise a GroupByTimeEvent.

**Arguments**:

- `name` (`str`): Name of the event.
- `group_by_time` (`str`): Fixed slice duration, such as `"10m"`, `"1h"`, or `"30s"`.
- `desc` (`str`): Human-readable description.
- `attributes` (`Mapping[str, str]`): Key-value metadata for the event. The event automatically adds `grouped_by` with the configured `group_by_time` value.

#### get\_id

```python
def get_id() -> int
```

Return a unique identifier derived from the event name.

**Returns**:

`int`: Positive 32-bit integer identifier.

#### get\_expression

```python
def get_expression() -> TimeSeriesExpression
```

Return the fixed-duration interval expression used by the event.

**Returns**:

`TimeSeriesExpression`: A `FixedDurationIntervalsExpression` configured with the parsed duration in milliseconds.

#### get\_event\_type\_str

```python
def get_event_type_str() -> str
```

Get the event type string for GroupByTimeEvent.

**Returns**:

`str`: `"GROUP_BY_TIME_EVENT"`.

#### determine\_definition\_hash

```python
def determine_definition_hash() -> int
```

Calculate definition hash.

The hash captures computation-relevant attributes: the event name and the
`group_by_time` duration string.

**Returns**:

`int`: Hash value representing the computation definition.

#### as\_dict

```python
def as_dict() -> dict
```

Return a dictionary representation of the event.

The result includes event metadata, the event expression, definition hash, and
attributes. The attributes always include `grouped_by`.

**Returns**:

`dict`: Dictionary containing event metadata.

#### as\_spark\_row

```python
def as_spark_row() -> Row
```

Return a Spark `Row` representation.

**Returns**:

`Row`: Spark Row containing event metadata.

#### determine\_events

```python
def determine_events(
        cls,
        spark: SparkSession,
        events: list[GroupByTimeEvent],
        *,
        solved_df: DataFrame = None,
        query: QueryBuilder = None,
        solver: QuerySolver = None,
        pre_filtered_containers_df: DataFrame = None) -> DataFrame
```

Determine event instances by splitting containers into fixed-duration time slices.

The method resolves matching containers through the solver's filter pipeline, computes
the number of slices needed for each container, explodes the containers into slice rows,
and returns event facts matching `EVENT_INSTANCE_FACT_SCHEMA`.

**Arguments**:

- `spark` (`SparkSession`): Active Spark session.
- `events` (`list of GroupByTimeEvent`): List of GroupByTimeEvent objects. Only the first event is used for the slicing configuration.
- `solved_df` (`DataFrame`): Not used by GroupByTimeEvent; kept for interface compatibility.
- `query` (`QueryBuilder`): Query builder with filters applied.
- `solver` (`QuerySolver`): Solver whose filter pipeline is used for container resolution.
- `pre_filtered_containers_df` (`DataFrame`): Pre-filtered containers for incremental processing.

**Returns**:

`DataFrame`: Spark DataFrame matching `EVENT_INSTANCE_FACT_SCHEMA`, with `container_id`, `event_instance_id`, `event_id`, `start_ts`, and `end_ts` columns.

#### determine\_metadata\_df

```python
def determine_metadata_df(cls, spark: SparkSession,
                          events: list[GroupByTimeEvent]) -> DataFrame
```

Create a Spark DataFrame containing event metadata.

**Arguments**:

- `spark` (`SparkSession`): Active Spark session.
- `events` (`list of GroupByTimeEvent`): List of GroupByTimeEvent objects.

**Returns**:

`DataFrame`: Spark DataFrame matching `EVENT_DIMENSION_SCHEMA`.