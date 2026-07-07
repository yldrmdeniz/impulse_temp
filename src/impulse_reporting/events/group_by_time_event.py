"""GroupByTimeEvent — splits a measurement container into fixed-duration time slices."""

from __future__ import annotations

import hashlib
import re
from enum import Enum
from typing import TYPE_CHECKING, Mapping

import pyspark.sql.functions as f
import zlib
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.types import LongType

from impulse_query_engine.analyze.query.events.group_by_time_expression import (
    GroupByTimeExpression,
)
from impulse_query_engine.analyze.query.query_builder import QueryBuilder
from impulse_query_engine.analyze.query.solvers.query_solver import QuerySolver
from impulse_reporting.events.event import Event
from impulse_reporting.persist.dimension_schema import EVENT_DIMENSION_SCHEMA
from impulse_reporting.persist.fact_schema import EVENT_INSTANCE_FACT_SCHEMA
from impulse_reporting.util.event_instance_util import generate_event_instance_id_column
from impulse_reporting.util.report_entity_util import ReportEntityUtil

if TYPE_CHECKING:
    from impulse_query_engine.analyze.metadata.time_series_expression import (
        TimeSeriesExpression,
    )


class TimeUnit(Enum):
    """Valid time units for GroupByTimeEvent duration strings."""

    SECONDS = "s"
    MINUTES = "m"
    HOURS = "h"
    DAYS = "d"


_UNIT_TO_MS = {
    "s": 1_000,
    "m": 60_000,
    "h": 3_600_000,
    "d": 86_400_000,
}

# Channel-data time unit -> number of those units per millisecond.  Used to
# translate the human-specified slice/gap durations (milliseconds) into the
# unit the channel data (and therefore the solve cache span) is stored in.
_UNIT_PER_MS = {
    "s": 0.001,
    "ms": 1.0,
    "us": 1_000.0,
    "ns": 1_000_000.0,
}

_VALID_UNITS = {u.value for u in TimeUnit}


def _parse_duration(duration_str: str) -> int:
    """Parse a duration string into milliseconds.

    Parameters
    ----------
    duration_str : str
        A string like ``"10m"``, ``"1h"``, ``"30s"``.

    Returns
    -------
    int
        Duration in milliseconds.

    Raises
    ------
    ValueError
        If the format is invalid or the unit is not recognised.
    """
    match = re.match(r"^(\d+)\s*([a-zA-Z]+)$", duration_str.strip())
    if not match:
        valid_list = ", ".join(f"{u.value} ({u.name})" for u in TimeUnit)
        raise ValueError(
            f"Invalid duration format: '{duration_str}'. "
            f"Expected format: <number><unit>, e.g. '10m'. Valid units: {valid_list}"
        )

    value_str, unit_str = match.groups()
    unit_str = unit_str.lower()

    if unit_str not in _VALID_UNITS:
        valid_list = ", ".join(f"{u.value} ({u.name})" for u in TimeUnit)
        raise ValueError(
            f"Invalid time unit: '{unit_str}' in duration '{duration_str}'. "
            f"Valid units: {valid_list}"
        )

    return int(value_str) * _UNIT_TO_MS[unit_str]


class GroupByTimeEvent(Event):
    """Event that splits a measurement container into fixed-duration time slices.

    Unlike ``ContainerEvent`` which produces one event per container,
    ``GroupByTimeEvent`` divides each container into multiple consecutive
    slices of the specified duration.
    """

    def __init__(
        self,
        name: str,
        group_by_time: str,
        desc: str | None = None,
        attributes: Mapping[str, str] | None = None,
        boundary_gap_ms: int = 1,
        channel_time_unit: str = "us",
    ):
        """
        Initialise a GroupByTimeEvent.

        Parameters
        ----------
        name : str
            Name of the event.
        group_by_time : str
            Duration string, e.g. ``"10m"``, ``"1h"``, ``"30s"``.
        desc : str, optional
            Human-readable description.
        attributes : Mapping[str, str], optional
            Key-value metadata for the event.
        boundary_gap_ms : int, optional
            Width, in milliseconds, of the small gap inserted at every slice
            boundary so that consecutive slices remain distinct intervals.
            Each slice therefore "loses" this much time at its trailing edge.
            Defaults to ``1`` (1 millisecond).
        channel_time_unit : str, optional
            Time unit of the channel-data timestamps the slices are evaluated
            against. One of ``"s"``, ``"ms"``, ``"us"``, ``"ns"``. The slice
            width and boundary gap are converted into this unit so the produced
            intervals align with the channel data. Defaults to ``"us"``
            (microseconds).
        """
        super().__init__(name)
        self.group_by_time = group_by_time
        self._group_by_ms = _parse_duration(group_by_time)
        self.description = desc
        self.required_channels = None

        if channel_time_unit not in _UNIT_PER_MS:
            valid = ", ".join(sorted(_UNIT_PER_MS))
            raise ValueError(
                f"Invalid channel_time_unit: '{channel_time_unit}'. Valid units: {valid}."
            )
        if boundary_gap_ms <= 0:
            raise ValueError(
                f"boundary_gap_ms must be positive, got {boundary_gap_ms}."
            )
        self.boundary_gap_ms = boundary_gap_ms
        self.channel_time_unit = channel_time_unit

        # Translate the human-specified slice width and boundary gap
        # (milliseconds) into the channel-data time unit so the synthetic
        # mask aligns with the data being sliced.
        units_per_ms = _UNIT_PER_MS[channel_time_unit]
        slice_width = self._group_by_ms * units_per_ms
        gap = self.boundary_gap_ms * units_per_ms
        self.expression = GroupByTimeExpression(slice_width, gap).alias(name)

        normalized_attributes: dict[str, str] = {}
        if attributes is not None:
            normalized_attributes = {str(k): str(v) for k, v in attributes.items()}
        normalized_attributes["grouped_by"] = self.group_by_time
        self.attributes = normalized_attributes

    # ------------------------------------------------------------------
    # Instance methods
    # ------------------------------------------------------------------

    def get_id(self) -> int:
        """Return a unique identifier derived from the event name.

        Returns
        -------
        int
            Positive 32-bit integer identifier.
        """
        return zlib.crc32(self.name.encode()) & 0x7FFFFFFF

    def get_expression(self) -> TimeSeriesExpression | None:
        """Return the time-slicing expression for this event.

        Returns
        -------
        TimeSeriesExpression
            A ``GroupByTimeExpression`` that yields one interval per time
            slice when solved against a container's channel data.
        """
        return self.expression

    def get_event_type_str(self) -> str:
        """Get the event type string for GroupByTimeEvent.

        Returns
        -------
        str
            Event type string.
        """
        return "GROUP_BY_TIME_EVENT"

    def determine_definition_hash(self) -> int:
        """Calculate definition hash.

        The hash captures computation-relevant attributes: the event name
        and the ``group_by_time`` duration string.

        Returns
        -------
        int
            Hash value representing the computation definition.
        """
        hash_input = f"{self.name}::{self.group_by_time}"
        hash_bytes = hashlib.sha256(hash_input.encode()).digest()
        return int.from_bytes(hash_bytes[:8], byteorder="big", signed=True)

    def as_dict(self) -> dict:
        """Return a dictionary representation of the event.

        Returns
        -------
        dict
        """
        return {
            "event_id": self.get_id(),
            "report_id": self.report_id,
            "event_type": self.get_event_type_str(),
            "event_name": self.name,
            "event_description": self.description,
            "required_channels": None,
            "event_expression": self.get_expression_str(),
            "definition_hash": self.determine_definition_hash(),
            "attributes": self.attributes,
        }

    def as_spark_row(self) -> Row:
        """Return a Spark ``Row`` representation.

        Returns
        -------
        Row
        """
        return Row(**self.as_dict())

    # ------------------------------------------------------------------
    # Class methods
    # ------------------------------------------------------------------

    @classmethod
    def determine_events(
        cls,
        spark: SparkSession,
        events: list[GroupByTimeEvent],
        *,
        solved_df: DataFrame = None,
        query: QueryBuilder = None,
        solver: QuerySolver = None,
        pre_filtered_containers_df: DataFrame = None,
    ) -> DataFrame:
        """Determine event instances by exploding the solved time slices.

        The event's ``GroupByTimeExpression`` is solved as part of the
        centralized batch solve, producing one ``[start_ts, end_ts]`` interval
        per time slice in each container's solved column.  This method mirrors
        ``BasicEvent.determine_events``: it unpivots and explodes those
        intervals into event-instance fact rows so the resulting
        ``event_instance_id`` values match the ones produced by aggregations
        that share the same event expression.

        Parameters
        ----------
        spark : SparkSession
            Active Spark session.
        events : list of GroupByTimeEvent
            List of GroupByTimeEvent objects to process.
        solved_df : DataFrame, optional
            Pre-solved wide DataFrame from the centralized batch solve. Required.
        query : QueryBuilder, optional
            Unused (kept for interface compatibility).
        solver : QuerySolver, optional
            Unused (kept for interface compatibility).
        pre_filtered_containers_df : DataFrame, optional
            Unused (kept for interface compatibility).

        Returns
        -------
        DataFrame
            Spark DataFrame matching ``EVENT_INSTANCE_FACT_SCHEMA``.
        """
        if solved_df is None:
            raise ValueError(
                "GroupByTimeEvent.determine_events requires solved_df. "
                "Provide a pre-solved DataFrame from the centralized batch-solve flow."
            )

        event_names = [event.get_name() for event in events]

        df = (
            solved_df.select("container_id", *event_names)
            .unpivot(
                f.col("container_id"),
                event_names,
                variableColumnName="event_name",
                valueColumnName="value",
            )
            .select(
                "container_id",
                "event_name",
                f.explode(f.col("value")).alias("event_instance"),
            )
            .withColumn("start_ts", f.col("event_instance").getItem(0))
            .withColumn("end_ts", f.col("event_instance").getItem(1))
            .withColumn(
                "event_instance_id",
                generate_event_instance_id_column(event_type=GroupByTimeEvent),
            )
            .withColumn(
                "event_id",
                ReportEntityUtil.get_event_id_column(elements=events, element_name="event_name"),
            )
            # Cast the interval bounds to LongType *after* event_instance_id is
            # computed from the (double) interval values, so the hash matches the
            # aggregation fact which derives its id from the same double values.
            .withColumn("start_ts", f.col("start_ts").cast(LongType()))
            .withColumn("end_ts", f.col("end_ts").cast(LongType()))
            .select(EVENT_INSTANCE_FACT_SCHEMA.fieldNames())
            .where(f.col("start_ts") < f.col("end_ts"))  # Ensure valid time intervals
        )
        return df

    @classmethod
    def determine_metadata_df(
        cls, spark: SparkSession, events: list[GroupByTimeEvent]
    ) -> DataFrame:
        """Create a Spark DataFrame containing event metadata.

        Parameters
        ----------
        spark : SparkSession
            Active Spark session.
        events : list of GroupByTimeEvent
            List of GroupByTimeEvent objects.

        Returns
        -------
        DataFrame
            Spark DataFrame matching ``EVENT_DIMENSION_SCHEMA``.
        """
        rows = [event.as_spark_row() for event in events]
        return spark.createDataFrame(rows, schema=EVENT_DIMENSION_SCHEMA)
