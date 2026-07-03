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

    MILLISECONDS = "ms"
    SECONDS = "s"
    MINUTES = "m"
    HOURS = "h"
    DAYS = "d"


_UNIT_TO_MS = {
    "ms": 1,
    "s": 1_000,
    "m": 60_000,
    "h": 3_600_000,
    "d": 86_400_000,
}

_VALID_UNITS = {u.value for u in TimeUnit}


def _parse_duration(duration_str: str) -> int:
    """Parse a duration string into milliseconds.

    Parameters
    ----------
    duration_str : str
        A string like ``"10m"``, ``"1h"``, ``"500ms"``.

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
        """
        super().__init__(name)
        self.group_by_time = group_by_time
        self._group_by_ms = _parse_duration(group_by_time)
        self.description = desc
        self.required_channels = None
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
        """GroupByTimeEvent has no time-series expression.

        Returns
        -------
        None
        """
        return None

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
        """Determine event instances by splitting containers into time slices.

        Parameters
        ----------
        spark : SparkSession
            Active Spark session.
        events : list of GroupByTimeEvent
            List of GroupByTimeEvent objects (only the first is used).
        solved_df : DataFrame, optional
            Not used (kept for interface compatibility).
        query : QueryBuilder, optional
            Query builder with filters applied.
        solver : QuerySolver, optional
            Solver whose filter pipeline is used for container resolution.
        pre_filtered_containers_df : DataFrame, optional
            Pre-filtered containers for incremental processing.

        Returns
        -------
        DataFrame
            Spark DataFrame matching ``EVENT_INSTANCE_FACT_SCHEMA``.
        """
        event = events[0]
        group_by_ms = event._group_by_ms

        # Resolve containers via solver filter pipeline
        container_tags_df = solver.filter_container_tags(spark, query)
        container_metrics_df = solver.filter_container_metrics(
            spark, query, container_tags_df, pre_filtered_containers_df
        )

        # Cast timestamps (use withColumn to handle fully-qualified column names)
        start_ts_col = solver.config.start_ts_col
        stop_ts_col = solver.config.stop_ts_col
        df = container_metrics_df.withColumn(
            "start_ts", f.col(start_ts_col).cast("long")
        ).withColumn("stop_ts", f.col(stop_ts_col).cast("long"))

        # Compute number of slices per container
        df = df.withColumn(
            "_num_slices",
            f.ceil((f.col("stop_ts") - f.col("start_ts")) / f.lit(group_by_ms)).cast("int"),
        )

        # Explode into slice indices
        df = df.withColumn(
            "_slice_index",
            f.explode(f.sequence(f.lit(0), f.col("_num_slices") - 1)),
        )

        # Compute slice boundaries
        df = df.withColumn(
            "start_ts",
            (f.col("start_ts") + f.col("_slice_index") * f.lit(group_by_ms)).cast(LongType()),
        )
        df = df.withColumn(
            "end_ts",
            f.least(
                f.col("start_ts") + f.lit(group_by_ms),
                f.col("stop_ts"),
            ).cast(LongType()),
        )

        # Add event_name for downstream utilities
        df = df.withColumn("event_name", f.lit(event.get_name()))

        # Generate event_instance_id (uses composite key hash)
        df = df.withColumn(
            "event_instance_id",
            generate_event_instance_id_column(event_type=GroupByTimeEvent),
        )

        # Add event_id column
        df = df.withColumn(
            "event_id",
            ReportEntityUtil.get_event_id_column(elements=events, element_name="event_name"),
        )

        # Select only the columns defined in the fact schema
        return df.select(EVENT_INSTANCE_FACT_SCHEMA.fieldNames())

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
