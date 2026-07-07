"""Unit tests for GroupByTimeEvent."""

import pytest
import pyspark.sql.functions as f

from impulse_query_engine.analyze.query.events.group_by_time_expression import (
    GroupByTimeExpression,
)
from impulse_query_engine.analyze.query.solvers.key_value_store_solver import KeyValueStoreSolver
from impulse_reporting.events.group_by_time_event import (
    GroupByTimeEvent,
    TimeUnit,
    _parse_duration,
)
from tests.conftest import basic_narrow_db, spark


def _solve_gbt(spark, db, event, channel_name="Engine RPM"):
    """Solve a GroupByTimeEvent's expression alongside a channel selector.

    ``GroupByTimeExpression`` carries no channel selectors, so a channel must be
    co-selected in the same solve batch for the per-container cache to hold data
    (the slice extent is derived from ``cache.span()``).
    """
    solver = KeyValueStoreSolver(spark)
    channel = db.query.channel(channel_name=channel_name)
    return db.query.select(channel.alias("_ch"), event.get_expression()).solve(spark, solver)


# ===========================================================================
# _parse_duration
# ===========================================================================
class TestParseDuration:
    """Tests for the _parse_duration helper function."""

    def test_parse_seconds(self):
        assert _parse_duration("30s") == 30_000

    def test_parse_minutes(self):
        assert _parse_duration("10m") == 600_000

    def test_parse_hours(self):
        assert _parse_duration("2h") == 7_200_000

    def test_parse_days(self):
        assert _parse_duration("1d") == 86_400_000

    def test_parse_with_whitespace(self):
        """Whitespace between number and unit should be tolerated."""
        assert _parse_duration("10 m") == 600_000
        assert _parse_duration("  5s  ") == 5_000

    def test_invalid_format_no_number(self):
        with pytest.raises(ValueError, match="Invalid duration format"):
            _parse_duration("minutes")

    def test_invalid_format_empty_string(self):
        with pytest.raises(ValueError, match="Invalid duration format"):
            _parse_duration("")

    def test_invalid_unit_km(self):
        with pytest.raises(ValueError, match="Invalid time unit"):
            _parse_duration("10km")

    def test_invalid_unit_kg(self):
        with pytest.raises(ValueError, match="Invalid time unit"):
            _parse_duration("5kg")

    def test_invalid_unit_ms(self):
        with pytest.raises(ValueError, match="Invalid time unit"):
            _parse_duration("500ms")

    def test_time_unit_enum_values(self):
        """All TimeUnit enum values should be parseable."""
        for unit in TimeUnit:
            result = _parse_duration(f"1{unit.value}")
            assert result > 0


# ===========================================================================
# Constructor
# ===========================================================================
class TestConstructor:
    """Tests for GroupByTimeEvent construction."""

    def test_init_required_params(self):
        event = GroupByTimeEvent(name="slices", group_by_time="10m")
        assert event.name == "slices"
        assert event.group_by_time == "10m"
        assert event._group_by_ms == 600_000
        assert event.description is None
        assert event.required_channels is None

    def test_init_optional_params(self):
        event = GroupByTimeEvent(
            name="slices",
            group_by_time="1h",
            desc="Hourly slices",
            attributes={"key": "value"},
        )
        assert event.description == "Hourly slices"
        assert event.attributes["key"] == "value"
        assert event.attributes["grouped_by"] == "1h"


# ===========================================================================
# get_id
# ===========================================================================
class TestGetId:
    """Tests for get_id."""

    def test_get_id_deterministic(self):
        ev1 = GroupByTimeEvent(name="evt", group_by_time="10m")
        ev2 = GroupByTimeEvent(name="evt", group_by_time="10m")
        assert ev1.get_id() == ev2.get_id()

    def test_get_id_unique_for_different_names(self):
        ev1 = GroupByTimeEvent(name="evt_a", group_by_time="10m")
        ev2 = GroupByTimeEvent(name="evt_b", group_by_time="10m")
        assert ev1.get_id() != ev2.get_id()

    def test_get_id_positive_32bit(self):
        event = GroupByTimeEvent(name="pos_check", group_by_time="5s")
        assert event.get_id() > 0
        assert event.get_id() <= 0x7FFFFFFF


# ===========================================================================
# get_expression
# ===========================================================================
class TestGetExpression:
    """Tests for get_expression."""

    def test_returns_group_by_time_expression(self):
        event = GroupByTimeEvent(name="expr", group_by_time="10m")
        expr = event.get_expression()
        assert isinstance(expr, GroupByTimeExpression)

    def test_expression_slice_width_in_channel_unit(self):
        # 10m == 600_000 ms; in microseconds that is 600_000 * 1_000.
        event = GroupByTimeEvent(
            name="expr", group_by_time="10m", channel_time_unit="us"
        )
        assert event.get_expression().slice_width == 600_000 * 1_000

    def test_expression_gap_in_channel_unit(self):
        # boundary_gap_ms default 1 ms -> 1_000 us.
        event = GroupByTimeEvent(
            name="expr", group_by_time="10m", channel_time_unit="us", boundary_gap_ms=1
        )
        assert event.get_expression().gap == 1_000


# ===========================================================================
# get_event_type_str
# ===========================================================================
class TestGetEventTypeStr:
    """Tests for get_event_type_str."""

    def test_returns_group_by_time_event(self):
        event = GroupByTimeEvent(name="evt", group_by_time="10m")
        assert event.get_event_type_str() == "GROUP_BY_TIME_EVENT"


# ===========================================================================
# as_dict
# ===========================================================================
class TestAsDict:
    """Tests for as_dict."""

    def test_full_structure(self):
        event = GroupByTimeEvent(name="my_event", group_by_time="10m", desc="test desc")
        d = event.as_dict()

        assert isinstance(d, dict)
        assert d["event_id"] == event.get_id()
        assert d["report_id"] == -1
        assert d["event_type"] == "GROUP_BY_TIME_EVENT"
        assert d["event_name"] == "my_event"
        assert d["event_description"] == "test desc"
        assert d["required_channels"] is None
        assert d["event_expression"].startswith("GroupByTimeExpression")
        assert isinstance(d["definition_hash"], int)
        assert d["attributes"]["grouped_by"] == "10m"


# ===========================================================================
# as_spark_row
# ===========================================================================
class TestAsSparkRow:
    """Tests for as_spark_row."""

    def test_row_has_9_fields(self):
        event = GroupByTimeEvent(name="row_evt", group_by_time="5m")
        row = event.as_spark_row()
        assert len(row) == 9


# ===========================================================================
# determine_definition_hash
# ===========================================================================
class TestDefinitionHash:
    """Tests for determine_definition_hash."""

    def test_deterministic(self):
        ev1 = GroupByTimeEvent(name="a", group_by_time="10m")
        ev2 = GroupByTimeEvent(name="a", group_by_time="10m")
        assert ev1.determine_definition_hash() == ev2.determine_definition_hash()

    def test_differs_by_group_by_time(self):
        ev1 = GroupByTimeEvent(name="a", group_by_time="10m")
        ev2 = GroupByTimeEvent(name="a", group_by_time="5m")
        assert ev1.determine_definition_hash() != ev2.determine_definition_hash()

    def test_differs_by_name(self):
        ev1 = GroupByTimeEvent(name="a", group_by_time="10m")
        ev2 = GroupByTimeEvent(name="b", group_by_time="10m")
        assert ev1.determine_definition_hash() != ev2.determine_definition_hash()

    def test_ignores_description(self):
        ev1 = GroupByTimeEvent(name="ev", group_by_time="10m", desc="Alpha")
        ev2 = GroupByTimeEvent(name="ev", group_by_time="10m", desc="Beta")
        assert ev1.determine_definition_hash() == ev2.determine_definition_hash()


# ===========================================================================
# determine_events (integration, needs Spark)
# ===========================================================================
class TestDetermineEvents:
    """Tests for determine_events (requires Spark session).

    ``GroupByTimeEvent`` now slices over the container's channel-data span via a
    ``GroupByTimeExpression`` solved into a wide ``solved_df`` (the same flow used
    by ``BasicEvent``).  A channel is co-selected so the per-container cache holds
    data to derive the slice extent from.
    """

    def test_valid_fact_dataframe(self, spark, basic_narrow_db):
        event = GroupByTimeEvent(name="10min_slices", group_by_time="10m")

        solved_df = _solve_gbt(spark, basic_narrow_db, event)
        df = GroupByTimeEvent.determine_events(spark, [event], solved_df=solved_df)

        assert df is not None
        assert set(df.columns) == {
            "container_id",
            "event_instance_id",
            "event_id",
            "start_ts",
            "end_ts",
        }
        assert df.count() > 0

    def test_requires_solved_df(self, spark):
        event = GroupByTimeEvent(name="slices", group_by_time="10m")
        with pytest.raises(ValueError, match="requires solved_df"):
            GroupByTimeEvent.determine_events(spark, [event])

    def test_start_ts_less_than_end_ts(self, spark, basic_narrow_db):
        event = GroupByTimeEvent(name="slices", group_by_time="10m")

        solved_df = _solve_gbt(spark, basic_narrow_db, event)
        df = GroupByTimeEvent.determine_events(spark, [event], solved_df=solved_df)

        invalid = df.filter(f.col("start_ts") >= f.col("end_ts")).count()
        assert invalid == 0

    def test_multiple_slices_per_container(self, spark, basic_narrow_db):
        """Using a small slice duration should produce multiple slices."""
        # Channel data spans tens of seconds per container; 10s slices (in the
        # microsecond channel unit) therefore yield several slices each.
        event = GroupByTimeEvent(name="small_slices", group_by_time="10s")

        solved_df = _solve_gbt(spark, basic_narrow_db, event)
        df = GroupByTimeEvent.determine_events(spark, [event], solved_df=solved_df)

        container_count = df.select("container_id").distinct().count()
        total_slices = df.count()
        assert total_slices > container_count

    def test_event_instance_id_unique_per_slice(self, spark, basic_narrow_db):
        """Each slice must receive a distinct event_instance_id."""
        event = GroupByTimeEvent(name="unique_slices", group_by_time="10s")

        solved_df = _solve_gbt(spark, basic_narrow_db, event)
        df = GroupByTimeEvent.determine_events(spark, [event], solved_df=solved_df)

        assert df.count() == df.select("event_instance_id").distinct().count()

    def test_distinct_events_have_distinct_ids(self, spark, basic_narrow_db):
        """Two GroupByTimeEvents with different names produce disjoint ids."""
        event_a = GroupByTimeEvent(name="slices_a", group_by_time="10s")
        event_b = GroupByTimeEvent(name="slices_b", group_by_time="10s")

        solver = KeyValueStoreSolver(spark)
        channel = basic_narrow_db.query.channel(channel_name="Engine RPM")
        solved_df = basic_narrow_db.query.select(
            channel.alias("_ch"),
            event_a.get_expression(),
            event_b.get_expression(),
        ).solve(spark, solver)

        df = GroupByTimeEvent.determine_events(
            spark, [event_a, event_b], solved_df=solved_df
        )

        ids_a = {
            row.event_instance_id
            for row in df.filter(f.col("event_id") == event_a.get_id())
            .select("event_instance_id")
            .collect()
        }
        ids_b = {
            row.event_instance_id
            for row in df.filter(f.col("event_id") == event_b.get_id())
            .select("event_instance_id")
            .collect()
        }
        assert ids_a and ids_b
        assert ids_a.isdisjoint(ids_b)



# ===========================================================================
# determine_metadata_df (needs Spark)
# ===========================================================================
class TestDetermineMetadataDf:
    """Tests for determine_metadata_df (requires Spark session)."""

    def test_valid_dimension_dataframe(self, spark):
        event = GroupByTimeEvent(name="meta_evt", group_by_time="5m", desc="desc")

        metadata_df = GroupByTimeEvent.determine_metadata_df(spark, [event])

        assert metadata_df is not None
        assert "event_id" in metadata_df.columns
        assert "report_id" in metadata_df.columns
        assert "event_name" in metadata_df.columns
        assert "event_type" in metadata_df.columns
        assert "attributes" in metadata_df.columns

    def test_attributes_contain_grouped_by(self):
        """Verify attributes dict includes grouped_by (tested at Python level)."""
        event = GroupByTimeEvent(name="meta_evt2", group_by_time="10m")
        assert event.as_dict()["attributes"]["grouped_by"] == "10m"


# ===========================================================================
# Timestamp correctness after determine_events (integration)
# ===========================================================================
class TestTimestampCorrectness:
    """Tests that start_ts and end_ts values are correct after slicing.

    Slices span the container's channel-data extent (``cache.span()``) in the
    channel time unit.  Consecutive slices are separated by the configured
    ``boundary_gap`` so they remain distinct intervals, so slices are *not*
    contiguous by design (a small gap is lost at each boundary).
    """

    def test_first_slice_starts_at_channel_span_start(self, spark, basic_narrow_db):
        """The first slice start_ts must equal the container's channel-data min tstart."""
        event = GroupByTimeEvent(name="ts_check", group_by_time="10s")
        # _solve_gbt co-selects "Engine RPM" (channel_id 5 in this fixture), so the
        # slice extent derives from that channel's span only.
        channel_min = {
            row.container_id: row.mn
            for row in basic_narrow_db.query.db.channels(spark)
            .filter(f.col("channel_id") == 5)
            .groupBy("container_id")
            .agg(f.min("tstart").alias("mn"))
            .collect()
        }

        solved_df = _solve_gbt(spark, basic_narrow_db, event)
        df = GroupByTimeEvent.determine_events(spark, [event], solved_df=solved_df)

        min_starts = {
            row.container_id: row.min_start
            for row in df.groupBy("container_id")
            .agg(f.min("start_ts").alias("min_start"))
            .collect()
        }
        for cid, min_start in min_starts.items():
            assert min_start == channel_min[cid], (
                f"Container {cid}: first slice start_ts {min_start} "
                f"!= channel span start {channel_min[cid]}"
            )

    def test_last_slice_ends_at_channel_span_end(self, spark, basic_narrow_db):
        """The last slice end_ts must equal the container's channel-data max tend."""
        event = GroupByTimeEvent(name="ts_check", group_by_time="10s")
        # _solve_gbt co-selects "Engine RPM" (channel_id 5 in this fixture), so the
        # slice extent derives from that channel's span only.
        channel_max = {
            row.container_id: row.mx
            for row in basic_narrow_db.query.db.channels(spark)
            .filter(f.col("channel_id") == 5)
            .groupBy("container_id")
            .agg(f.max("tend").alias("mx"))
            .collect()
        }

        solved_df = _solve_gbt(spark, basic_narrow_db, event)
        df = GroupByTimeEvent.determine_events(spark, [event], solved_df=solved_df)

        max_ends = {
            row.container_id: row.max_end
            for row in df.groupBy("container_id").agg(f.max("end_ts").alias("max_end")).collect()
        }
        for cid, max_end in max_ends.items():
            assert max_end == channel_max[cid], (
                f"Container {cid}: last slice end_ts {max_end} "
                f"!= channel span end {channel_max[cid]}"
            )

    def test_boundary_gap_between_consecutive_slices(self, spark, basic_narrow_db):
        """Consecutive slices must be separated by exactly the boundary gap."""
        # boundary_gap_ms=1 -> 1_000 us in the channel unit.
        event = GroupByTimeEvent(
            name="gap_check", group_by_time="10s", boundary_gap_ms=1, channel_time_unit="us"
        )
        expected_gap = 1_000

        solved_df = _solve_gbt(spark, basic_narrow_db, event)
        df = GroupByTimeEvent.determine_events(spark, [event], solved_df=solved_df)

        from pyspark.sql.window import Window

        w = Window.partitionBy("container_id").orderBy("start_ts")
        df_with_prev = df.withColumn("prev_end_ts", f.lag("end_ts").over(w))
        bad_gaps = df_with_prev.filter(
            (f.col("prev_end_ts").isNotNull())
            & ((f.col("start_ts") - f.col("prev_end_ts")) != f.lit(expected_gap))
        )
        assert bad_gaps.count() == 0, (
            f"Found {bad_gaps.count()} slice boundaries not separated by {expected_gap}"
        )

    def test_no_slice_exceeds_group_by_duration(self, spark, basic_narrow_db):
        """No slice duration should exceed the configured slice width (channel unit)."""
        event = GroupByTimeEvent(
            name="duration_check", group_by_time="10s", channel_time_unit="us"
        )
        slice_width_us = 10 * 1_000_000  # 10s in microseconds

        solved_df = _solve_gbt(spark, basic_narrow_db, event)
        df = GroupByTimeEvent.determine_events(spark, [event], solved_df=solved_df)

        violations = df.filter((f.col("end_ts") - f.col("start_ts")) > slice_width_us)
        assert violations.count() == 0, f"Found {violations.count()} slices exceeding 10s"

    def test_all_slices_have_positive_duration(self, spark, basic_narrow_db):
        """Every slice must have end_ts > start_ts."""
        event = GroupByTimeEvent(name="positive_dur", group_by_time="10s")

        solved_df = _solve_gbt(spark, basic_narrow_db, event)
        df = GroupByTimeEvent.determine_events(spark, [event], solved_df=solved_df)

        invalid = df.filter(f.col("end_ts") <= f.col("start_ts"))
        assert invalid.count() == 0, f"Found {invalid.count()} slices with end_ts <= start_ts"

    def test_start_ts_and_end_ts_are_long_type(self, spark, basic_narrow_db):
        """start_ts and end_ts must be LongType (epoch time in the channel unit)."""
        from pyspark.sql.types import LongType

        event = GroupByTimeEvent(name="type_check", group_by_time="10s")

        solved_df = _solve_gbt(spark, basic_narrow_db, event)
        df = GroupByTimeEvent.determine_events(spark, [event], solved_df=solved_df)

        schema_fields = {field.name: field.dataType for field in df.schema.fields}
        assert isinstance(schema_fields["start_ts"], LongType)
        assert isinstance(schema_fields["end_ts"], LongType)



# ===========================================================================
# End-to-end Report integration (determine_report)
# ===========================================================================
class TestGroupByTimeEventInReport:
    """Integration test: GroupByTimeEvent through a full Report.determine_report cycle.

    A ``StatsAggregator`` bound to the event is included so a channel is loaded
    into the solve batch (the slice extent derives from the container's
    channel-data span).  The key invariant is that the ``event_instance_id``
    values in the event-instance fact and the aggregation fact align, so the two
    facts can be joined on ``event_instance_id``.
    """

    @staticmethod
    def _build_report(spark, basic_narrow_db, name):
        from unittest.mock import create_autospec, patch

        from databricks.sdk import WorkspaceClient

        from impulse_reporting.core.report import Report

        config = {
            "source": {
                "container_metrics_table": "spark_catalog.silver.container_metrics",
                "channel_metrics_table": "spark_catalog.silver.channel_metrics",
                "channels_uri": "spark_catalog.silver.channels",
            },
            "query_engine": {"solver": "KeyValueStoreSolver"},
        }

        with (
            patch.object(Report, "create_measurement_db", return_value=basic_narrow_db),
            patch.object(Report, "create_query_builder", return_value=basic_narrow_db.query),
            patch.object(Report, "create_solver", return_value=KeyValueStoreSolver(spark)),
            patch.object(Report, "create_sink", return_value=None),
        ):
            report = Report(
                name=name,
                spark=spark,
                workspace_client=create_autospec(WorkspaceClient),
                config=config,
            )
        return report

    def test_report_determine_produces_valid_timestamps(self, spark, basic_narrow_db):
        """determine_report with a GroupByTimeEvent yields multiple valid slices."""
        from impulse_reporting.aggregations.stats_aggregator import StatsAggregator
        from impulse_reporting.core.page import Page

        report = self._build_report(spark, basic_narrow_db, "test_groupby_report")

        event = GroupByTimeEvent(name="10s_slices", group_by_time="10s")
        report.add_event(event)

        page = Page(page_number=1)
        report.add_page(page)
        page.add_aggregation(
            StatsAggregator(
                name="rpm_stats",
                input_expressions=[report.get_db().query.channel(channel_name="Engine RPM")],
                channel_names=["Engine RPM"],
                statistics=["min", "max", "mean"],
                event=event,
            )
        )

        report.determine_report()

        assert "GROUP_BY_TIME_EVENT" in report.event_dfs
        changed_df = report.event_dfs["GROUP_BY_TIME_EVENT"]["changed"]
        assert changed_df is not None

        container_count = changed_df.select("container_id").distinct().count()
        total_slices = changed_df.count()
        assert total_slices > container_count, "expected multiple slices per container"

        # Every slice is a valid, positive-duration interval.
        invalid = changed_df.filter(f.col("start_ts") >= f.col("end_ts")).count()
        assert invalid == 0

        # event_instance_id is unique per slice.
        assert total_slices == changed_df.select("event_instance_id").distinct().count()

    def test_event_and_aggregation_ids_match(self, spark, basic_narrow_db):
        """The core fix: aggregation event_instance_ids all map to event ids."""
        from impulse_reporting.aggregations.stats_aggregator import StatsAggregator
        from impulse_reporting.core.page import Page

        report = self._build_report(spark, basic_narrow_db, "test_id_match_report")

        event = GroupByTimeEvent(name="10s_slices", group_by_time="10s")
        report.add_event(event)

        page = Page(page_number=1)
        report.add_page(page)
        page.add_aggregation(
            StatsAggregator(
                name="rpm_stats",
                input_expressions=[report.get_db().query.channel(channel_name="Engine RPM")],
                channel_names=["Engine RPM"],
                statistics=["min", "max", "mean"],
                event=event,
            )
        )

        report.determine_report()

        event_df = report.event_dfs["GROUP_BY_TIME_EVENT"]["changed"]
        stats_df = report.aggregation_dfs["STATS_AGGREGATOR"]["changed"]

        event_ids = event_df.select("event_instance_id").distinct()
        stats_ids = stats_df.select("event_instance_id").distinct()

        n_stats_ids = stats_ids.count()
        matched = stats_ids.join(event_ids, on="event_instance_id", how="inner").count()

        assert n_stats_ids > 0, "aggregation must produce rows"
        assert matched == n_stats_ids, (
            "every aggregation event_instance_id must correspond to an event "
            f"event_instance_id (matched {matched} of {n_stats_ids})"
        )

