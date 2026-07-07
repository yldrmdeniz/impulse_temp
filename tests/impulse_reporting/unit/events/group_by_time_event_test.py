"""Unit tests for GroupByTimeEvent."""

import pytest
import pyspark.sql.functions as f

from impulse_query_engine.analyze.query.solvers.key_value_store_solver import KeyValueStoreSolver
from impulse_reporting.events.group_by_time_event import (
    GroupByTimeEvent,
    TimeUnit,
    _parse_duration,
)
from tests.conftest import basic_narrow_db, spark


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

    def test_returns_fixed_duration_expression(self):
        event = GroupByTimeEvent(name="sliced_expr", group_by_time="10m")
        assert event.get_expression() is not None
        assert str(event.get_expression()) == "FixedDurationIntervalsExpression<duration=600000>"


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
        assert d["event_expression"] == "NA"
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
    """Tests for determine_events (requires Spark session)."""

    def test_valid_fact_dataframe(self, spark, basic_narrow_db):
        event = GroupByTimeEvent(name="10min_slices", group_by_time="10m")

        df = GroupByTimeEvent.determine_events(
            spark,
            [event],
            query=basic_narrow_db.query,
            solver=KeyValueStoreSolver(spark),
        )

        assert df is not None
        assert set(df.columns) == {
            "container_id",
            "event_instance_id",
            "event_id",
            "start_ts",
            "end_ts",
        }
        assert df.count() > 0

    def test_start_ts_less_than_end_ts(self, spark, basic_narrow_db):
        event = GroupByTimeEvent(name="slices", group_by_time="10m")

        df = GroupByTimeEvent.determine_events(
            spark,
            [event],
            query=basic_narrow_db.query,
            solver=KeyValueStoreSolver(spark),
        )

        invalid = df.filter(f.col("start_ts") >= f.col("end_ts")).count()
        assert invalid == 0

    def test_multiple_slices_per_container(self, spark, basic_narrow_db):
        """Using a small slice duration should produce multiple slices."""
        event = GroupByTimeEvent(name="small_slices", group_by_time="1s")

        df = GroupByTimeEvent.determine_events(
            spark,
            [event],
            query=basic_narrow_db.query,
            solver=KeyValueStoreSolver(spark),
        )

        # With 1s (1000ms) slices, we should get more rows than containers
        container_count = df.select("container_id").distinct().count()
        total_slices = df.count()
        assert total_slices > container_count


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
    """Tests that start_ts and end_ts values are correct after slicing."""

    def test_first_slice_start_matches_container_start(self, spark, basic_narrow_db):
        """The first slice's start_ts must equal the container's original start_ts."""
        event = GroupByTimeEvent(name="ts_check", group_by_time="10m")
        solver = KeyValueStoreSolver(spark)

        # Get the original container start timestamps
        container_tags_df = solver.filter_container_tags(spark, basic_narrow_db.query)
        container_metrics_df = solver.filter_container_metrics(
            spark, basic_narrow_db.query, container_tags_df, None
        )
        original_starts = {
            row.container_id: row.start_ts
            for row in container_metrics_df.select("container_id", "start_ts").collect()
        }

        # Get the event slices
        df = GroupByTimeEvent.determine_events(
            spark,
            [event],
            query=basic_narrow_db.query,
            solver=solver,
        )

        # For each container, the minimum start_ts should equal the original
        min_starts = {
            row.container_id: row.min_start
            for row in df.groupBy("container_id")
            .agg(f.min("start_ts").alias("min_start"))
            .collect()
        }

        for cid, original_start in original_starts.items():
            assert min_starts[cid] == original_start, (
                f"Container {cid}: first slice start_ts {min_starts[cid]} "
                f"!= container start_ts {original_start}"
            )

    def test_last_slice_end_matches_container_stop(self, spark, basic_narrow_db):
        """The last slice's end_ts must equal the container's original stop_ts."""
        event = GroupByTimeEvent(name="ts_check", group_by_time="10m")
        solver = KeyValueStoreSolver(spark)

        container_tags_df = solver.filter_container_tags(spark, basic_narrow_db.query)
        container_metrics_df = solver.filter_container_metrics(
            spark, basic_narrow_db.query, container_tags_df, None
        )
        original_stops = {
            row.container_id: row.stop_ts
            for row in container_metrics_df.select("container_id", "stop_ts").collect()
        }

        df = GroupByTimeEvent.determine_events(
            spark,
            [event],
            query=basic_narrow_db.query,
            solver=solver,
        )

        max_ends = {
            row.container_id: row.max_end
            for row in df.groupBy("container_id").agg(f.max("end_ts").alias("max_end")).collect()
        }

        for cid, original_stop in original_stops.items():
            assert max_ends[cid] == original_stop, (
                f"Container {cid}: last slice end_ts {max_ends[cid]} "
                f"!= container stop_ts {original_stop}"
            )

    def test_slices_are_contiguous(self, spark, basic_narrow_db):
        """Each slice's start_ts must equal the previous slice's end_ts (no gaps)."""
        event = GroupByTimeEvent(name="contiguous_check", group_by_time="10m")
        solver = KeyValueStoreSolver(spark)

        df = GroupByTimeEvent.determine_events(
            spark,
            [event],
            query=basic_narrow_db.query,
            solver=solver,
        )

        from pyspark.sql.window import Window

        w = Window.partitionBy("container_id").orderBy("start_ts")
        df_with_prev = df.withColumn("prev_end_ts", f.lag("end_ts").over(w))

        # Filter to rows that have a previous slice (skip first slice per container)
        gaps = df_with_prev.filter(
            (f.col("prev_end_ts").isNotNull()) & (f.col("start_ts") != f.col("prev_end_ts"))
        )
        assert gaps.count() == 0, f"Found {gaps.count()} gaps between slices"

    def test_no_slice_exceeds_group_by_duration(self, spark, basic_narrow_db):
        """No slice duration should exceed the configured group_by_ms."""
        group_by_time = "10m"
        group_by_ms = 600_000
        event = GroupByTimeEvent(name="duration_check", group_by_time=group_by_time)
        solver = KeyValueStoreSolver(spark)

        df = GroupByTimeEvent.determine_events(
            spark,
            [event],
            query=basic_narrow_db.query,
            solver=solver,
        )

        violations = df.filter((f.col("end_ts") - f.col("start_ts")) > group_by_ms)
        assert (
            violations.count() == 0
        ), f"Found {violations.count()} slices exceeding {group_by_time}"

    def test_all_slices_have_positive_duration(self, spark, basic_narrow_db):
        """Every slice must have end_ts > start_ts."""
        event = GroupByTimeEvent(name="positive_dur", group_by_time="10m")
        solver = KeyValueStoreSolver(spark)

        df = GroupByTimeEvent.determine_events(
            spark,
            [event],
            query=basic_narrow_db.query,
            solver=solver,
        )

        invalid = df.filter(f.col("end_ts") <= f.col("start_ts"))
        assert invalid.count() == 0, f"Found {invalid.count()} slices with end_ts <= start_ts"

    def test_start_ts_and_end_ts_are_long_type(self, spark, basic_narrow_db):
        """start_ts and end_ts must be LongType (epoch milliseconds)."""
        from pyspark.sql.types import LongType

        event = GroupByTimeEvent(name="type_check", group_by_time="10m")
        solver = KeyValueStoreSolver(spark)

        df = GroupByTimeEvent.determine_events(
            spark,
            [event],
            query=basic_narrow_db.query,
            solver=solver,
        )

        schema_fields = {field.name: field.dataType for field in df.schema.fields}
        assert isinstance(schema_fields["start_ts"], LongType)
        assert isinstance(schema_fields["end_ts"], LongType)


# ===========================================================================
# End-to-end Report integration (determine_report)
# ===========================================================================
class TestGroupByTimeEventInReport:
    """Integration test: GroupByTimeEvent through a full Report.determine_report cycle."""

    def test_report_determine_produces_valid_timestamps(self, spark, basic_narrow_db):
        """Running determine_report with a GroupByTimeEvent yields correct start_ts/end_ts."""
        from unittest.mock import create_autospec, patch

        from databricks.sdk import WorkspaceClient

        from impulse_reporting.core.report import Report
        from impulse_reporting.events.group_by_time_event import GroupByTimeEvent

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
                name="test_groupby_report",
                spark=spark,
                workspace_client=create_autospec(WorkspaceClient),
                config=config,
            )

        event = GroupByTimeEvent(name="1min_slices", group_by_time="1m")
        report.add_event(event)
        report.determine_report()

        # Get the event fact DataFrame from report
        event_dfs = report.event_dfs
        assert "GROUP_BY_TIME_EVENT" in event_dfs

        changed_df = event_dfs["GROUP_BY_TIME_EVENT"]["changed"]
        assert changed_df is not None

        # Expected slices with 1m (60s) duration:
        # cid=1: start=1751528502708, stop=1751528610253, dur=107545ms → 2 slices
        # cid=2: start=1751528501483, stop=1751528610235, dur=108752ms → 2 slices
        # cid=3: start=1751528500169, stop=1751528610252, dur=110083ms → 2 slices
        expected_slices = {
            1: [
                (1751528502708, 1751528562708),
                (1751528562708, 1751528610253),
            ],
            2: [
                (1751528501483, 1751528561483),
                (1751528561483, 1751528610235),
            ],
            3: [
                (1751528500169, 1751528560169),
                (1751528560169, 1751528610252),
            ],
        }

        assert changed_df.count() == 6, f"Expected 6 slices total, got {changed_df.count()}"

        # Verify exact start_ts/end_ts per container
        rows = changed_df.orderBy("container_id", "start_ts").collect()
        actual_slices = {}
        for row in rows:
            actual_slices.setdefault(row.container_id, []).append((row.start_ts, row.end_ts))

        for cid, expected in expected_slices.items():
            actual = actual_slices[cid]
            assert actual == expected, f"Container {cid}: expected {expected}, got {actual}"

    def test_report_produces_exact_slice_count(self, spark, basic_narrow_db):
        """Verify the exact number of slices matches ceil(duration / group_by_ms) per container."""
        from unittest.mock import create_autospec, patch

        from databricks.sdk import WorkspaceClient

        from impulse_reporting.core.report import Report
        from impulse_reporting.events.group_by_time_event import GroupByTimeEvent

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
                name="test_slice_count_report",
                spark=spark,
                workspace_client=create_autospec(WorkspaceClient),
                config=config,
            )

        group_by_time = "1m"
        group_by_ms = 60_000
        event = GroupByTimeEvent(name="1min_slices", group_by_time=group_by_time)
        report.add_event(event)
        report.determine_report()

        changed_df = report.event_dfs["GROUP_BY_TIME_EVENT"]["changed"]

        # Hardcoded expected slices with 1m (60000ms) duration:
        # cid=1: start=1751528502708, stop=1751528610253, dur=107545ms → 2 slices
        # cid=2: start=1751528501483, stop=1751528610235, dur=108752ms → 2 slices
        # cid=3: start=1751528500169, stop=1751528610252, dur=110083ms → 2 slices
        expected_slices = {
            1: [
                (1751528502708, 1751528562708),
                (1751528562708, 1751528610253),
            ],
            2: [
                (1751528501483, 1751528561483),
                (1751528561483, 1751528610235),
            ],
            3: [
                (1751528500169, 1751528560169),
                (1751528560169, 1751528610252),
            ],
        }
        expected_total = 6

        # Assert total slice count
        actual_total = changed_df.count()
        assert (
            actual_total == expected_total
        ), f"Expected {expected_total} total slices, got {actual_total}"

        # Assert exact start_ts/end_ts per container
        rows = changed_df.orderBy("container_id", "start_ts").collect()
        actual_slices = {}
        for row in rows:
            actual_slices.setdefault(row.container_id, []).append((row.start_ts, row.end_ts))

        for cid, expected in expected_slices.items():
            actual = actual_slices[cid]
            assert len(actual) == len(
                expected
            ), f"Container {cid}: expected {len(expected)} slices, got {len(actual)}"
            assert actual == expected, f"Container {cid}: expected {expected}, got {actual}"
