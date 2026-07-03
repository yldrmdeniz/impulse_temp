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

    def test_parse_milliseconds(self):
        assert _parse_duration("500ms") == 500

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

    def test_returns_none(self):
        event = GroupByTimeEvent(name="no_expr", group_by_time="10m")
        assert event.get_expression() is None


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
        event = GroupByTimeEvent(name="small_slices", group_by_time="1ms")

        df = GroupByTimeEvent.determine_events(
            spark,
            [event],
            query=basic_narrow_db.query,
            solver=KeyValueStoreSolver(spark),
        )

        # With 1ms slices, we should get more rows than containers
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
# determine_events with qualified column names (regression test)
# ===========================================================================
class TestDetermineEventsQualifiedColumns:
    """Regression tests for DataFrames with fully-qualified column names.

    In real Databricks environments, Spark can return DataFrames whose columns
    are qualified like `catalog.schema.table.col`. withColumnRenamed fails
    silently on such names, so we use withColumn + f.col() instead.
    """

    def test_qualified_stop_ts_col(self, spark, basic_narrow_db):
        """determine_events should work when solver.config.stop_ts_col is qualified."""
        from unittest.mock import MagicMock, patch

        event = GroupByTimeEvent(name="qualified_test", group_by_time="10m")
        solver = KeyValueStoreSolver(spark)

        # Get a real container_metrics DataFrame from the solver
        container_tags_df = solver.filter_container_tags(spark, basic_narrow_db.query)
        real_metrics_df = solver.filter_container_metrics(
            spark, basic_narrow_db.query, container_tags_df, None
        )

        # Rename columns to simulate fully-qualified names (catalog.schema.table.col)
        qualified_start = "development.silver.container_metric.start_ts"
        qualified_stop = "development.silver.container_metric.end_ts"
        qualified_df = real_metrics_df.withColumnRenamed(
            "start_ts", qualified_start
        ).withColumnRenamed("stop_ts", qualified_stop)

        # Mock solver to return the qualified DataFrame and config
        mock_solver = MagicMock()
        mock_solver.filter_container_tags.return_value = container_tags_df
        mock_solver.filter_container_metrics.return_value = qualified_df
        mock_solver.config.start_ts_col = qualified_start
        mock_solver.config.stop_ts_col = qualified_stop
        mock_solver.config.container_id_col = solver.config.container_id_col

        df = GroupByTimeEvent.determine_events(
            spark,
            [event],
            query=basic_narrow_db.query,
            solver=mock_solver,
        )

        assert df is not None
        assert "start_ts" in df.columns
        assert "end_ts" in df.columns
        assert df.count() > 0
