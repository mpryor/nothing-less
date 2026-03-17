"""Tests for session save/load round-trip and apply_buffer_state helpers."""

import re

import pytest

from nless.app import NlessApp
from nless.session import (
    SessionBufferState,
    SessionColumn,
    SessionComputedColumn,
    SessionFilter,
    _apply_session_columns,
    _apply_session_computed_columns,
    _apply_session_filters,
    _apply_session_sort,
    _apply_session_unique_columns,
    _deserialize_buffer_state,
    _sources_match,
    apply_buffer_state,
    capture_buffer_state,
)
from nless.types import CliArgs, Filter


@pytest.fixture
def cli_args():
    return CliArgs(delimiter=None, filters=[], unique_keys=set(), sort_by=None)


class TestSourcesMatch:
    def test_exact_match(self):
        assert _sources_match("/home/user/file.log", "/home/user/file.log")

    def test_basename_match(self):
        assert _sources_match("/home/user/file.log", "/tmp/file.log")

    def test_no_match(self):
        assert not _sources_match("/home/user/a.log", "/tmp/b.log")


class TestDeserializeBufferState:
    def test_minimal_dict(self):
        state = _deserialize_buffer_state(
            {"delimiter": ",", "raw_mode": False, "filters": [], "columns": []}
        )
        assert state.delimiter == ","
        assert state.raw_mode is False
        assert state.filters == []

    def test_missing_optional_fields_use_defaults(self):
        state = _deserialize_buffer_state(
            {"delimiter": None, "raw_mode": False, "filters": [], "columns": []}
        )
        assert state.delimiter_regex is None
        assert state.delimiter_name is None
        assert state.sort_column is None
        assert state.sort_reverse is False
        assert state.unique_column_names == []
        assert state.computed_columns == []
        assert state.highlights == []
        assert state.time_window is None
        assert state.rolling_time_window is False
        assert state.is_tailing is False
        assert state.tab_name == ""
        assert state.search_term is None
        assert state.cursor_row == 0
        assert state.cursor_column == 0
        assert state.source_labels == []

    def test_computed_column_with_regex_flags(self):
        state = _deserialize_buffer_state(
            {
                "delimiter": ",",
                "raw_mode": False,
                "filters": [],
                "columns": [],
                "computed_columns": [
                    {
                        "name": "cc1",
                        "col_ref": "col1",
                        "col_ref_index": 0,
                        "json_ref": "",
                        "delimiter_regex": r"\d+",
                        "delimiter_regex_flags": re.IGNORECASE,
                    }
                ],
            }
        )
        assert len(state.computed_columns) == 1
        assert state.computed_columns[0].delimiter_regex_flags == re.IGNORECASE


class TestCaptureAndApplyRoundTrip:
    @pytest.mark.asyncio
    async def test_basic_round_trip(self, cli_args):
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test():
            buf = app.buffers[0]
            buf.add_logs(["name,age,city", "Alice,30,NYC", "Bob,25,SF"])

            state = capture_buffer_state(buf)
            assert state.delimiter == ","
            assert len(state.columns) == len(buf.current_columns)

            # Apply to same buffer (idempotent)
            skipped = apply_buffer_state(buf, state)
            assert skipped == []

    @pytest.mark.asyncio
    async def test_sort_round_trip(self, cli_args):
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test():
            buf = app.buffers[0]
            buf.add_logs(["name,age", "Alice,30", "Bob,25"])
            buf.query.sort_column = "name"
            buf.query.sort_reverse = True

            state = capture_buffer_state(buf)
            assert state.sort_column == "name"
            assert state.sort_reverse is True

            buf.query.clear_sort()
            apply_buffer_state(buf, state)
            assert buf.query.sort_column == "name"
            assert buf.query.sort_reverse is True

    @pytest.mark.asyncio
    async def test_filter_round_trip(self, cli_args):
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test():
            buf = app.buffers[0]
            buf.add_logs(["name,age", "Alice,30", "Bob,25"])
            buf.query.filters = [
                Filter(
                    column="name",
                    pattern=re.compile("Alice", re.IGNORECASE),
                )
            ]

            state = capture_buffer_state(buf)
            assert len(state.filters) == 1
            assert state.filters[0].flags & re.IGNORECASE

            buf.query.clear_all()
            apply_buffer_state(buf, state)
            assert len(buf.query.filters) == 1
            assert buf.query.filters[0].pattern.flags & re.IGNORECASE

    @pytest.mark.asyncio
    async def test_highlight_round_trip(self, cli_args):
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test():
            buf = app.buffers[0]
            buf.add_logs(["name,age", "Alice,30"])
            buf.regex_highlights = [(re.compile("Alice"), "#ff0000")]

            state = capture_buffer_state(buf)
            assert len(state.highlights) == 1
            assert state.highlights[0].color == "#ff0000"

            buf.regex_highlights = []
            apply_buffer_state(buf, state)
            assert len(buf.regex_highlights) == 1


class TestApplySessionFilters:
    @pytest.mark.asyncio
    async def test_filter_with_missing_column(self, cli_args):
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test():
            buf = app.buffers[0]
            buf.add_logs(["name,age", "Alice,30"])
            state = SessionBufferState(
                delimiter=",",
                delimiter_regex=None,
                delimiter_name=None,
                raw_mode=False,
                sort_column=None,
                sort_reverse=False,
                filters=[
                    SessionFilter(column="nonexistent", pattern="foo", exclude=False)
                ],
                columns=[],
                unique_column_names=[],
                highlights=[],
                time_window=None,
                rolling_time_window=False,
                is_tailing=False,
            )
            skipped = []
            _apply_session_filters(buf, state, skipped)
            assert any("nonexistent" in s for s in skipped)
            # Filter is still applied (column might appear later)
            assert len(buf.query.filters) == 1


class TestApplySessionSort:
    @pytest.mark.asyncio
    async def test_sort_missing_column(self, cli_args):
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test():
            buf = app.buffers[0]
            buf.add_logs(["name,age", "Alice,30"])
            state = SessionBufferState(
                delimiter=",",
                delimiter_regex=None,
                delimiter_name=None,
                raw_mode=False,
                sort_column="nonexistent",
                sort_reverse=True,
                filters=[],
                columns=[],
                unique_column_names=[],
                highlights=[],
                time_window=None,
                rolling_time_window=False,
                is_tailing=False,
            )
            skipped = []
            _apply_session_sort(buf, state, skipped)
            assert any("nonexistent" in s for s in skipped)
            assert buf.query.sort_column is None  # not applied


class TestApplySessionColumns:
    @pytest.mark.asyncio
    async def test_unmatched_columns_reported(self, cli_args):
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test():
            buf = app.buffers[0]
            buf.add_logs(["name,age", "Alice,30"])
            state = SessionBufferState(
                delimiter=",",
                delimiter_regex=None,
                delimiter_name=None,
                raw_mode=False,
                sort_column=None,
                sort_reverse=False,
                filters=[],
                columns=[
                    SessionColumn(
                        name="nonexistent",
                        render_position=0,
                        hidden=False,
                        pinned=False,
                    )
                ],
                unique_column_names=[],
                highlights=[],
                time_window=None,
                rolling_time_window=False,
                is_tailing=False,
            )
            skipped = []
            _apply_session_columns(buf, state, skipped)
            assert any("column setting" in s for s in skipped)


class TestApplySessionComputedColumns:
    @pytest.mark.asyncio
    async def test_missing_source_column(self, cli_args):
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test():
            buf = app.buffers[0]
            buf.add_logs(["name,age", "Alice,30"])
            state = SessionBufferState(
                delimiter=",",
                delimiter_regex=None,
                delimiter_name=None,
                raw_mode=False,
                sort_column=None,
                sort_reverse=False,
                filters=[],
                columns=[],
                unique_column_names=[],
                computed_columns=[
                    SessionComputedColumn(
                        name="cc1",
                        col_ref="nonexistent",
                        col_ref_index=0,
                        json_ref="",
                    )
                ],
                highlights=[],
                time_window=None,
                rolling_time_window=False,
                is_tailing=False,
            )
            skipped = []
            _apply_session_computed_columns(buf, state, skipped)
            assert any("nonexistent" in s for s in skipped)

    @pytest.mark.asyncio
    async def test_regex_flags_preserved(self, cli_args):
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test():
            buf = app.buffers[0]
            buf.add_logs(["name,age", "Alice,30"])
            state = SessionBufferState(
                delimiter=",",
                delimiter_regex=None,
                delimiter_name=None,
                raw_mode=False,
                sort_column=None,
                sort_reverse=False,
                filters=[],
                columns=[],
                unique_column_names=[],
                computed_columns=[
                    SessionComputedColumn(
                        name="cc1",
                        col_ref="name",
                        col_ref_index=0,
                        json_ref="",
                        delimiter_regex=r"\d+",
                        delimiter_regex_flags=re.IGNORECASE,
                    )
                ],
                highlights=[],
                time_window=None,
                rolling_time_window=False,
                is_tailing=False,
            )
            skipped = []
            _apply_session_computed_columns(buf, state, skipped)
            assert skipped == []
            cc = next(c for c in buf.current_columns if c.name == "cc1")
            assert isinstance(cc.delimiter, re.Pattern)
            assert cc.delimiter.flags & re.IGNORECASE


class TestApplySessionUniqueColumns:
    @pytest.mark.asyncio
    async def test_missing_unique_column(self, cli_args):
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test():
            buf = app.buffers[0]
            buf.add_logs(["name,age", "Alice,30"])
            state = SessionBufferState(
                delimiter=",",
                delimiter_regex=None,
                delimiter_name=None,
                raw_mode=False,
                sort_column=None,
                sort_reverse=False,
                filters=[],
                columns=[],
                unique_column_names=["nonexistent"],
                highlights=[],
                time_window=None,
                rolling_time_window=False,
                is_tailing=False,
            )
            skipped = []
            _apply_session_unique_columns(buf, state, skipped)
            assert any("nonexistent" in s for s in skipped)


class TestApplyBufferStateIntegration:
    @pytest.mark.asyncio
    async def test_full_apply_reports_all_skips(self, cli_args):
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test():
            buf = app.buffers[0]
            buf.add_logs(["name,age", "Alice,30"])
            state = SessionBufferState(
                delimiter=",",
                delimiter_regex=None,
                delimiter_name=None,
                raw_mode=False,
                sort_column="nonexistent_sort",
                sort_reverse=False,
                filters=[
                    SessionFilter(
                        column="nonexistent_filter", pattern="x", exclude=False
                    )
                ],
                columns=[
                    SessionColumn(
                        name="gone", render_position=0, hidden=True, pinned=False
                    )
                ],
                unique_column_names=[],
                highlights=[],
                time_window=None,
                rolling_time_window=False,
                is_tailing=False,
            )
            skipped = apply_buffer_state(buf, state)
            assert len(skipped) >= 2  # sort + column setting at minimum
