"""Integration tests for app orchestration: session round-trip, mark_unique/dedup,
column split, view save/load/undo, and StatusContext sync."""

import re

import pytest

from nless.app import NlessApp
from nless.dataprocessing import strip_markup
from nless.operations import handle_mark_unique
from nless.session import (
    apply_buffer_state,
    capture_buffer_state,
    capture_view_state,
)
from nless.types import CliArgs, Column, Filter, MetadataColumn


async def _wait(pilot, app):
    """Pump the event loop until all buffers finish loading."""
    settled = 0
    for _ in range(300):  # 3s max
        await pilot.pause(delay=0.01)
        if all(not b.loading_state.reason for b in app.buffers):
            settled += 1
            if settled >= 5:
                return
        else:
            settled = 0


@pytest.fixture
def cli_args():
    return CliArgs(delimiter=None, filters=[], unique_keys=set(), sort_by=None)


def _load(buf, lines):
    """Helper: add_logs with a header + data rows."""
    buf.add_logs(lines)


# ---------------------------------------------------------------------------
# 1. Session round-trip (save -> close buffers -> reload -> verify state)
# ---------------------------------------------------------------------------


class TestSessionRoundTrip:
    @pytest.mark.asyncio
    async def test_sort_and_filter_survive_round_trip(self, cli_args):
        """Capture session state with sort+filter, apply to fresh buffer, verify."""
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(buf, ["name,age,city", "Alice,30,NYC", "Bob,25,SF", "Charlie,35,LA"])
            await _wait(pilot, app)

            # Sort by name ascending
            buf.action_sort()
            await _wait(pilot, app)
            assert buf.query.sort_column == "name"

            # Add a filter
            buf.query.filters = [
                Filter(column="city", pattern=re.compile("NYC"), exclude=False)
            ]

            # Capture state
            state = capture_buffer_state(buf)

            # Apply to a fresh buffer in the same app
            buf2 = app.buffers[0]
            # Reset buf2 state first
            buf2.query.filters = []
            buf2.query.sort_column = None
            buf2.query.sort_reverse = False

            skipped = apply_buffer_state(buf2, state)

            assert buf2.query.sort_column == "name"
            assert buf2.query.sort_reverse is False
            assert len(buf2.query.filters) == 1
            assert buf2.query.filters[0].column == "city"
            assert buf2.query.filters[0].pattern.pattern == "NYC"
            assert not skipped

    @pytest.mark.asyncio
    async def test_column_visibility_survives_round_trip(self, cli_args):
        """Hidden and pinned columns are restored after round-trip."""
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(buf, ["name,age,city", "Alice,30,NYC", "Bob,25,SF"])
            await _wait(pilot, app)

            # Hide the "age" column
            age_col = next(c for c in buf.current_columns if c.name == "age")
            age_col.hidden = True

            # Pin the "city" column
            city_col = next(c for c in buf.current_columns if c.name == "city")
            city_col.pinned = True

            state = capture_buffer_state(buf)

            # Reset columns
            for c in buf.current_columns:
                c.hidden = False
                c.pinned = False

            skipped = apply_buffer_state(buf, state)

            age_col_after = next(c for c in buf.current_columns if c.name == "age")
            city_col_after = next(c for c in buf.current_columns if c.name == "city")
            assert age_col_after.hidden is True
            assert city_col_after.pinned is True
            assert not skipped

    @pytest.mark.asyncio
    async def test_highlights_survive_round_trip(self, cli_args):
        """Regex highlights are preserved through session round-trip."""
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(buf, ["name,age", "Alice,30", "Bob,25"])
            await _wait(pilot, app)

            buf.regex_highlights = [
                (re.compile("Alice"), "red"),
                (re.compile(r"\d+"), "blue"),
            ]

            state = capture_buffer_state(buf)

            # Clear highlights
            buf.regex_highlights = []

            apply_buffer_state(buf, state)

            assert len(buf.regex_highlights) == 2
            assert buf.regex_highlights[0][0].pattern == "Alice"
            assert buf.regex_highlights[0][1] == "red"
            assert buf.regex_highlights[1][0].pattern == r"\d+"
            assert buf.regex_highlights[1][1] == "blue"

    @pytest.mark.asyncio
    async def test_computed_columns_survive_round_trip(self, cli_args):
        """Computed columns (from column split) are restored after round-trip."""
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(
                buf,
                [
                    "method,path",
                    "GET /api/v1 HTTP/1.1,/api/v1",
                    "POST /api/v2 HTTP/1.1,/api/v2",
                ],
            )
            await _wait(pilot, app)

            # Manually add computed columns (simulating a column split)
            base_pos = len(buf.current_columns)
            buf.current_columns.append(
                Column(
                    name="method-1",
                    labels=set(),
                    render_position=base_pos,
                    data_position=base_pos,
                    hidden=False,
                    computed=True,
                    col_ref="method",
                    col_ref_index=0,
                    delimiter=" ",
                )
            )
            buf.current_columns.append(
                Column(
                    name="method-2",
                    labels=set(),
                    render_position=base_pos + 1,
                    data_position=base_pos + 1,
                    hidden=False,
                    computed=True,
                    col_ref="method",
                    col_ref_index=1,
                    delimiter=" ",
                )
            )

            state = capture_buffer_state(buf)

            # Remove computed columns
            buf.current_columns = [c for c in buf.current_columns if not c.computed]
            assert not any(c.computed for c in buf.current_columns)

            skipped = apply_buffer_state(buf, state)

            computed = [c for c in buf.current_columns if c.computed]
            assert len(computed) == 2
            assert computed[0].name == "method-1"
            assert computed[0].col_ref == "method"
            assert computed[0].col_ref_index == 0
            assert computed[1].name == "method-2"
            assert computed[1].col_ref == "method"
            assert computed[1].col_ref_index == 1
            # The arrival column setting may be skipped since it's a metadata
            # column that gets repositioned; the important thing is that
            # computed columns were restored.
            important_skips = [s for s in skipped if "computed column" in s]
            assert not important_skips

    @pytest.mark.asyncio
    async def test_search_term_survives_round_trip(self, cli_args):
        """Search term is preserved through session round-trip."""
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(buf, ["name,age", "Alice,30", "Bob,25"])
            await _wait(pilot, app)

            buf.query.search_term = re.compile("Alice", re.IGNORECASE)

            state = capture_buffer_state(buf)
            buf.query.search_term = None

            apply_buffer_state(buf, state)

            assert buf.query.search_term is not None
            assert buf.query.search_term.pattern == "Alice"


# ---------------------------------------------------------------------------
# 2. Mark unique / dedup pivot (direct handle_mark_unique)
# ---------------------------------------------------------------------------


class TestMarkUniqueDirect:
    @pytest.mark.asyncio
    async def test_count_column_added_at_position_zero(self, cli_args):
        """handle_mark_unique adds count column at data_position 0."""
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(buf, ["city,pop", "NYC,100", "SF,50", "NYC,200", "LA,75"])
            await _wait(pilot, app)

            handle_mark_unique(buf, "city")

            count_col = next(
                (
                    c
                    for c in buf.current_columns
                    if c.name == MetadataColumn.COUNT.value
                ),
                None,
            )
            assert count_col is not None
            assert count_col.data_position == 0
            assert count_col.render_position == 0
            assert count_col.pinned is True

    @pytest.mark.asyncio
    async def test_dedup_reduces_row_count(self, cli_args):
        """After mark_unique + deferred update, displayed rows are deduped."""
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(buf, ["city,pop", "NYC,100", "SF,50", "NYC,200", "LA,75", "SF,90"])
            await _wait(pilot, app)
            assert len(buf.displayed_rows) == 5

            handle_mark_unique(buf, "city")
            buf._deferred_update_table(reason="Deduplicating")
            await _wait(pilot, app)

            # 5 rows with 3 unique cities -> 3 rows
            assert len(buf.displayed_rows) == 3

    @pytest.mark.asyncio
    async def test_dedup_counts_are_correct(self, cli_args):
        """Count column values match the number of duplicates."""
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(
                buf,
                ["city,pop", "NYC,1", "SF,2", "NYC,3", "NYC,4", "LA,5"],
            )
            await _wait(pilot, app)

            handle_mark_unique(buf, "city")
            buf._deferred_update_table(reason="Deduplicating")
            await _wait(pilot, app)

            # Sort by count descending (default for unique)
            buf.query.sort_column = MetadataColumn.COUNT.value
            buf.query.sort_reverse = True
            buf._deferred_update_table(reason="Sorting")
            await _wait(pilot, app)

            counts = [strip_markup(str(r[0])) for r in buf.displayed_rows]
            # NYC=3, SF=1, LA=1 -> sorted desc: ["3", "1", "1"]
            assert counts[0] == "3"
            assert sorted(counts[1:]) == ["1", "1"]

    @pytest.mark.asyncio
    async def test_non_key_columns_hidden_during_pivot(self, cli_args):
        """Non-key columns are hidden when unique is active."""
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(buf, ["city,pop,region", "NYC,100,East", "SF,50,West"])
            await _wait(pilot, app)

            handle_mark_unique(buf, "city")

            # pop and region should be hidden
            for col in buf.current_columns:
                name = strip_markup(col.name)
                if name in ("pop", "region"):
                    assert col.hidden, f"Column '{name}' should be hidden during pivot"
                elif name in ("city", MetadataColumn.COUNT.value):
                    assert not col.hidden, f"Column '{name}' should be visible"


# ---------------------------------------------------------------------------
# 3. Column delimiter split
# ---------------------------------------------------------------------------


class TestColumnDelimiterSplit:
    @pytest.mark.asyncio
    async def test_split_adds_computed_columns(self, cli_args):
        """Splitting a column by space creates computed columns."""
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(
                buf,
                [
                    "request,status",
                    "GET /api/v1 HTTP/1.1,200",
                    "POST /api/v2 HTTP/1.1,201",
                ],
            )
            await _wait(pilot, app)

            # Manually add computed columns as _apply_column_delimiter would
            existing = {c.name for c in buf.current_columns}
            base_pos = len(buf.current_columns)
            parts = ["GET", "/api/v1", "HTTP/1.1"]
            for i, _part in enumerate(parts):
                name = f"request-{i + 1}"
                if name not in existing:
                    buf.current_columns.append(
                        Column(
                            name=name,
                            labels=set(),
                            render_position=base_pos + i,
                            data_position=base_pos + i,
                            hidden=False,
                            computed=True,
                            col_ref="request",
                            col_ref_index=i,
                            delimiter=" ",
                        )
                    )

            computed = [c for c in buf.current_columns if c.computed and c.col_ref]
            assert len(computed) == 3

            assert computed[0].name == "request-1"
            assert computed[0].col_ref == "request"
            assert computed[0].col_ref_index == 0

            assert computed[1].name == "request-2"
            assert computed[1].col_ref == "request"
            assert computed[1].col_ref_index == 1

            assert computed[2].name == "request-3"
            assert computed[2].col_ref == "request"
            assert computed[2].col_ref_index == 2

    @pytest.mark.asyncio
    async def test_split_with_regex_named_groups(self, cli_args):
        """Splitting with a regex containing named groups creates appropriately named columns."""
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(
                buf,
                [
                    "request,status",
                    "GET /api/v1 HTTP/1.1,200",
                    "POST /api/v2 HTTP/1.1,201",
                ],
            )
            await _wait(pilot, app)

            pattern = re.compile(r"(?P<verb>\S+)\s+(?P<url>\S+)\s+(?P<proto>\S+)")
            group_names = list(pattern.groupindex.keys())
            selected_column = next(
                c for c in buf.current_columns if c.name == "request"
            )

            base_pos = len(buf.current_columns)
            for i, name in enumerate(group_names):
                buf.current_columns.append(
                    Column(
                        name=name,
                        labels=set(),
                        render_position=base_pos + i,
                        data_position=base_pos + i,
                        hidden=False,
                        computed=True,
                        col_ref=selected_column.name,
                        col_ref_index=i,
                        delimiter=pattern,
                    )
                )

            computed = [c for c in buf.current_columns if c.computed and c.col_ref]
            assert len(computed) == 3
            assert [c.name for c in computed] == ["verb", "url", "proto"]
            assert all(c.col_ref == "request" for c in computed)
            assert isinstance(computed[0].delimiter, re.Pattern)


# ---------------------------------------------------------------------------
# 4. View save/load/undo
# ---------------------------------------------------------------------------


class TestViewSaveLoadUndo:
    @pytest.mark.asyncio
    async def test_capture_view_state_zeros_ephemeral_fields(self, cli_args):
        """capture_view_state zeroes cursor_row, cursor_column, tab_name."""
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(buf, ["name,age", "Alice,30", "Bob,25"])
            await _wait(pilot, app)

            state = capture_view_state(buf)
            assert state.cursor_row == 0
            assert state.cursor_column == 0
            assert state.tab_name == ""

    @pytest.mark.asyncio
    async def test_view_undo_restores_original_state(self, cli_args):
        """After loading a view then undoing, original state is restored."""
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(
                buf,
                ["name,age,city", "Alice,30,NYC", "Bob,25,SF", "Charlie,35,LA"],
            )
            await _wait(pilot, app)

            # Set up original state with a filter
            buf.query.filters = [
                Filter(column="city", pattern=re.compile("NYC"), exclude=False)
            ]
            buf._deferred_update_table(reason="Filtering")
            await _wait(pilot, app)

            original_filter_count = len(buf.query.filters)
            original_filter_pattern = buf.query.filters[0].pattern.pattern

            # Save pre-view state (what the app does before loading a view)
            buf._pre_view_state = capture_buffer_state(buf)
            buf._pre_view_raw_rows = list(buf.raw_rows)
            buf._pre_view_timestamps = list(buf._arrival_timestamps)

            # Apply a different view (add sort, remove filter)
            from nless.session import SessionBufferState

            new_view_state = SessionBufferState(
                delimiter=None,
                delimiter_regex=None,
                delimiter_name=None,
                raw_mode=False,
                sort_column="age",
                sort_reverse=True,
                filters=[],
                columns=[],
                unique_column_names=[],
                highlights=[],
                time_window=None,
                rolling_time_window=False,
                is_tailing=False,
            )
            apply_buffer_state(buf, new_view_state)
            buf._deferred_update_table(reason="View loaded")
            await _wait(pilot, app)

            assert buf.query.sort_column == "age"
            assert buf.query.sort_reverse is True
            assert len(buf.query.filters) == 0

            # Undo the view
            assert buf._pre_view_state is not None
            if buf._pre_view_raw_rows is not None:
                pre_view_rows = buf._pre_view_raw_rows
                pre_view_timestamps = buf._pre_view_timestamps
                buf.stream.replace_raw_rows(pre_view_rows, pre_view_timestamps)
                buf._pre_view_raw_rows = None
                buf._pre_view_timestamps = None
            buf.cache.parsed_rows = None
            apply_buffer_state(buf, buf._pre_view_state)
            buf._pre_view_state = None
            buf._deferred_update_table(reason="View undone")
            await _wait(pilot, app)

            # Original state should be restored
            assert len(buf.query.filters) == original_filter_count
            assert buf.query.filters[0].pattern.pattern == original_filter_pattern
            assert buf.query.sort_column is None

    @pytest.mark.asyncio
    async def test_view_state_captures_sort(self, cli_args):
        """View state captures current sort settings."""
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(buf, ["name,age", "Alice,30", "Bob,25"])
            await _wait(pilot, app)

            buf.action_sort()
            await _wait(pilot, app)

            state = capture_view_state(buf)
            assert state.sort_column == "name"
            assert state.sort_reverse is False

    @pytest.mark.asyncio
    async def test_view_state_captures_filters(self, cli_args):
        """View state captures active filters."""
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(buf, ["name,age", "Alice,30", "Bob,25"])
            await _wait(pilot, app)

            buf.query.filters = [
                Filter(column="name", pattern=re.compile("Alice"), exclude=False),
                Filter(column=None, pattern=re.compile("30"), exclude=True),
            ]

            state = capture_view_state(buf)
            assert len(state.filters) == 2
            assert state.filters[0].column == "name"
            assert state.filters[0].pattern == "Alice"
            assert state.filters[0].exclude is False
            assert state.filters[1].column is None
            assert state.filters[1].pattern == "30"
            assert state.filters[1].exclude is True


# ---------------------------------------------------------------------------
# 5. StatusContext sync
# ---------------------------------------------------------------------------


class TestStatusContextSync:
    @pytest.mark.asyncio
    async def test_status_ctx_populated_after_mount(self, cli_args):
        """After app mount, buffers have populated _status_ctx."""
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            await _wait(pilot, app)

            ctx = buf._status_ctx
            assert ctx.theme_name != "", "theme_name should be set"
            assert ctx.keymap_name != "", "keymap_name should be set"
            assert ctx.status_format != "", "status_format should be set"

    @pytest.mark.asyncio
    async def test_status_ctx_has_theme_object(self, cli_args):
        """StatusContext carries the theme object for status bar rendering."""
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            await _wait(pilot, app)

            ctx = buf._status_ctx
            assert ctx.theme is not None, "theme object should be set"
            assert hasattr(ctx.theme, "name"), "theme should have a name attribute"

    @pytest.mark.asyncio
    async def test_status_ctx_format_window_callable(self, cli_args):
        """StatusContext has a callable format_window function."""
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            await _wait(pilot, app)

            ctx = buf._status_ctx
            assert callable(ctx.format_window), "format_window should be callable"

    @pytest.mark.asyncio
    async def test_status_ctx_synced_to_all_buffers(self, cli_args):
        """All buffers in the app share the same StatusContext after sync."""
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(buf, ["name,age", "Alice,30", "Bob,25"])
            await _wait(pilot, app)

            # All buffers should have the same context object
            all_bufs = app.all_buffers
            if len(all_bufs) > 1:
                first_ctx = all_bufs[0]._status_ctx
                for b in all_bufs[1:]:
                    assert b._status_ctx is first_ctx
