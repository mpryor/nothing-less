"""Tests for sorting, filtering, searching, uniqueness, and column operations."""

import re

import pytest

from nless.app import NlessApp
from nless.datatable import Datatable
from nless.types import CliArgs, MetadataColumn


PAUSE = 0.3


@pytest.fixture
def cli_args():
    return CliArgs(delimiter=None, filters=[], unique_keys=set(), sort_by=None)


def _load(buf, lines):
    """Helper: add_logs with a header + data rows."""
    buf.add_logs(lines)


# ---------------------------------------------------------------------------
# Sorting (deferred via _deferred_update_table)
# ---------------------------------------------------------------------------


class TestSort:
    @pytest.mark.asyncio
    async def test_sort_ascending(self, cli_args):
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(buf, ["name,age,city", "Charlie,35,LA", "Alice,30,NYC", "Bob,25,SF"])

            buf.action_sort()
            await pilot.pause(delay=PAUSE)

            assert buf.sort_column == "name"
            assert buf.sort_reverse is False
            assert [r[0] for r in buf.displayed_rows] == ["Alice", "Bob", "Charlie"]

    @pytest.mark.asyncio
    async def test_sort_descending_on_second_press(self, cli_args):
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(buf, ["name,age,city", "Alice,30,NYC", "Charlie,35,LA", "Bob,25,SF"])

            buf.action_sort()
            await pilot.pause(delay=PAUSE)
            buf.action_sort()
            await pilot.pause(delay=PAUSE)

            assert buf.sort_reverse is True
            assert [r[0] for r in buf.displayed_rows] == ["Charlie", "Bob", "Alice"]

    @pytest.mark.asyncio
    async def test_sort_cleared_on_third_press(self, cli_args):
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(buf, ["name,age,city", "Charlie,35,LA", "Alice,30,NYC", "Bob,25,SF"])
            original = [r[0] for r in buf.displayed_rows]

            buf.action_sort()
            await pilot.pause(delay=PAUSE)
            buf.action_sort()
            await pilot.pause(delay=PAUSE)
            buf.action_sort()
            await pilot.pause(delay=PAUSE)

            assert buf.sort_column is None
            assert [r[0] for r in buf.displayed_rows] == original

    @pytest.mark.asyncio
    async def test_sort_numeric_column(self, cli_args):
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(buf, ["name,age", "Alice,30", "Bob,5", "Charlie,100"])

            from nless.datatable import Datatable

            dt = buf.query_one(Datatable)
            dt.move_cursor(column=1)
            await pilot.pause(delay=PAUSE)

            buf.action_sort()
            await pilot.pause(delay=PAUSE)

            assert buf.sort_column == "age"
            assert [r[1] for r in buf.displayed_rows] == ["5", "30", "100"]

    @pytest.mark.asyncio
    async def test_sort_loading_cleared(self, cli_args):
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(buf, ["name,age", "B,2", "A,1"])

            buf.action_sort()
            await pilot.pause(delay=PAUSE)

            assert not buf._is_loading


# ---------------------------------------------------------------------------
# Filtering (async via _copy_buffer_async)
# ---------------------------------------------------------------------------


class TestFilter:
    @pytest.mark.asyncio
    async def test_filter_by_column(self, cli_args):
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(
                buf,
                ["name,age,city", "Alice,30,NYC", "Bob,25,SF", "Charlie,35,NYC"],
            )

            app._perform_filter("NYC", "city")
            await pilot.pause(delay=PAUSE)

            assert len(app.buffers) == 2
            new_buf = app.buffers[1]
            assert len(new_buf.displayed_rows) == 2
            assert all(r[2] == "NYC" for r in new_buf.displayed_rows)

    @pytest.mark.asyncio
    async def test_filter_any_column(self, cli_args):
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(buf, ["name,age,city", "Alice,30,NYC", "Bob,25,SF"])

            app._perform_filter("Alice")
            await pilot.pause(delay=PAUSE)

            assert len(app.buffers) == 2
            assert len(app.buffers[1].displayed_rows) == 1

    @pytest.mark.asyncio
    async def test_filter_regex(self, cli_args):
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(
                buf,
                [
                    "name,age,city",
                    "Alice,30,NYC",
                    "Bob,25,SF",
                    "Anna,28,LA",
                ],
            )

            app._perform_filter("^A", "name")
            await pilot.pause(delay=PAUSE)

            new_buf = app.buffers[1]
            assert len(new_buf.displayed_rows) == 2
            names = {r[0] for r in new_buf.displayed_rows}
            assert names == {"Alice", "Anna"}

    @pytest.mark.asyncio
    async def test_filter_no_match(self, cli_args):
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(buf, ["name,age", "Alice,30", "Bob,25"])

            app._perform_filter("ZZZZZ", "name")
            await pilot.pause(delay=PAUSE)

            assert len(app.buffers) == 2
            assert len(app.buffers[1].displayed_rows) == 0

    @pytest.mark.asyncio
    async def test_clear_filters(self, cli_args):
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(buf, ["name,age", "Alice,30", "Bob,25", "Charlie,35"])

            # Apply a filter
            app._perform_filter("Alice", "name")
            await pilot.pause(delay=PAUSE)
            assert len(app.buffers[1].displayed_rows) == 1

            # Clear filters on the filtered buffer
            app.curr_buffer_idx = 1
            app._perform_filter(None)
            await pilot.pause(delay=PAUSE)

            assert len(app.buffers) == 3
            # Cleared buffer should have all rows that survived the first copy
            assert len(app.buffers[2].current_filters) == 0

    @pytest.mark.asyncio
    async def test_filter_preserves_original(self, cli_args):
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(buf, ["name,age", "Alice,30", "Bob,25", "Charlie,35"])

            app._perform_filter("Alice", "name")
            await pilot.pause(delay=PAUSE)

            # Original buffer should be untouched
            assert len(buf.displayed_rows) == 3
            assert len(buf.current_filters) == 0

    @pytest.mark.asyncio
    async def test_filter_loading_cleared(self, cli_args):
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(buf, ["name,age", "Alice,30", "Bob,25"])

            app._perform_filter("Alice", "name")
            await pilot.pause(delay=PAUSE)

            assert not buf._is_loading


# ---------------------------------------------------------------------------
# Search (deferred via _deferred_update_table)
# ---------------------------------------------------------------------------


class TestSearch:
    @pytest.mark.asyncio
    async def test_search_highlights_matches(self, cli_args):
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(buf, ["name,age,city", "Alice,30,NYC", "Bob,25,SF", "Anna,28,NYC"])

            buf._perform_search("NYC")
            await pilot.pause(delay=PAUSE)

            assert buf.search_term is not None
            assert (
                len(buf.search_matches) == 2
            )  # NYC appears in city column of rows 0 and 2

    @pytest.mark.asyncio
    async def test_search_sets_match_index(self, cli_args):
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(buf, ["name,age", "Alice,30", "Bob,25"])

            buf._perform_search("Alice")
            await pilot.pause(delay=PAUSE)

            assert buf.current_match_index >= 0

    @pytest.mark.asyncio
    async def test_clear_search(self, cli_args):
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(buf, ["name,age", "Alice,30", "Bob,25"])

            buf._perform_search("Alice")
            await pilot.pause(delay=PAUSE)
            assert buf.search_term is not None

            buf._perform_search(None)
            await pilot.pause(delay=PAUSE)
            assert buf.search_term is None
            assert len(buf.search_matches) == 0

    @pytest.mark.asyncio
    async def test_search_to_filter(self, cli_args):
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(buf, ["name,age,city", "Alice,30,NYC", "Bob,25,SF", "Charlie,35,NYC"])

            buf._perform_search("NYC")
            await pilot.pause(delay=PAUSE)

            app.action_search_to_filter()
            await pilot.pause(delay=PAUSE)

            assert len(app.buffers) == 2
            new_buf = app.buffers[1]
            assert len(new_buf.current_filters) == 1
            assert new_buf.current_filters[0].column is None  # "any" filter

    @pytest.mark.asyncio
    async def test_search_to_filter_no_search(self, cli_args):
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(buf, ["name,age", "Alice,30"])

            app.action_search_to_filter()
            await pilot.pause(delay=PAUSE)

            # Should not create a new buffer
            assert len(app.buffers) == 1


# ---------------------------------------------------------------------------
# Mark Unique / Composite Key (async via _copy_buffer_async)
# ---------------------------------------------------------------------------


class TestMarkUnique:
    @pytest.mark.asyncio
    async def test_mark_unique_creates_buffer(self, cli_args):
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(
                buf,
                [
                    "name,city",
                    "Alice,NYC",
                    "Bob,SF",
                    "Charlie,NYC",
                    "Dave,SF",
                ],
            )

            app.action_mark_unique()
            await pilot.pause(delay=PAUSE)

            assert len(app.buffers) == 2
            new_buf = app.buffers[1]
            assert "name" in new_buf.unique_column_names

    @pytest.mark.asyncio
    async def test_mark_unique_adds_count_column(self, cli_args):
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(
                buf,
                ["city,pop", "NYC,100", "SF,50", "NYC,200", "LA,75"],
            )

            app.action_mark_unique()
            await pilot.pause(delay=PAUSE)

            new_buf = app.buffers[1]
            col_names = [c.name for c in new_buf.current_columns]
            assert MetadataColumn.COUNT.value in col_names

    @pytest.mark.asyncio
    async def test_mark_unique_deduplicates(self, cli_args):
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(
                buf,
                ["city,pop", "NYC,100", "SF,50", "NYC,200"],
            )

            app.action_mark_unique()
            await pilot.pause(delay=PAUSE)

            new_buf = app.buffers[1]
            # 3 rows with column "city" marked unique -> 2 unique cities
            assert len(new_buf.displayed_rows) == 2

    @pytest.mark.asyncio
    async def test_mark_unique_sorts_by_count_desc(self, cli_args):
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(
                buf,
                ["city,pop", "NYC,1", "SF,2", "NYC,3", "NYC,4"],
            )

            app.action_mark_unique()
            await pilot.pause(delay=PAUSE)

            new_buf = app.buffers[1]
            assert new_buf.sort_column == MetadataColumn.COUNT.value
            assert new_buf.sort_reverse is True

    @pytest.mark.asyncio
    async def test_mark_unique_loading_cleared(self, cli_args):
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(buf, ["name,age", "A,1", "B,2", "A,3"])

            app.action_mark_unique()
            await pilot.pause(delay=PAUSE)

            assert not buf._is_loading

    @pytest.mark.asyncio
    async def test_mark_unique_streaming_updates_counts(self, cli_args):
        """After creating a composite key buffer, streaming duplicate data should update counts."""
        from nless.input import LineStream

        stream = LineStream()
        app = NlessApp(cli_args=cli_args, starting_stream=stream)
        async with app.run_test(size=(120, 40)) as pilot:
            await pilot.pause(delay=PAUSE)

            # Initial data via stream
            stream.notify(["city,pop", "NYC,100", "SF,50", "NYC,200"])
            await pilot.pause(delay=PAUSE)

            buf = app.buffers[0]
            assert len(buf.displayed_rows) == 3

            # Create composite key on "city"
            app.action_mark_unique()
            await pilot.pause(delay=PAUSE)

            new_buf = app.buffers[1]
            assert "city" in new_buf.unique_column_names

            # Wait for initial deferred update to complete
            for _ in range(10):
                await pilot.pause(delay=PAUSE)
                if not new_buf._is_loading:
                    break
            assert not new_buf._is_loading, "Initial deferred update never completed"
            assert len(new_buf.displayed_rows) == 2  # NYC and SF

            # Get initial count for NYC (should be 2)
            initial_rows = {
                new_buf._get_cell_value_without_markup(
                    r[1]
                ): new_buf._get_cell_value_without_markup(r[0])
                for r in new_buf.displayed_rows
            }
            assert initial_rows["NYC"] == "2"

            # Stream one duplicate at a time to isolate failures
            stream.notify(["NYC,300"])
            for _ in range(10):
                await pilot.pause(delay=PAUSE)
                if not new_buf._is_loading:
                    break
            rows_after_1 = {
                new_buf._get_cell_value_without_markup(
                    r[1]
                ): new_buf._get_cell_value_without_markup(r[0])
                for r in new_buf.displayed_rows
            }
            assert rows_after_1["NYC"] == "3", (
                f"After 1st stream: NYC should be 3, got {rows_after_1['NYC']}"
            )

            stream.notify(["NYC,400"])
            for _ in range(10):
                await pilot.pause(delay=PAUSE)
                if not new_buf._is_loading:
                    break
            rows_after_2 = {
                new_buf._get_cell_value_without_markup(
                    r[1]
                ): new_buf._get_cell_value_without_markup(r[0])
                for r in new_buf.displayed_rows
            }
            assert rows_after_2["NYC"] == "4", (
                f"After 2nd stream: NYC should be 4, got {rows_after_2['NYC']}"
            )

            stream.notify(["SF,60"])
            for _ in range(10):
                await pilot.pause(delay=PAUSE)
                if not new_buf._is_loading:
                    break
            rows_after_3 = {
                new_buf._get_cell_value_without_markup(
                    r[1]
                ): new_buf._get_cell_value_without_markup(r[0])
                for r in new_buf.displayed_rows
            }
            assert rows_after_3["SF"] == "2", (
                f"After 3rd stream: SF should be 2, got {rows_after_3['SF']}"
            )


# ---------------------------------------------------------------------------
# Move Column (deferred via _deferred_update_table)
# ---------------------------------------------------------------------------


class TestMoveColumn:
    @pytest.mark.asyncio
    async def test_move_column_right(self, cli_args):
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(buf, ["a,b,c", "1,2,3"])
            positions_before = {c.name: c.render_position for c in buf.current_columns}
            assert positions_before == {"a": 0, "b": 1, "c": 2}

            buf.action_move_column_right()
            await pilot.pause(delay=PAUSE)

            positions_after = {c.name: c.render_position for c in buf.current_columns}
            assert positions_after["a"] == 1
            assert positions_after["b"] == 0

    @pytest.mark.asyncio
    async def test_move_column_left(self, cli_args):
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(buf, ["a,b,c", "1,2,3"])

            from nless.datatable import Datatable

            dt = buf.query_one(Datatable)
            dt.move_cursor(column=2)
            await pilot.pause(delay=PAUSE)

            buf.action_move_column_left()
            await pilot.pause(delay=PAUSE)

            positions = {c.name: c.render_position for c in buf.current_columns}
            assert positions["c"] == 1
            assert positions["b"] == 2

    @pytest.mark.asyncio
    async def test_move_column_at_boundary_noop(self, cli_args):
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(buf, ["a,b,c", "1,2,3"])

            # Cursor on col 0, move left should be a no-op
            buf.action_move_column_left()
            await pilot.pause(delay=PAUSE)

            positions = {c.name: c.render_position for c in buf.current_columns}
            assert positions == {"a": 0, "b": 1, "c": 2}


# ---------------------------------------------------------------------------
# Deferred update generation counter
# ---------------------------------------------------------------------------


class TestDeferredGeneration:
    @pytest.mark.asyncio
    async def test_rapid_sorts_only_last_applies(self, cli_args):
        """Rapid sort toggles should only execute the final state."""
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(buf, ["name,age", "Charlie,3", "Alice,1", "Bob,2"])

            # Fire three sorts without waiting — only the last should apply
            buf.action_sort()
            buf.action_sort()
            buf.action_sort()
            await pilot.pause(delay=PAUSE)

            # Three presses: asc → desc → clear
            assert buf.sort_column is None
            assert not buf._is_loading


# ---------------------------------------------------------------------------
# Delimiter Change (handle_delimiter_submitted)
# ---------------------------------------------------------------------------


async def _submit_prompt(app, pilot, action_name, input_id, value):
    """Mount a prompt via action, set value, and submit via Enter."""
    getattr(app, action_name)()
    await pilot.pause(delay=PAUSE)
    inp = app.query_one(f"#{input_id}")
    inp.value = value
    await pilot.press("enter")
    await pilot.pause(delay=PAUSE)


class TestDelimiterChange:
    @pytest.mark.asyncio
    async def test_change_delimiter_csv_to_tsv(self, cli_args):
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(buf, ["a\tb\tc", "1\t2\t3", "4\t5\t6"])
            await pilot.pause(delay=PAUSE)

            # Inferred as tab — change to comma
            assert buf.delimiter == "\t"
            initial_col_names = [c.name for c in buf.current_columns]
            assert len(initial_col_names) == 3

            await _submit_prompt(app, pilot, "action_delimiter", "delimiter_input", ",")
            await pilot.pause(delay=PAUSE)

            # Now delimiter is comma, header re-parsed as single column "a\tb\tc"
            assert buf.delimiter == ","
            col_names = [c.name for c in buf.current_columns]
            assert col_names != initial_col_names

    @pytest.mark.asyncio
    async def test_change_delimiter_clears_state(self, cli_args):
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(
                buf,
                ["name,age,city", "Alice,30,NYC", "Bob,25,SF", "Charlie,35,NYC"],
            )

            # Apply sort, search, and filter state
            buf.action_sort()
            await pilot.pause(delay=PAUSE)
            buf._perform_search("Alice")
            await pilot.pause(delay=PAUSE)
            assert buf.sort_column is not None
            assert buf.search_term is not None

            # Change delimiter — should clear state
            await _submit_prompt(app, pilot, "action_delimiter", "delimiter_input", ",")
            await pilot.pause(delay=PAUSE)

            assert buf.sort_column is None
            assert buf.search_term is None
            assert buf.current_filters == []
            assert buf.unique_column_names == set()

    @pytest.mark.asyncio
    async def test_change_delimiter_to_raw(self, cli_args):
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(buf, ["name,age", "Alice,30", "Bob,25"])
            await pilot.pause(delay=PAUSE)

            assert len(buf.current_columns) == 2

            await _submit_prompt(
                app, pilot, "action_delimiter", "delimiter_input", "raw"
            )
            await pilot.pause(delay=PAUSE)

            assert buf.delimiter == "raw"
            col_names = [c.name for c in buf.current_columns]
            assert col_names == ["log"]

    @pytest.mark.asyncio
    async def test_change_delimiter_to_json(self):
        raw_args = CliArgs(delimiter="raw", filters=[], unique_keys=set(), sort_by=None)
        app = NlessApp(cli_args=raw_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(
                buf,
                [
                    '{"name":"Alice","age":30}',
                    '{"name":"Bob","age":25}',
                ],
            )
            await pilot.pause(delay=PAUSE)
            assert buf.delimiter == "raw"

            await _submit_prompt(
                app, pilot, "action_delimiter", "delimiter_input", "json"
            )
            await pilot.pause(delay=PAUSE)

            assert buf.delimiter == "json"
            col_names = [c.name for c in buf.current_columns]
            assert "name" in col_names
            assert "age" in col_names


# ---------------------------------------------------------------------------
# Column Filter (handle_column_filter_submitted)
# ---------------------------------------------------------------------------


class TestColumnFilter:
    @pytest.mark.asyncio
    async def test_filter_columns_by_name(self, cli_args):
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(buf, ["name,age,city,zip", "Alice,30,NYC,10001"])
            await pilot.pause(delay=PAUSE)

            await _submit_prompt(
                app, pilot, "action_filter_columns", "column_filter_input", "age"
            )
            await pilot.pause(delay=PAUSE)

            visible = [c for c in buf.current_columns if not c.hidden]
            assert len(visible) == 1
            assert visible[0].name == "age"

    @pytest.mark.asyncio
    async def test_filter_columns_all_restores(self, cli_args):
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(buf, ["name,age,city", "Alice,30,NYC"])
            await pilot.pause(delay=PAUSE)

            # Hide some columns
            await _submit_prompt(
                app, pilot, "action_filter_columns", "column_filter_input", "age"
            )
            await pilot.pause(delay=PAUSE)
            assert sum(1 for c in buf.current_columns if c.hidden) > 0

            # Restore all
            await _submit_prompt(
                app, pilot, "action_filter_columns", "column_filter_input", "all"
            )
            await pilot.pause(delay=PAUSE)

            assert all(not c.hidden for c in buf.current_columns)

    @pytest.mark.asyncio
    async def test_filter_columns_preserves_metadata(self, cli_args):
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(
                buf,
                ["city,pop", "NYC,100", "SF,50", "NYC,200"],
            )

            # Create composite key to get count column
            app.action_mark_unique()
            await pilot.pause(delay=PAUSE)
            new_buf = app.buffers[1]
            app.curr_buffer_idx = 1

            # Filter columns — count should stay visible
            await _submit_prompt(
                app, pilot, "action_filter_columns", "column_filter_input", "city"
            )
            await pilot.pause(delay=PAUSE)

            visible_names = [c.name for c in new_buf.current_columns if not c.hidden]
            assert MetadataColumn.COUNT.value in visible_names
            assert "city" in visible_names

    @pytest.mark.asyncio
    async def test_filter_columns_multiple_patterns(self, cli_args):
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(buf, ["name,age,city,zip", "Alice,30,NYC,10001"])
            await pilot.pause(delay=PAUSE)

            await _submit_prompt(
                app,
                pilot,
                "action_filter_columns",
                "column_filter_input",
                "name|city",
            )
            await pilot.pause(delay=PAUSE)

            visible = [c for c in buf.current_columns if not c.hidden]
            visible_names = {c.name for c in visible}
            assert "name" in visible_names
            assert "city" in visible_names
            assert "age" not in visible_names
            assert "zip" not in visible_names


# ---------------------------------------------------------------------------
# Column Delimiter (handle_column_delimiter_submitted)
# ---------------------------------------------------------------------------


class TestColumnDelimiter:
    @pytest.mark.asyncio
    async def test_column_delimiter_json(self, cli_args):
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(
                buf,
                [
                    "name\tdata",
                    'Alice\t{"key1":"val1","key2":"val2"}',
                    'Bob\t{"key1":"val3","key2":"val4"}',
                ],
            )
            await pilot.pause(delay=PAUSE)

            # Move cursor to the JSON column (column 1)
            dt = buf.query_one(Datatable)
            dt.move_cursor(column=1)
            await pilot.pause(delay=PAUSE)

            initial_col_count = len(buf.current_columns)

            await _submit_prompt(
                app,
                pilot,
                "action_column_delimiter",
                "column_delimiter_input",
                "json",
            )
            await pilot.pause(delay=PAUSE)

            assert len(buf.current_columns) > initial_col_count
            new_col_names = [c.name for c in buf.current_columns]
            assert "data.key1" in new_col_names
            assert "data.key2" in new_col_names
            # New columns should be computed
            for col in buf.current_columns:
                if col.name.startswith("data."):
                    assert col.computed is True
                    assert col.json_ref == col.name

    @pytest.mark.asyncio
    async def test_column_delimiter_split(self, cli_args):
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(
                buf,
                [
                    "id\tpath",
                    "1\ta-b-c",
                    "2\td-e-f",
                ],
            )
            await pilot.pause(delay=PAUSE)

            # Move cursor to the "path" column
            dt = buf.query_one(Datatable)
            dt.move_cursor(column=1)
            await pilot.pause(delay=PAUSE)

            initial_col_count = len(buf.current_columns)

            await _submit_prompt(
                app,
                pilot,
                "action_column_delimiter",
                "column_delimiter_input",
                "-",
            )
            await pilot.pause(delay=PAUSE)

            assert len(buf.current_columns) > initial_col_count
            new_col_names = [c.name for c in buf.current_columns]
            # "a-b-c" splits into 3 parts → columns "path-1", "path-2", "path-3"
            assert "path-1" in new_col_names
            assert "path-2" in new_col_names
            assert "path-3" in new_col_names


# ---------------------------------------------------------------------------
# JSON Header (on_select_changed with json_header_select)
# ---------------------------------------------------------------------------


class TestJsonHeader:
    @pytest.mark.asyncio
    async def test_json_header_adds_column(self, cli_args):
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(
                buf,
                [
                    "id\tpayload",
                    '1\t{"color":"red","size":10}',
                    '2\t{"color":"blue","size":20}',
                ],
            )
            await pilot.pause(delay=PAUSE)

            # Move cursor to JSON column
            dt = buf.query_one(Datatable)
            dt.move_cursor(column=1)
            await pilot.pause(delay=PAUSE)

            initial_col_count = len(buf.current_columns)

            # action_json_header mounts NlessSelect with JSON keys
            app.action_json_header()
            await pilot.pause(delay=PAUSE)

            # Press Enter to select the first key ("color")
            await pilot.press("enter")
            await pilot.pause(delay=PAUSE)

            assert len(buf.current_columns) == initial_col_count + 1
            new_col = buf.current_columns[-1]
            assert new_col.computed is True
            assert "color" in new_col.json_ref
            assert new_col.delimiter == "json"

    @pytest.mark.asyncio
    async def test_json_header_column_values(self, cli_args):
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(
                buf,
                [
                    "id\tpayload",
                    '1\t{"color":"red","size":10}',
                    '2\t{"color":"blue","size":20}',
                ],
            )
            await pilot.pause(delay=PAUSE)

            dt = buf.query_one(Datatable)
            dt.move_cursor(column=1)
            await pilot.pause(delay=PAUSE)

            app.action_json_header()
            await pilot.pause(delay=PAUSE)

            # Select first key
            await pilot.press("enter")
            await pilot.pause(delay=PAUSE)

            # After deferred update, the new column should have extracted values
            new_col = buf.current_columns[-1]
            new_col_idx = new_col.render_position
            values = [
                buf._get_cell_value_without_markup(r[new_col_idx])
                for r in buf.displayed_rows
            ]
            assert "red" in values
            assert "blue" in values


# ---------------------------------------------------------------------------
# Filter Cursor Word (action_filter_cursor_word)
# ---------------------------------------------------------------------------


class TestFilterCursorWord:
    @pytest.mark.asyncio
    async def test_filter_cursor_word_creates_buffer(self, cli_args):
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(
                buf,
                ["name,age,city", "Alice,30,NYC", "Bob,25,SF", "Charlie,35,NYC"],
            )
            await pilot.pause(delay=PAUSE)

            # Cursor at (0, 0) → cell "Alice"
            app.action_filter_cursor_word()
            await pilot.pause(delay=PAUSE)

            assert len(app.buffers) == 2
            new_buf = app.buffers[1]
            assert len(new_buf.current_filters) == 1
            assert new_buf.current_filters[0].pattern.pattern == "^Alice$"
            assert new_buf.current_filters[0].column == "name"

    @pytest.mark.asyncio
    async def test_filter_cursor_word_escapes_regex(self, cli_args):
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(
                buf,
                ["name,val", "foo.bar,1", "baz,2"],
            )
            await pilot.pause(delay=PAUSE)

            # Cursor at (0, 0) → cell "foo.bar" — dot should be escaped
            app.action_filter_cursor_word()
            await pilot.pause(delay=PAUSE)

            assert len(app.buffers) == 2
            new_buf = app.buffers[1]
            escaped = re.escape("foo.bar")
            assert new_buf.current_filters[0].pattern.pattern == f"^{escaped}$"


# ---------------------------------------------------------------------------
# Filter Composite Key (_filter_composite_key)
# ---------------------------------------------------------------------------


class TestFilterCompositeKey:
    @pytest.mark.asyncio
    async def test_filter_composite_key(self, cli_args):
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(
                buf,
                [
                    "city,pop",
                    "NYC,100",
                    "SF,50",
                    "NYC,200",
                    "LA,75",
                ],
            )
            await pilot.pause(delay=PAUSE)

            # Mark unique on "city"
            app.action_mark_unique()
            await pilot.pause(delay=PAUSE)

            assert len(app.buffers) == 2
            unique_buf = app.buffers[1]
            app.curr_buffer_idx = 1

            # Move cursor to the "city" column (the unique column)
            dt = unique_buf.query_one(Datatable)
            city_col_idx = unique_buf._get_col_idx_by_name("city", render_position=True)
            dt.move_cursor(column=city_col_idx, row=0)
            await pilot.pause(delay=PAUSE)

            # Press Enter to trigger _filter_composite_key
            await pilot.press("enter")
            await pilot.pause(delay=PAUSE)

            assert len(app.buffers) == 3
            filtered_buf = app.buffers[2]
            assert len(filtered_buf.current_filters) >= 1
            # The filter should match the city from the cursor row
            filter_columns = [f.column for f in filtered_buf.current_filters]
            assert "city" in filter_columns


# ---------------------------------------------------------------------------
# Close Buffer (action_close_active_buffer)
# ---------------------------------------------------------------------------


class TestCloseBuffer:
    @pytest.mark.asyncio
    async def test_close_buffer_removes_it(self, cli_args):
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(buf, ["name,age", "Alice,30", "Bob,25"])

            # Create a second buffer via filter
            app._perform_filter("Alice", "name")
            await pilot.pause(delay=PAUSE)
            assert len(app.buffers) == 2

            # Close the active buffer (buffer 1)
            app.action_close_active_buffer()
            await pilot.pause(delay=PAUSE)

            assert len(app.buffers) == 1

    @pytest.mark.asyncio
    async def test_close_buffer_adjusts_index(self, cli_args):
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(buf, ["name,age", "Alice,30", "Bob,25", "Charlie,35"])

            # Create two additional buffers
            app._perform_filter("Alice", "name")
            await pilot.pause(delay=PAUSE)
            app._perform_filter("Bob", "name")
            await pilot.pause(delay=PAUSE)
            assert len(app.buffers) == 3

            # Active is the last buffer (index 2)
            assert app.curr_buffer_idx == 2

            # Close last buffer
            app.action_close_active_buffer()
            await pilot.pause(delay=PAUSE)

            assert len(app.buffers) == 2
            assert app.curr_buffer_idx <= len(app.buffers) - 1


# ---------------------------------------------------------------------------
# Search Navigation (action_next_search / action_previous_search)
# ---------------------------------------------------------------------------


class TestSearchNavigation:
    @pytest.mark.asyncio
    async def test_next_search_advances(self, cli_args):
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(
                buf,
                ["name,age,city", "Alice,30,NYC", "Bob,25,NYC", "Charlie,35,LA"],
            )

            buf._perform_search("NYC")
            await pilot.pause(delay=PAUSE)

            assert len(buf.search_matches) == 2
            first_index = buf.current_match_index

            buf.action_next_search()
            await pilot.pause(delay=PAUSE)

            assert buf.current_match_index == (first_index + 1) % len(
                buf.search_matches
            )

    @pytest.mark.asyncio
    async def test_previous_search_wraps(self, cli_args):
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(
                buf,
                ["name,age,city", "Alice,30,NYC", "Bob,25,NYC", "Charlie,35,LA"],
            )

            buf._perform_search("NYC")
            await pilot.pause(delay=PAUSE)

            assert len(buf.search_matches) == 2

            # Navigate to first match (index 0)
            buf.current_match_index = 0

            # Previous should wrap to last match
            buf.action_previous_search()

            assert buf.current_match_index == len(buf.search_matches) - 1

    @pytest.mark.asyncio
    async def test_search_cursor_word(self, cli_args):
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(
                buf,
                ["name,age,city", "Alice,30,NYC", "Bob,25,SF", "Charlie,35,NYC"],
            )
            await pilot.pause(delay=PAUSE)

            # Cursor at (0, 0) → "Alice"
            buf.action_search_cursor_word()
            await pilot.pause(delay=PAUSE)

            assert buf.search_term is not None
            assert buf.search_term.pattern == re.escape("Alice")
            assert len(buf.search_matches) == 1
