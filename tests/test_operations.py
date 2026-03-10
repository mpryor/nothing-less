"""Tests for sorting, filtering, searching, uniqueness, and column operations."""

import re

import pytest

from nless.app import NlessApp
from nless.dataprocessing import strip_markup
from nless.datatable import Datatable
from nless.types import CliArgs, MetadataColumn


async def _wait(pilot, app):
    """Pump the event loop until all buffers finish loading."""
    settled = 0
    for _ in range(300):  # 3s max
        await pilot.pause(delay=0.01)
        if all(not b._loading_reason for b in app.buffers):
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
            await _wait(pilot, app)

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
            await _wait(pilot, app)
            buf.action_sort()
            await _wait(pilot, app)

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
            await _wait(pilot, app)
            buf.action_sort()
            await _wait(pilot, app)
            buf.action_sort()
            await _wait(pilot, app)

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
            await _wait(pilot, app)

            buf.action_sort()
            await _wait(pilot, app)

            assert buf.sort_column == "age"
            assert [r[1] for r in buf.displayed_rows] == ["5", "30", "100"]

    @pytest.mark.asyncio
    async def test_sort_loading_cleared(self, cli_args):
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(buf, ["name,age", "B,2", "A,1"])

            buf.action_sort()
            await _wait(pilot, app)

            assert not buf._loading_reason


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
            await _wait(pilot, app)

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
            await _wait(pilot, app)

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
            await _wait(pilot, app)

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
            await _wait(pilot, app)

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
            await _wait(pilot, app)
            assert len(app.buffers[1].displayed_rows) == 1

            # Clear filters on the filtered buffer
            app.curr_buffer_idx = 1
            app._perform_filter(None)
            await _wait(pilot, app)

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
            await _wait(pilot, app)

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
            await _wait(pilot, app)

            assert not buf._loading_reason
            # Verify the filter actually created a new buffer with correct rows
            assert len(app.buffers) == 2
            filtered_buf = app.buffers[1]
            plain_rows = [
                [strip_markup(c) for c in r] for r in filtered_buf.displayed_rows
            ]
            assert all(r[0] == "Alice" for r in plain_rows)


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
            await _wait(pilot, app)

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
            await _wait(pilot, app)

            assert buf.current_match_index >= 0

    @pytest.mark.asyncio
    async def test_clear_search(self, cli_args):
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(buf, ["name,age", "Alice,30", "Bob,25"])

            buf._perform_search("Alice")
            await _wait(pilot, app)
            assert buf.search_term is not None

            buf._perform_search(None)
            await _wait(pilot, app)
            assert buf.search_term is None
            assert len(buf.search_matches) == 0

    @pytest.mark.asyncio
    async def test_search_to_filter(self, cli_args):
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(buf, ["name,age,city", "Alice,30,NYC", "Bob,25,SF", "Charlie,35,NYC"])

            buf._perform_search("NYC")
            await _wait(pilot, app)

            app.action_search_to_filter()
            await _wait(pilot, app)

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
            await _wait(pilot, app)

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
            await _wait(pilot, app)

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
            await _wait(pilot, app)

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
            await _wait(pilot, app)

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
            await _wait(pilot, app)

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
            await _wait(pilot, app)

            assert not buf._loading_reason

    @pytest.mark.asyncio
    async def test_mark_unique_streaming_updates_counts(self, cli_args):
        """After creating a composite key buffer, streaming duplicate data should update counts."""
        from nless.input import LineStream

        stream = LineStream()
        app = NlessApp(cli_args=cli_args, starting_stream=stream)
        async with app.run_test(size=(120, 40)) as pilot:
            await _wait(pilot, app)

            # Initial data via stream
            stream.notify(["city,pop", "NYC,100", "SF,50", "NYC,200"])
            await _wait(pilot, app)

            buf = app.buffers[0]
            assert len(buf.displayed_rows) == 3

            # Create composite key on "city"
            app.action_mark_unique()
            await _wait(pilot, app)

            new_buf = app.buffers[1]
            assert "city" in new_buf.unique_column_names

            # Wait for initial deferred update to complete
            await _wait(pilot, app)
            assert len(new_buf.displayed_rows) == 2  # NYC and SF

            # Get initial count for NYC (should be 2)
            initial_rows = {
                strip_markup(r[1]): strip_markup(r[0]) for r in new_buf.displayed_rows
            }
            assert initial_rows["NYC"] == "2"

            # Stream one duplicate at a time to isolate failures
            stream.notify(["NYC,300"])
            await _wait(pilot, app)
            rows_after_1 = {
                strip_markup(r[1]): strip_markup(r[0]) for r in new_buf.displayed_rows
            }
            assert rows_after_1["NYC"] == "3", (
                f"After 1st stream: NYC should be 3, got {rows_after_1['NYC']}"
            )

            stream.notify(["NYC,400"])
            await _wait(pilot, app)
            rows_after_2 = {
                strip_markup(r[1]): strip_markup(r[0]) for r in new_buf.displayed_rows
            }
            assert rows_after_2["NYC"] == "4", (
                f"After 2nd stream: NYC should be 4, got {rows_after_2['NYC']}"
            )

            stream.notify(["SF,60"])
            await _wait(pilot, app)
            rows_after_3 = {
                strip_markup(r[1]): strip_markup(r[0]) for r in new_buf.displayed_rows
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
            positions_before = {
                c.name: c.render_position for c in buf.current_columns if not c.hidden
            }
            assert positions_before == {"a": 0, "b": 1, "c": 2}

            buf.action_move_column_right()
            await _wait(pilot, app)

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
            await _wait(pilot, app)

            buf.action_move_column_left()
            await _wait(pilot, app)

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
            await _wait(pilot, app)

            positions = {
                c.name: c.render_position for c in buf.current_columns if not c.hidden
            }
            assert positions == {"a": 0, "b": 1, "c": 2}


# ---------------------------------------------------------------------------
# Pin / unpin column
# ---------------------------------------------------------------------------


class TestPinColumn:
    @pytest.mark.asyncio
    async def test_pin_column(self, cli_args):
        """Pinning a column should move it to the left and set fixed_columns."""
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(buf, ["a,b,c", "1,2,3", "4,5,6"])
            await _wait(pilot, app)

            # Move cursor to column "b" (render_position=1)
            dt = buf.query_one(Datatable)
            dt.move_cursor(column=1)
            await _wait(pilot, app)

            buf.action_pin_column()
            await _wait(pilot, app)

            col_b = next(c for c in buf.current_columns if c.name == "b")
            assert col_b.pinned is True
            assert "P" in col_b.labels
            # Pinned column should be at position 0
            assert col_b.render_position == 0
            # "a" should have shifted right to position 1
            col_a = next(c for c in buf.current_columns if c.name == "a")
            assert col_a.render_position == 1
            # "c" stays at position 2
            col_c = next(c for c in buf.current_columns if c.name == "c")
            assert col_c.render_position == 2
            # Datatable should reflect 1 fixed column
            assert dt.fixed_columns == 1

    @pytest.mark.asyncio
    async def test_unpin_column(self, cli_args):
        """Unpinning should move the column back after pinned columns."""
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(buf, ["a,b,c", "1,2,3", "4,5,6"])
            await _wait(pilot, app)

            # Pin column "a"
            dt = buf.query_one(Datatable)
            dt.move_cursor(column=0)
            await _wait(pilot, app)
            buf.action_pin_column()
            await _wait(pilot, app)

            col_a = next(c for c in buf.current_columns if c.name == "a")
            assert col_a.pinned is True

            # Now unpin it
            dt.move_cursor(column=0)
            await _wait(pilot, app)
            buf.action_pin_column()
            await _wait(pilot, app)

            assert col_a.pinned is False
            assert "P" not in col_a.labels
            assert dt.fixed_columns == 0

    @pytest.mark.asyncio
    async def test_pin_multiple_columns(self, cli_args):
        """Pinning multiple columns should stack them on the left."""
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(buf, ["a,b,c,d", "1,2,3,4"])
            await _wait(pilot, app)

            dt = buf.query_one(Datatable)

            # Pin "a" (at position 0)
            dt.move_cursor(column=0)
            await _wait(pilot, app)
            buf.action_pin_column()
            await _wait(pilot, app)

            # Pin "c" (now at position 2 since a shifted to 0)
            dt.move_cursor(column=2)
            await _wait(pilot, app)
            buf.action_pin_column()
            await _wait(pilot, app)

            col_a = next(c for c in buf.current_columns if c.name == "a")
            col_c = next(c for c in buf.current_columns if c.name == "c")
            assert col_a.pinned and col_c.pinned
            assert dt.fixed_columns == 2
            # Both should be in positions 0-1
            pinned_positions = sorted([col_a.render_position, col_c.render_position])
            assert pinned_positions == [0, 1]

    @pytest.mark.asyncio
    async def test_cannot_move_pinned_past_unpinned(self, cli_args):
        """Column reorder should not swap pinned and unpinned columns."""
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(buf, ["a,b,c", "1,2,3"])
            await _wait(pilot, app)

            # Pin "a"
            dt = buf.query_one(Datatable)
            dt.move_cursor(column=0)
            await _wait(pilot, app)
            buf.action_pin_column()
            await _wait(pilot, app)

            # Try to move pinned "a" right — should be blocked
            dt.move_cursor(column=0)
            await _wait(pilot, app)
            buf.action_move_column_right()
            await _wait(pilot, app)

            col_a = next(c for c in buf.current_columns if c.name == "a")
            assert col_a.render_position == 0  # didn't move


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
            await _wait(pilot, app)

            # Three presses: asc → desc → clear
            assert buf.sort_column is None
            assert not buf._loading_reason
            # Rows should be in original insertion order (unsorted)
            names = [strip_markup(r[0]) for r in buf.displayed_rows]
            assert names == ["Charlie", "Alice", "Bob"]


# ---------------------------------------------------------------------------
# Delimiter Change (handle_delimiter_submitted)
# ---------------------------------------------------------------------------


async def _submit_prompt(app, pilot, action_name, input_id, value):
    """Mount a prompt via action, set value, and submit via Enter."""
    getattr(app, action_name)()
    await _wait(pilot, app)
    inp = app.query_one(f"#{input_id}")
    inp.value = value
    await pilot.press("enter")
    await _wait(pilot, app)


class TestDelimiterChange:
    @pytest.mark.asyncio
    async def test_change_delimiter_csv_to_tsv(self, cli_args):
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(buf, ["a\tb\tc", "1\t2\t3", "4\t5\t6"])
            await _wait(pilot, app)

            # Inferred as tab — change to comma
            assert buf.delimiter == "\t"
            initial_col_names = [c.name for c in buf.current_columns if not c.hidden]
            assert len(initial_col_names) == 3

            await _submit_prompt(app, pilot, "action_delimiter", "delimiter_input", ",")
            await _wait(pilot, app)

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
            await _wait(pilot, app)
            buf._perform_search("Alice")
            await _wait(pilot, app)
            assert buf.sort_column is not None
            assert buf.search_term is not None

            # Change delimiter — should clear state
            await _submit_prompt(app, pilot, "action_delimiter", "delimiter_input", ",")
            await _wait(pilot, app)

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
            await _wait(pilot, app)

            assert len([c for c in buf.current_columns if not c.hidden]) == 2

            await _submit_prompt(
                app, pilot, "action_delimiter", "delimiter_input", "raw"
            )
            await _wait(pilot, app)

            assert buf.delimiter == "raw"
            col_names = [c.name for c in buf.current_columns if not c.hidden]
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
            await _wait(pilot, app)
            assert buf.delimiter == "raw"

            await _submit_prompt(
                app, pilot, "action_delimiter", "delimiter_input", "json"
            )
            await _wait(pilot, app)

            assert buf.delimiter == "json"
            col_names = [c.name for c in buf.current_columns]
            assert "name" in col_names
            assert "age" in col_names

    @pytest.mark.asyncio
    async def test_change_delimiter_to_json_with_preamble(self):
        """Switching to JSON skips non-JSON preamble lines and finds first valid JSON."""
        raw_args = CliArgs(delimiter="raw", filters=[], unique_keys=set(), sort_by=None)
        app = NlessApp(cli_args=raw_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(
                buf,
                [
                    "# this is a comment",
                    "some other preamble",
                    '{"name":"Alice","age":30}',
                    '{"name":"Bob","age":25}',
                ],
            )
            await _wait(pilot, app)
            assert buf.delimiter == "raw"

            await _submit_prompt(
                app, pilot, "action_delimiter", "delimiter_input", "json"
            )
            await _wait(pilot, app)

            assert buf.delimiter == "json"
            col_names = [c.name for c in buf.current_columns if not c.hidden]
            assert "name" in col_names
            assert "age" in col_names
            # 2 preamble lines saved (comment + other preamble)
            assert len(buf._preamble_lines) == 2
            assert buf.displayed_rows, "Should have at least 1 data row"

    @pytest.mark.asyncio
    async def test_change_delimiter_to_json_from_csv_with_preamble(self):
        """Switching CSV -> JSON when first_log_line is CSV header, not JSON."""
        args = CliArgs(delimiter=",", filters=[], unique_keys=set(), sort_by=None)
        app = NlessApp(cli_args=args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(
                buf,
                [
                    "name,age",
                    '{"name":"Alice","age":30}',
                    '{"name":"Bob","age":25}',
                ],
            )
            await _wait(pilot, app)
            assert buf.delimiter == ","
            assert buf.first_log_line == "name,age"

            await _submit_prompt(
                app, pilot, "action_delimiter", "delimiter_input", "json"
            )
            await _wait(pilot, app)

            assert buf.delimiter == "json"
            col_names = [c.name for c in buf.current_columns if not c.hidden]
            assert "name" in col_names
            assert "age" in col_names
            # CSV header saved as preamble
            assert "name,age" in buf._preamble_lines
            # Both JSON lines displayed (in JSON mode, header line is also data)
            assert len(buf.displayed_rows) == 2

    @pytest.mark.asyncio
    async def test_json_to_raw_roundtrip_no_duplicates(self):
        """Switching json→raw must not duplicate lines or lose preamble."""
        args = CliArgs(delimiter="raw", filters=[], unique_keys=set(), sort_by=None)
        app = NlessApp(cli_args=args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(
                buf,
                [
                    "not json preamble",
                    '{"id":1,"msg":"hello"}',
                    '{"id":2,"msg":"world"}',
                ],
            )
            await _wait(pilot, app)

            # Switch to json — preamble saved
            await _submit_prompt(
                app, pilot, "action_delimiter", "delimiter_input", "json"
            )
            await _wait(pilot, app)
            assert buf.delimiter == "json"
            assert "not json preamble" in buf._preamble_lines

            # Switch back to raw — all 3 original lines visible, no dupes
            await _submit_prompt(
                app, pilot, "action_delimiter", "delimiter_input", "raw"
            )
            await _wait(pilot, app)
            assert buf.delimiter == "raw"
            displayed = [strip_markup(r[0]) for r in buf.displayed_rows]
            assert len(displayed) == 3, f"Expected 3, got {len(displayed)}: {displayed}"
            assert len(set(displayed)) == 3, f"Duplicates found: {displayed}"
            assert "not json preamble" in displayed

    @pytest.mark.asyncio
    async def test_regex_named_groups_delimiter(self, cli_args):
        raw_args = CliArgs(delimiter="raw", filters=[], unique_keys=set(), sort_by=None)
        app = NlessApp(cli_args=raw_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(buf, ["host=web1 level=INFO", "host=web2 level=ERROR"])
            await _wait(pilot, app)

            await _submit_prompt(
                app,
                pilot,
                "action_delimiter",
                "delimiter_input",
                r"(?P<host>\S+)\s+(?P<level>\S+)",
            )
            await _wait(pilot, app)

            # Dismiss the "Save as log format?" prompt
            await pilot.press("escape")
            await _wait(pilot, app)

            assert isinstance(buf.delimiter, re.Pattern)
            col_names = [c.name for c in buf.current_columns if not c.hidden]
            assert col_names == ["host", "level"]

    @pytest.mark.asyncio
    async def test_standard_to_raw_reinserts_header(self, cli_args):
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(buf, ["name,age", "Alice,30", "Bob,25"])
            await _wait(pilot, app)

            raw_count_before = len(buf.raw_rows)

            await _submit_prompt(
                app, pilot, "action_delimiter", "delimiter_input", "raw"
            )
            await _wait(pilot, app)

            # Header "name,age" should be reinserted as data
            assert len(buf.raw_rows) == raw_count_before + 1

    @pytest.mark.asyncio
    async def test_switch_to_raw_preserves_header(self):
        """Header line must appear in displayed_rows after switching to raw mode.

        Reproduces: standard delimiter where first line is the header;
        switching to raw via 'D' must reinsert the header as a data row.
        """
        space_args = CliArgs(delimiter=" ", filters=[], unique_keys=set(), sort_by=None)
        app = NlessApp(cli_args=space_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            lines = [
                "name age city",
                "Alice 30 NYC",
                "Bob 25 SF",
                "Charlie 35 LA",
                "Dave 40 CHI",
            ]
            _load(buf, lines)
            await _wait(pilot, app)

            assert buf.first_log_line == lines[0]
            assert len(buf.displayed_rows) == 4  # data rows only

            await _submit_prompt(
                app, pilot, "action_delimiter", "delimiter_input", "raw"
            )
            await _wait(pilot, app)

            # Header should be reinserted into raw_rows
            assert buf.first_log_line in buf.raw_rows

            # All 5 lines (header + 4 data) must appear in displayed_rows
            displayed_text = [strip_markup(r[0]) for r in buf.displayed_rows]
            assert len(displayed_text) == 5, (
                f"Expected 5 rows, got {len(displayed_text)}: {displayed_text}"
            )
            assert lines[0] in displayed_text, (
                f"Header line missing from displayed_rows: {displayed_text}"
            )

    @pytest.mark.asyncio
    async def test_switch_to_raw_preserves_header_inferred(self):
        """Same as above but with inferred (not forced) delimiter."""
        args = CliArgs(delimiter=None, filters=[], unique_keys=set(), sort_by=None)
        app = NlessApp(cli_args=args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            lines = [
                "name age city",
                "Alice 30 NYC",
                "Bob 25 SF",
                "Charlie 35 LA",
                "Dave 40 CHI",
            ]
            _load(buf, lines)
            await _wait(pilot, app)

            assert buf.first_log_line == lines[0]
            original_header = buf.first_log_line

            await _submit_prompt(
                app, pilot, "action_delimiter", "delimiter_input", "raw"
            )
            await _wait(pilot, app)

            displayed_text = [strip_markup(r[0]) for r in buf.displayed_rows]
            # Original header must appear somewhere in displayed output
            assert original_header in displayed_text, (
                f"Header '{original_header}' missing from displayed_rows: {displayed_text}"
            )

    @pytest.mark.asyncio
    async def test_switch_to_raw_preserves_skipped_preamble(self):
        """Header skipped by find_header_index must reappear in raw mode.

        Reproduces: CSV with quoted commas inferred as space-delimited.
        The CSV header has 2 space-separated tokens while data lines have
        42, so find_header_index skips it.  Switching to raw must recover it.
        """
        args = CliArgs(delimiter=None, filters=[], unique_keys=set(), sort_by=None)
        app = NlessApp(cli_args=args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            header = "timestamp,message,description,long message"
            lines = [
                header,
                '2024-06-01T12:00:00Z,"route=GET /api/data, status=200, response_time=150ms", desc one,"long msg one"',
                '2024-06-01T12:01:00Z,"route=POST /api/data, status=201, response_time=300ms", desc two,"long msg two"',
                '2024-06-01T12:02:00Z,"route=GET /api/data, status=500, response_time=50ms", desc three,"long msg three"',
                '2024-06-01T12:03:00Z,"route=GET /api/data, status=200, response_time=100ms", desc four,"long msg four"',
            ]
            _load(buf, lines)
            await _wait(pilot, app)

            # Header was skipped by find_header_index (space != consensus)
            assert buf.first_log_line != header
            assert buf._preamble_lines == [header]

            await _submit_prompt(
                app, pilot, "action_delimiter", "delimiter_input", "raw"
            )
            await _wait(pilot, app)

            displayed_text = [strip_markup(r[0]) for r in buf.displayed_rows]
            assert header in displayed_text, (
                f"Preamble header missing from raw output: {displayed_text[:3]}"
            )

    @pytest.mark.asyncio
    async def test_switch_to_correct_delimiter_restores_preamble_as_header(self):
        """Switching to comma after space mis-inference uses preamble as header.

        When find_header_index skips the CSV header (wrong field count for
        space), switching to comma should restore it as first_log_line so
        the correct column names are used.
        """
        args = CliArgs(delimiter=None, filters=[], unique_keys=set(), sort_by=None)
        app = NlessApp(cli_args=args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            header = "timestamp,message,description,long message"
            lines = [
                header,
                '2024-06-01T12:00:00Z,"route=GET /api/data, status=200, response_time=150ms", desc one,"long msg one"',
                '2024-06-01T12:01:00Z,"route=POST /api/data, status=201, response_time=300ms", desc two,"long msg two"',
                '2024-06-01T12:02:00Z,"route=GET /api/data, status=500, response_time=50ms", desc three,"long msg three"',
                '2024-06-01T12:03:00Z,"route=GET /api/data, status=200, response_time=100ms", desc four,"long msg four"',
            ]
            _load(buf, lines)
            await _wait(pilot, app)

            assert buf._preamble_lines == [header]

            await _submit_prompt(app, pilot, "action_delimiter", "delimiter_input", ",")
            await _wait(pilot, app)

            # Preamble should now be the header
            assert buf.first_log_line == header
            col_names = [
                strip_markup(c.name)
                for c in buf.current_columns
                if not c.hidden and c.name != MetadataColumn.ARRIVAL.value
            ]
            assert col_names == ["timestamp", "message", "description", "long message"]
            # All 4 data lines should be displayed
            assert len(buf.displayed_rows) == 4

    @pytest.mark.asyncio
    async def test_tab_escape_delimiter(self, cli_args):
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(buf, ["a\tb\tc", "1\t2\t3"])
            await _wait(pilot, app)

            await _submit_prompt(
                app, pilot, "action_delimiter", "delimiter_input", "\\t"
            )
            await _wait(pilot, app)

            assert buf.delimiter == "\t"


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
            await _wait(pilot, app)

            await _submit_prompt(
                app, pilot, "action_filter_columns", "column_filter_input", "age"
            )
            await _wait(pilot, app)

            visible = [c for c in buf.current_columns if not c.hidden]
            assert len(visible) == 1
            assert visible[0].name == "age"

    @pytest.mark.asyncio
    async def test_filter_columns_all_restores(self, cli_args):
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(buf, ["name,age,city", "Alice,30,NYC"])
            await _wait(pilot, app)

            # Hide some columns
            await _submit_prompt(
                app, pilot, "action_filter_columns", "column_filter_input", "age"
            )
            await _wait(pilot, app)
            assert sum(1 for c in buf.current_columns if c.hidden) > 0

            # Restore all
            await _submit_prompt(
                app, pilot, "action_filter_columns", "column_filter_input", "all"
            )
            await _wait(pilot, app)

            visible = [c for c in buf.current_columns if not c.hidden]
            hidden = [c for c in buf.current_columns if c.hidden]
            # All user columns restored; only hidden metadata stays hidden
            assert len(visible) == 3  # name, age, city
            assert all(c.name in {mc.value for mc in MetadataColumn} for c in hidden)

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
            await _wait(pilot, app)
            new_buf = app.buffers[1]
            app.curr_buffer_idx = 1

            # Filter columns — count should stay visible
            await _submit_prompt(
                app, pilot, "action_filter_columns", "column_filter_input", "city"
            )
            await _wait(pilot, app)

            visible_names = [c.name for c in new_buf.current_columns if not c.hidden]
            assert MetadataColumn.COUNT.value in visible_names
            assert "city" in visible_names

    @pytest.mark.asyncio
    async def test_filter_columns_multiple_patterns(self, cli_args):
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(buf, ["name,age,city,zip", "Alice,30,NYC,10001"])
            await _wait(pilot, app)

            await _submit_prompt(
                app,
                pilot,
                "action_filter_columns",
                "column_filter_input",
                "name|city",
            )
            await _wait(pilot, app)

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
            await _wait(pilot, app)

            # Move cursor to the JSON column (column 1)
            dt = buf.query_one(Datatable)
            dt.move_cursor(column=1)
            await _wait(pilot, app)

            initial_col_count = len(buf.current_columns)

            await _submit_prompt(
                app,
                pilot,
                "action_column_delimiter",
                "column_delimiter_input",
                "json",
            )
            await _wait(pilot, app)

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
            await _wait(pilot, app)

            # Move cursor to the "path" column
            dt = buf.query_one(Datatable)
            dt.move_cursor(column=1)
            await _wait(pilot, app)

            initial_col_count = len(buf.current_columns)

            await _submit_prompt(
                app,
                pilot,
                "action_column_delimiter",
                "column_delimiter_input",
                "-",
            )
            await _wait(pilot, app)

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
            await _wait(pilot, app)

            # Move cursor to JSON column
            dt = buf.query_one(Datatable)
            dt.move_cursor(column=1)
            await _wait(pilot, app)

            initial_col_count = len(buf.current_columns)

            # action_json_header mounts NlessSelect with JSON keys
            app.action_json_header()
            await _wait(pilot, app)

            # Press Enter to select the first key ("color")
            await pilot.press("enter")
            await _wait(pilot, app)

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
            await _wait(pilot, app)

            dt = buf.query_one(Datatable)
            dt.move_cursor(column=1)
            await _wait(pilot, app)

            app.action_json_header()
            await _wait(pilot, app)

            # Select first key
            await pilot.press("enter")
            await _wait(pilot, app)

            # After deferred update, the new column should have extracted values
            new_col = buf.current_columns[-1]
            new_col_idx = new_col.render_position
            values = [strip_markup(r[new_col_idx]) for r in buf.displayed_rows]
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
            await _wait(pilot, app)

            # Cursor at (0, 0) → cell "Alice"
            app.action_filter_cursor_word()
            await _wait(pilot, app)

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
            await _wait(pilot, app)

            # Cursor at (0, 0) → cell "foo.bar" — dot should be escaped
            app.action_filter_cursor_word()
            await _wait(pilot, app)

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
            await _wait(pilot, app)

            # Mark unique on "city"
            app.action_mark_unique()
            await _wait(pilot, app)

            assert len(app.buffers) == 2
            unique_buf = app.buffers[1]
            app.curr_buffer_idx = 1

            # Move cursor to the "city" column (the unique column)
            dt = unique_buf.query_one(Datatable)
            city_col_idx = unique_buf._get_col_idx_by_name("city", render_position=True)
            dt.move_cursor(column=city_col_idx, row=0)
            await _wait(pilot, app)

            # Press Enter to trigger _filter_composite_key
            await pilot.press("enter")
            await _wait(pilot, app)

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
            await _wait(pilot, app)
            assert len(app.buffers) == 2

            # Close the active buffer (buffer 1)
            app.action_close_active_buffer()
            await _wait(pilot, app)

            assert len(app.buffers) == 1

    @pytest.mark.asyncio
    async def test_close_buffer_adjusts_index(self, cli_args):
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(buf, ["name,age", "Alice,30", "Bob,25", "Charlie,35"])

            # Create two additional buffers
            app._perform_filter("Alice", "name")
            await _wait(pilot, app)
            app._perform_filter("Bob", "name")
            await _wait(pilot, app)
            assert len(app.buffers) == 3

            # Active is the last buffer (index 2)
            assert app.curr_buffer_idx == 2

            # Close last buffer
            app.action_close_active_buffer()
            await _wait(pilot, app)

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
            await _wait(pilot, app)

            assert len(buf.search_matches) == 2
            first_index = buf.current_match_index

            buf.action_next_search()
            await _wait(pilot, app)

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
            await _wait(pilot, app)

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
            await _wait(pilot, app)

            # Cursor at (0, 0) → "Alice"
            buf.action_search_cursor_word()
            await _wait(pilot, app)

            assert buf.search_term is not None
            assert buf.search_term.pattern == re.escape("Alice")
            assert len(buf.search_matches) == 1


class TestExcludeFilter:
    @pytest.mark.asyncio
    async def test_exclude_filter_by_column(self, cli_args):
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(
                buf,
                ["name,age,city", "Alice,30,NYC", "Bob,25,SF", "Charlie,35,NYC"],
            )

            app._perform_filter("NYC", "city", exclude=True)
            await _wait(pilot, app)

            assert len(app.buffers) == 2
            new_buf = app.buffers[1]
            assert len(new_buf.displayed_rows) == 1
            assert new_buf.displayed_rows[0][2] == "SF"

    @pytest.mark.asyncio
    async def test_exclude_filter_any_column(self, cli_args):
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(buf, ["name,age,city", "Alice,30,NYC", "Bob,25,SF"])

            app._perform_filter("Alice", exclude=True)
            await _wait(pilot, app)

            assert len(app.buffers) == 2
            assert len(app.buffers[1].displayed_rows) == 1
            assert app.buffers[1].displayed_rows[0][0] == "Bob"

    @pytest.mark.asyncio
    async def test_exclude_filter_regex(self, cli_args):
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(
                buf,
                ["name,age,city", "Alice,30,NYC", "Bob,25,SF", "Anna,28,LA"],
            )

            app._perform_filter("^A", "name", exclude=True)
            await _wait(pilot, app)

            new_buf = app.buffers[1]
            assert len(new_buf.displayed_rows) == 1
            assert new_buf.displayed_rows[0][0] == "Bob"

    @pytest.mark.asyncio
    async def test_exclude_filter_no_match_keeps_all(self, cli_args):
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(buf, ["name,age", "Alice,30", "Bob,25"])

            app._perform_filter("ZZZZZ", "name", exclude=True)
            await _wait(pilot, app)

            assert len(app.buffers) == 2
            assert len(app.buffers[1].displayed_rows) == 2

    @pytest.mark.asyncio
    async def test_exclude_and_include_combined(self, cli_args):
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(
                buf,
                [
                    "name,age,city",
                    "Alice,30,NYC",
                    "Bob,25,SF",
                    "Charlie,35,NYC",
                    "Diana,28,LA",
                ],
            )

            # Include filter: city=NYC
            app._perform_filter("NYC", "city")
            await _wait(pilot, app)

            assert len(app.buffers[1].displayed_rows) == 2

            # Exclude filter on top: name=Alice
            app.curr_buffer_idx = 1
            app._perform_filter("Alice", "name", exclude=True)
            await _wait(pilot, app)

            assert len(app.buffers) == 3
            new_buf = app.buffers[2]
            assert len(new_buf.displayed_rows) == 1
            assert new_buf.displayed_rows[0][0] == "Charlie"

    @pytest.mark.asyncio
    async def test_exclude_filter_preserves_original(self, cli_args):
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(buf, ["name,age", "Alice,30", "Bob,25", "Charlie,35"])

            app._perform_filter("Alice", "name", exclude=True)
            await _wait(pilot, app)

            assert len(buf.displayed_rows) == 3


# ---------------------------------------------------------------------------
# Arrival Timestamp Metadata Column
# ---------------------------------------------------------------------------


class TestArrivalTimestamp:
    @pytest.mark.asyncio
    async def test_arrival_column_exists_hidden(self, cli_args):
        """ARRIVAL column should be created hidden when first row is parsed."""
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(buf, ["a,b", "1,2"])
            await _wait(pilot, app)

            arrival = next(
                (
                    c
                    for c in buf.current_columns
                    if c.name == MetadataColumn.ARRIVAL.value
                ),
                None,
            )
            assert arrival is not None
            assert arrival.hidden is True
            assert arrival.computed is True

    @pytest.mark.asyncio
    async def test_arrival_timestamps_recorded(self, cli_args):
        """Arrival timestamps should be recorded for each raw row."""
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(buf, ["a,b", "1,2", "3,4"])
            await _wait(pilot, app)

            assert len(buf._arrival_timestamps) == len(buf.raw_rows)
            assert all(isinstance(ts, float) for ts in buf._arrival_timestamps)

    @pytest.mark.asyncio
    async def test_arrival_not_in_visible_columns(self, cli_args):
        """ARRIVAL should not appear in visible column labels by default."""
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(buf, ["a,b", "1,2"])
            await _wait(pilot, app)

            visible = buf._get_visible_column_labels()
            assert MetadataColumn.ARRIVAL.value not in visible

    @pytest.mark.asyncio
    async def test_toggle_arrival_shows_pinned(self, cli_args):
        """Pressing A should show arrival column pinned to the left."""
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(buf, ["a,b", "1,2"])
            await _wait(pilot, app)

            app.action_toggle_arrival()
            await _wait(pilot, app)

            arrival = next(
                c for c in buf.current_columns if c.name == MetadataColumn.ARRIVAL.value
            )
            assert arrival.hidden is False
            assert arrival.pinned is True
            # Should be in visible labels now
            visible = buf._get_visible_column_labels()
            assert MetadataColumn.ARRIVAL.value in visible

    @pytest.mark.asyncio
    async def test_toggle_arrival_hides_again(self, cli_args):
        """Pressing A twice should hide the arrival column again."""
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(buf, ["a,b", "1,2"])
            await _wait(pilot, app)

            app.action_toggle_arrival()
            await _wait(pilot, app)
            app.action_toggle_arrival()
            await _wait(pilot, app)

            arrival = next(
                c for c in buf.current_columns if c.name == MetadataColumn.ARRIVAL.value
            )
            assert arrival.hidden is True
            assert arrival.pinned is False

    @pytest.mark.asyncio
    async def test_toggle_arrival_pinned_left_of_data(self, cli_args):
        """Arrival column should appear before data columns when shown."""
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(buf, ["name,age", "Alice,30"])
            await _wait(pilot, app)

            app.action_toggle_arrival()
            await _wait(pilot, app)

            arrival = next(
                c for c in buf.current_columns if c.name == MetadataColumn.ARRIVAL.value
            )
            data_cols = [
                c
                for c in buf.current_columns
                if c.name != MetadataColumn.ARRIVAL.value and not c.hidden
            ]
            assert all(arrival.render_position < c.render_position for c in data_cols)


# ---------------------------------------------------------------------------
# Duration Parsing (_parse_duration)
# ---------------------------------------------------------------------------


class TestParseDuration:
    def test_plain_number_as_minutes(self):
        from nless.buffer import NlessBuffer

        assert NlessBuffer._parse_duration("5") == 300.0
        assert NlessBuffer._parse_duration("0.5") == 30.0

    def test_seconds(self):
        from nless.buffer import NlessBuffer

        assert NlessBuffer._parse_duration("30s") == 30.0

    def test_minutes(self):
        from nless.buffer import NlessBuffer

        assert NlessBuffer._parse_duration("5m") == 300.0

    def test_hours(self):
        from nless.buffer import NlessBuffer

        assert NlessBuffer._parse_duration("2h") == 7200.0

    def test_days(self):
        from nless.buffer import NlessBuffer

        assert NlessBuffer._parse_duration("1d") == 86400.0

    def test_compound_duration(self):
        from nless.buffer import NlessBuffer

        assert NlessBuffer._parse_duration("1h30m") == 5400.0
        assert NlessBuffer._parse_duration("1d2h30m15s") == 95415.0

    def test_empty_returns_none(self):
        from nless.buffer import NlessBuffer

        assert NlessBuffer._parse_duration("") is None
        assert NlessBuffer._parse_duration("   ") is None

    def test_invalid_returns_none(self):
        from nless.buffer import NlessBuffer

        assert NlessBuffer._parse_duration("abc") is None


# ---------------------------------------------------------------------------
# Time Window Filter
# ---------------------------------------------------------------------------


class TestTimeWindow:
    @pytest.mark.asyncio
    async def test_time_window_filters_rows(self, cli_args):
        """Setting a time window should filter out old rows."""
        import time

        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(buf, ["a,b", "1,2", "3,4", "5,6"])
            await _wait(pilot, app)

            # Backdate the first row's timestamp so it falls outside the window
            buf._arrival_timestamps[0] = time.time() - 7200  # 2 hours ago

            buf.time_window = 3600.0  # 1 hour
            buf._parsed_rows = None
            buf._cached_col_widths = None
            buf._deferred_update_table(reason="test")
            await _wait(pilot, app)

            # Only the 2 recent rows should remain visible
            assert len(buf.displayed_rows) == 2

    @pytest.mark.asyncio
    async def test_time_window_clear(self, cli_args):
        """Clearing the time window should restore all rows."""
        import time

        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(buf, ["a,b", "1,2", "3,4", "5,6"])
            await _wait(pilot, app)

            buf._arrival_timestamps[0] = time.time() - 7200
            buf.time_window = 3600.0
            buf._parsed_rows = None
            buf._cached_col_widths = None
            buf._deferred_update_table(reason="test")
            await _wait(pilot, app)
            assert len(buf.displayed_rows) == 2

            # Clear the window
            buf.time_window = None
            buf._parsed_rows = None
            buf._cached_col_widths = None
            buf._deferred_update_table(reason="test")
            await _wait(pilot, app)

            assert len(buf.displayed_rows) == 3

    @pytest.mark.asyncio
    async def test_time_window_via_action(self, cli_args):
        """The @ action without '+' should filter old rows and set a ceiling."""
        import time

        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(buf, ["a,b", "1,2", "3,4", "5,6"])
            await _wait(pilot, app)

            # Backdate the first row
            buf._arrival_timestamps[0] = time.time() - 7200

            await _submit_prompt(
                app, pilot, "action_time_window", "time_window_input", "1h"
            )
            await _wait(pilot, app)

            # Filter was applied (old row dropped)
            assert len(buf.displayed_rows) == 2
            # Non-rolling: time_window stays active with a ceiling
            assert buf.time_window == 3600.0
            assert buf._time_window_ceiling is not None

    @pytest.mark.asyncio
    async def test_time_window_off_via_action(self, cli_args):
        """Submitting 'off' should clear the time window."""
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(buf, ["a,b", "1,2"])
            await _wait(pilot, app)

            # Set a window first
            buf.time_window = 300.0

            await _submit_prompt(
                app, pilot, "action_time_window", "time_window_input", "off"
            )
            await _wait(pilot, app)

            assert buf.time_window is None

    @pytest.mark.asyncio
    async def test_fixed_window_stable_on_sort(self, cli_args):
        """After a fixed time window, sorting should keep the same row count."""
        import time

        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(buf, ["a,b", "1,2", "3,4", "5,6"])
            await _wait(pilot, app)

            buf._arrival_timestamps[0] = time.time() - 7200

            await _submit_prompt(
                app, pilot, "action_time_window", "time_window_input", "1h"
            )
            await _wait(pilot, app)
            assert len(buf.displayed_rows) == 2

            # Sort — fixed ceiling means the same rows stay visible
            buf.action_sort()
            await _wait(pilot, app)
            assert len(buf.displayed_rows) == 2

    @pytest.mark.asyncio
    async def test_rolling_window_via_action(self, cli_args):
        """Appending '+' should enable rolling mode."""
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(buf, ["a,b", "1,2"])
            await _wait(pilot, app)

            await _submit_prompt(
                app, pilot, "action_time_window", "time_window_input", "5m+"
            )
            await _wait(pilot, app)

            assert buf.time_window == 300.0
            assert buf.rolling_time_window is True
            assert buf._rolling_timer is not None

    @pytest.mark.asyncio
    async def test_non_rolling_window_has_ceiling(self, cli_args):
        """Without '+', a fixed ceiling is set and no timer runs."""
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(buf, ["a,b", "1,2"])
            await _wait(pilot, app)

            await _submit_prompt(
                app, pilot, "action_time_window", "time_window_input", "5m"
            )
            await _wait(pilot, app)

            # Fixed window: time_window stays, ceiling is set, no timer
            assert buf.time_window == 300.0
            assert buf.rolling_time_window is False
            assert buf._time_window_ceiling is not None
            assert buf._rolling_timer is None

    @pytest.mark.asyncio
    async def test_clear_stops_rolling(self, cli_args):
        """Clearing a rolling window should stop the timer."""
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(buf, ["a,b", "1,2"])
            await _wait(pilot, app)

            await _submit_prompt(
                app, pilot, "action_time_window", "time_window_input", "5m+"
            )
            await _wait(pilot, app)
            assert buf._rolling_timer is not None

            await _submit_prompt(
                app, pilot, "action_time_window", "time_window_input", "off"
            )
            await _wait(pilot, app)

            assert buf.time_window is None
            assert buf.rolling_time_window is False
            assert buf._rolling_timer is None


# ---------------------------------------------------------------------------
# Raw Pager Mode
# ---------------------------------------------------------------------------


class TestRawPagerMode:
    @pytest.mark.asyncio
    async def test_cli_raw_flag_enables_raw_mode(self):
        """--raw CLI flag should set raw_mode on the buffer."""
        args = CliArgs(
            delimiter="raw", filters=[], unique_keys=set(), sort_by=None, raw=True
        )
        app = NlessApp(cli_args=args, starting_stream=None)
        async with app.run_test(size=(120, 40)):
            buf = app.buffers[0]
            assert buf.raw_mode is True
            assert buf.delimiter == "raw"

    @pytest.mark.asyncio
    async def test_raw_mode_uses_raw_pager_widget(self):
        """In raw mode, the buffer should compose a RawPager widget."""
        from nless.rawpager import RawPager

        args = CliArgs(
            delimiter="raw", filters=[], unique_keys=set(), sort_by=None, raw=True
        )
        app = NlessApp(cli_args=args, starting_stream=None)
        async with app.run_test(size=(120, 40)):
            buf = app.buffers[0]
            widget = buf.query_one(".nless-view")
            assert isinstance(widget, RawPager)

    @pytest.mark.asyncio
    async def test_raw_mode_displays_lines_as_is(self):
        """Lines should appear in the pager without column parsing."""
        from nless.rawpager import RawPager

        args = CliArgs(
            delimiter="raw", filters=[], unique_keys=set(), sort_by=None, raw=True
        )
        app = NlessApp(cli_args=args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            lines = ["Hello world", "  indented line", "\ttab line"]
            _load(buf, lines)
            await _wait(pilot, app)

            pager = buf.query_one(RawPager)
            assert len(pager.rows) == 3
            # Each row is a single-element list containing the raw line
            assert pager.rows[0][0] == "Hello world"
            assert pager.rows[1][0] == "  indented line"
            assert pager.rows[2][0] == "\ttab line"

    @pytest.mark.asyncio
    async def test_raw_mode_auto_detection(self):
        """Non-tabular input should auto-detect raw mode."""
        from nless.rawpager import RawPager

        args = CliArgs(delimiter=None, filters=[], unique_keys=set(), sort_by=None)
        app = NlessApp(cli_args=args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            # Single-word lines with no delimiter pattern
            _load(buf, ["hello", "world", "foo"])
            await _wait(pilot, app)

            assert buf.raw_mode is True
            assert buf.delimiter == "raw"
            widget = buf.query_one(".nless-view")
            assert isinstance(widget, RawPager)

    @pytest.mark.asyncio
    async def test_raw_mode_switch_to_table(self):
        """Pressing D and entering a delimiter should switch to table mode."""
        from nless.rawpager import RawPager

        args = CliArgs(
            delimiter="raw", filters=[], unique_keys=set(), sort_by=None, raw=True
        )
        app = NlessApp(cli_args=args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(buf, ["name,age", "Alice,30", "Bob,25"])
            await _wait(pilot, app)

            assert buf.raw_mode is True

            # Switch to CSV delimiter
            buf.switch_delimiter(",")
            await _wait(pilot, app)

            assert buf.raw_mode is False
            assert buf.delimiter == ","
            widget = buf.query_one(".nless-view")
            assert not isinstance(widget, RawPager)

    @pytest.mark.asyncio
    async def test_raw_mode_switch_from_table(self):
        """Switching delimiter to 'raw' from table mode should enable raw mode."""
        from nless.rawpager import RawPager

        args = CliArgs(delimiter=None, filters=[], unique_keys=set(), sort_by=None)
        app = NlessApp(cli_args=args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(buf, ["name,age", "Alice,30", "Bob,25"])
            await _wait(pilot, app)

            assert buf.raw_mode is False

            buf.switch_delimiter("raw")
            await _wait(pilot, app)

            assert buf.raw_mode is True
            widget = buf.query_one(".nless-view")
            assert isinstance(widget, RawPager)

    @pytest.mark.asyncio
    async def test_raw_mode_search(self):
        """Search should work in raw mode, highlighting matches."""
        args = CliArgs(
            delimiter="raw", filters=[], unique_keys=set(), sort_by=None, raw=True
        )
        app = NlessApp(cli_args=args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(buf, ["Hello world", "Goodbye world", "Hello again"])
            await _wait(pilot, app)

            buf._perform_search("Hello")
            await _wait(pilot, app)

            assert buf.search_term is not None
            assert len(buf.search_matches) == 2

    @pytest.mark.asyncio
    async def test_raw_mode_navigation(self):
        """Cursor navigation should work in raw mode."""
        from nless.rawpager import RawPager

        args = CliArgs(
            delimiter="raw", filters=[], unique_keys=set(), sort_by=None, raw=True
        )
        app = NlessApp(cli_args=args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(buf, ["line 1", "line 2", "line 3"])
            await _wait(pilot, app)

            pager = buf.query_one(RawPager)
            pager.move_cursor(row=2)
            assert pager.cursor_row == 2
            pager.move_cursor(row=0)
            assert pager.cursor_row == 0

    @pytest.mark.asyncio
    async def test_raw_mode_brackets_escaped(self):
        """Lines with Rich markup-like brackets should display safely."""
        from nless.rawpager import RawPager

        args = CliArgs(
            delimiter="raw", filters=[], unique_keys=set(), sort_by=None, raw=True
        )
        app = NlessApp(cli_args=args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(buf, ['f"[/{color}]"', "normal line", "[bold]not markup"])
            await _wait(pilot, app)

            pager = buf.query_one(RawPager)
            assert len(pager.rows) == 3
            # Brackets should be escaped so Rich doesn't interpret them
            assert "\\[" in pager.rows[0][0]
            assert "\\[" in pager.rows[2][0]


# ---------------------------------------------------------------------------
# Write buffer to file
# ---------------------------------------------------------------------------


class TestWriteBuffer:
    @pytest.mark.asyncio
    async def test_write_csv_output(self, cli_args, tmp_path):
        from nless.operations import write_buffer

        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(buf, ["name,age,city", "Alice,30,NYC", "Bob,25,SF"])
            await _wait(pilot, app)

            output_path = str(tmp_path / "output.csv")
            write_buffer(buf, output_path)

            with open(output_path) as f:
                lines = f.readlines()
            assert len(lines) == 3  # header + 2 data rows
            assert "name" in lines[0]
            assert "Alice" in lines[1]
            assert "Bob" in lines[2]

    @pytest.mark.asyncio
    async def test_write_empty_buffer(self, cli_args, tmp_path):
        from nless.operations import write_buffer

        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(buf, ["name,age"])
            await _wait(pilot, app)

            output_path = str(tmp_path / "output.csv")
            write_buffer(buf, output_path)

            with open(output_path) as f:
                lines = f.readlines()
            # Header only, no data rows
            assert len(lines) == 1
            assert "name" in lines[0]

    def test_infer_output_format(self):
        from nless.operations import _infer_output_format

        assert _infer_output_format("out.json") == "json"
        assert _infer_output_format("out.jsonl") == "json"
        assert _infer_output_format("out.tsv") == "tsv"
        assert _infer_output_format("out.csv") == "csv"
        assert _infer_output_format("out.txt") == "raw"
        assert _infer_output_format("out.log") == "raw"
        assert _infer_output_format("out.xyz") == "csv"
        assert _infer_output_format("out") == "csv"
        assert _infer_output_format("/dev/stdout") == "csv"

    @pytest.mark.asyncio
    async def test_write_json_output(self, cli_args, tmp_path):
        import json as json_mod

        from nless.operations import write_buffer

        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(buf, ["name,age,city", "Alice,30,NYC", "Bob,25,SF"])
            await _wait(pilot, app)

            output_path = str(tmp_path / "output.json")
            write_buffer(buf, output_path)

            with open(output_path) as f:
                lines = f.readlines()
            assert len(lines) == 2  # JSON Lines, no header row
            obj = json_mod.loads(lines[0])
            assert obj["name"] == "Alice"
            assert obj["age"] == "30"

    @pytest.mark.asyncio
    async def test_write_tsv_output(self, cli_args, tmp_path):
        from nless.operations import write_buffer

        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(buf, ["name,age,city", "Alice,30,NYC", "Bob,25,SF"])
            await _wait(pilot, app)

            output_path = str(tmp_path / "output.tsv")
            write_buffer(buf, output_path)

            with open(output_path) as f:
                content = f.read()
            assert "\t" in content
            assert "name" in content.split("\n")[0]

    @pytest.mark.asyncio
    async def test_write_raw_output(self, cli_args, tmp_path):
        from nless.operations import write_buffer

        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(buf, ["name,age,city", "Alice,30,NYC", "Bob,25,SF"])
            await _wait(pilot, app)

            output_path = str(tmp_path / "output.txt")
            write_buffer(buf, output_path)

            with open(output_path) as f:
                lines = f.readlines()
            # Raw format: no header row, tab-separated values
            assert len(lines) == 2
            assert "Alice" in lines[0]

    @pytest.mark.asyncio
    async def test_write_unknown_extension_defaults_to_csv(self, cli_args, tmp_path):
        from nless.operations import write_buffer

        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(buf, ["name,age,city", "Alice,30,NYC", "Bob,25,SF"])
            await _wait(pilot, app)

            output_path = str(tmp_path / "output.xyz")
            write_buffer(buf, output_path)

            with open(output_path) as f:
                lines = f.readlines()
            # CSV: header + 2 data rows
            assert len(lines) == 3
            assert "name" in lines[0]

    @pytest.mark.asyncio
    async def test_write_jsonl_extension(self, cli_args, tmp_path):
        import json as json_mod

        from nless.operations import write_buffer

        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(buf, ["name,age,city", "Alice,30,NYC"])
            await _wait(pilot, app)

            output_path = str(tmp_path / "output.jsonl")
            write_buffer(buf, output_path)

            with open(output_path) as f:
                lines = f.readlines()
            assert len(lines) == 1
            obj = json_mod.loads(lines[0])
            assert obj["name"] == "Alice"


class TestColumnAggregations:
    def test_numeric_aggregations(self):
        from nless.operations import compute_column_aggregations

        class FakeBuffer:
            displayed_rows = [["Alice", "30"], ["Bob", "25"], ["Carol", "45"]]

        result = compute_column_aggregations(FakeBuffer(), 1)
        assert "Count: 3" in result
        assert "Distinct: 3" in result
        assert "Sum: 100" in result
        assert "Min: 25" in result
        assert "Max: 45" in result

    def test_non_numeric_aggregations(self):
        from nless.operations import compute_column_aggregations

        class FakeBuffer:
            displayed_rows = [["Alice", "NYC"], ["Bob", "SF"], ["Carol", "NYC"]]

        result = compute_column_aggregations(FakeBuffer(), 1)
        assert "Count: 3" in result
        assert "Distinct: 2" in result
        assert "Sum" not in result
        assert "Avg" not in result

    def test_mixed_numeric_non_numeric(self):
        from nless.operations import compute_column_aggregations

        class FakeBuffer:
            displayed_rows = [["Alice", "30"], ["Bob", "N/A"], ["Carol", "45"]]

        result = compute_column_aggregations(FakeBuffer(), 1)
        assert "Count: 3" in result
        assert "Sum: 75" in result
        assert "1 non-numeric skipped" in result

    def test_empty_buffer(self):
        from nless.operations import compute_column_aggregations

        class FakeBuffer:
            displayed_rows = []

        result = compute_column_aggregations(FakeBuffer(), 0)
        assert result is None

    def test_column_index_out_of_range(self):
        from nless.operations import compute_column_aggregations

        class FakeBuffer:
            displayed_rows = [["Alice"]]

        result = compute_column_aggregations(FakeBuffer(), 5)
        assert result is None

    @pytest.mark.asyncio
    async def test_aggregations_action(self, cli_args):
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(buf, ["name,age,city", "Alice,30,NYC", "Bob,25,SF", "Carol,45,LA"])
            await _wait(pilot, app)

            # Move cursor to age column (column 1)
            data_table = buf.query_one(".nless-view")
            data_table.cursor_column = 1
            await pilot.pause()

            buf.action_aggregations()
            await pilot.pause()
            # Verify it doesn't crash — notification content is tested above


class TestViewExcludedLines:
    @pytest.mark.asyncio
    async def test_tilde_after_deleting_prior_buffer(self, cli_args):
        """~ should find excluded lines even after the parent buffer is deleted."""
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            _load(buf, ["name,status", "alice,running", "bob,pending", "carol,running"])
            await _wait(pilot, app)

            # Filter to only "running" rows — creates Buffer 1
            app._perform_filter("^running$", "status")
            await _wait(pilot, app)
            assert len(app.buffers) == 2

            # Delete the original (Buffer 0)
            app._switch_to_buffer(0)
            app.action_close_active_buffer()
            await pilot.pause()
            assert len(app.buffers) == 1

            # Press ~ — should find "bob,pending" as excluded
            curr = app._get_current_buffer()
            curr.action_view_unparsed_logs()
            await _wait(pilot, app)
            assert len(app.buffers) == 2, "Unparsed buffer should have been created"

    @pytest.mark.asyncio
    async def test_chained_tilde_after_deleting_intermediate_buffers(self, cli_args):
        """Chained ~ should work after closing intermediate buffers.

        Scenario: json-parsed file → ~ shows non-JSON lines → filter ~
        by [INFO → close unfiltered ~ → ~ on filtered → close filtered →
        ~ on remaining raw buffer should still find excluded lines.
        """
        from nless.input import LineStream

        cli_args_json = CliArgs(
            delimiter="json",
            filters=[],
            unique_keys=set(),
            sort_by=None,
        )
        stream = LineStream()
        app = NlessApp(cli_args=cli_args_json, starting_stream=stream)
        async with app.run_test(size=(120, 40)) as pilot:
            stream.notify(
                [
                    '{"level":"error","msg":"timeout"}',
                    "[INFO] server started",
                    '{"level":"warn","msg":"slow"}',
                    "[INFO] health check",
                    "[WARN] disk high",
                ]
            )
            await _wait(pilot, app)

            # ~ to see unparsed lines (non-JSON)
            app.buffers[0].action_view_unparsed_logs()
            await _wait(pilot, app)
            assert len(app.buffers) == 2

            # Filter unparsed buffer by [INFO, then close the unfiltered one
            app._switch_to_buffer(1)
            await pilot.pause()
            app._perform_filter(r"\[INFO", "log")
            await _wait(pilot, app)
            app._switch_to_buffer(1)
            app.action_close_active_buffer()
            await pilot.pause()

            # ~ on filtered unparsed buffer
            for i, b in enumerate(app.buffers):
                if b.current_filters:
                    app._switch_to_buffer(i)
                    break
            app._get_current_buffer().action_view_unparsed_logs()
            await _wait(pilot, app)

            # Close the filtered buffer
            for i, b in enumerate(app.buffers):
                if b.current_filters:
                    app._switch_to_buffer(i)
                    app.action_close_active_buffer()
                    break
            await pilot.pause()

            # ~ on the remaining raw unparsed buffer — should NOT say "all shown"
            for i, b in enumerate(app.buffers):
                if b.delimiter == "raw" and not b.current_filters:
                    app._switch_to_buffer(i)
                    break
            before = len(app.buffers)
            app._get_current_buffer().action_view_unparsed_logs()
            await _wait(pilot, app)
            assert len(app.buffers) > before, (
                "~ should find excluded lines, not say 'all logs are being shown'"
            )
