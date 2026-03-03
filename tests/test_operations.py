"""Tests for sorting, filtering, searching, uniqueness, and column operations."""

import pytest

from nless.app import NlessApp
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
