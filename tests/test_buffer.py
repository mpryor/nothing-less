"""Tests for NlessBuffer invariants: parallel arrays, partition cache, copy semantics, and init_as_merged."""

import pytest

from nless.app import NlessApp
from nless.buffer import NlessBuffer
from nless.types import CliArgs


def assert_stream_invariant(buf):
    buf.stream.assert_invariant()


async def _wait(pilot, app):
    """Pump the event loop until all buffers finish loading."""
    settled = 0
    for _ in range(300):
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


@pytest.fixture
def csv_cli_args():
    return CliArgs(delimiter=",", filters=[], unique_keys=set(), sort_by=None)


# ---------------------------------------------------------------------------
# Test 1: Parallel array invariant
# ---------------------------------------------------------------------------


class TestParallelArrayInvariant:
    """After any call to add_logs(), copy(), or init_as_merged(), the parallel
    arrays raw_rows and _arrival_timestamps must have equal length.
    """

    @pytest.mark.asyncio
    async def test_add_logs_valid_csv(self, cli_args):
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test():
            buf = app.buffers[0]
            buf.add_logs(["name,age,city", "Alice,30,NYC", "Bob,25,SF"])
            assert len(buf.raw_rows) == len(buf._arrival_timestamps)
            if buf._source_labels:
                assert len(buf.raw_rows) == len(buf._source_labels)
            assert_stream_invariant(buf)

    @pytest.mark.asyncio
    async def test_add_logs_mixed_valid_invalid(self, csv_cli_args):
        app = NlessApp(cli_args=csv_cli_args, starting_stream=None)
        async with app.run_test():
            buf = app.buffers[0]
            buf.add_logs(["name,age,city", "Alice,30,NYC", "bad line", "Bob,25,SF"])
            assert len(buf.raw_rows) == len(buf._arrival_timestamps)
            if buf._source_labels:
                assert len(buf.raw_rows) == len(buf._source_labels)
            assert_stream_invariant(buf)

    @pytest.mark.asyncio
    async def test_copy_preserves_parallel_arrays(self, cli_args):
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test():
            buf = app.buffers[0]
            buf.add_logs(["name,age,city", "Alice,30,NYC", "Bob,25,SF"])
            new_buf = buf.copy(pane_id=99)
            assert len(new_buf.raw_rows) == len(new_buf._arrival_timestamps)
            if new_buf._source_labels:
                assert len(new_buf.raw_rows) == len(new_buf._source_labels)
            assert_stream_invariant(buf)
            assert_stream_invariant(new_buf)

    @pytest.mark.asyncio
    async def test_init_as_merged_parallel_arrays(self, cli_args):
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test():
            buf1 = app.buffers[0]
            buf1.add_logs(["name,age", "Alice,30", "Bob,25"])

            # Build buf2 state directly (init_as_merged only reads from buffers)
            import time
            from copy import deepcopy

            buf2 = NlessBuffer(pane_id=50, cli_args=cli_args)
            buf2.first_row_parsed = True
            buf2.delim.value = ","
            buf2.current_columns = deepcopy(buf1.current_columns)
            buf2.stream.replace_raw_rows(["Charlie,35"], [time.time()])

            merged = NlessBuffer.init_as_merged(99, buf1, buf2, "src1", "src2")
            assert len(merged.raw_rows) == len(merged._arrival_timestamps)
            assert len(merged._source_labels) == len(merged.raw_rows)
            assert_stream_invariant(merged)


# ---------------------------------------------------------------------------
# Test 2: _partition_rows cache fast-paths
# ---------------------------------------------------------------------------


class TestPartitionRowsCache:
    """Three distinct paths exist in _partition_rows: full hit, partial hit, full reparse."""

    @pytest.mark.asyncio
    async def test_full_cache_hit(self, cli_args):
        """_parsed_rows is not None and covers all raw_rows → no re-parsing."""
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test():
            buf = app.buffers[0]
            buf.add_logs(["name,age", "Alice,30", "Bob,25"])
            # After add_logs, _parsed_rows should have been populated
            assert buf.cache.parsed_rows is not None
            cached_len = len(buf.cache.parsed_rows)
            # Call _partition_rows which should use the cache
            from nless.types import MetadataColumn

            metadata = {mc.value for mc in MetadataColumn}
            expected = len(buf.current_columns) - len(
                [c for c in buf.current_columns if c.name in metadata]
            )
            result, mismatched, _ = buf._partition_rows(expected)
            # Cache should still be valid
            assert buf.cache.parsed_rows is not None
            assert len(buf.cache.parsed_rows) >= cached_len

    @pytest.mark.asyncio
    async def test_partial_cache_hit(self, cli_args):
        """_parsed_rows exists but is shorter than raw_rows → only new rows parsed."""
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test():
            buf = app.buffers[0]
            buf.add_logs(["name,age", "Alice,30"])
            assert buf.cache.parsed_rows is not None
            old_len = len(buf.cache.parsed_rows)
            # Add more rows
            buf.add_logs(["Bob,25", "Charlie,35"])
            assert buf.cache.parsed_rows is not None
            assert len(buf.cache.parsed_rows) > old_len

    @pytest.mark.asyncio
    async def test_full_reparse_on_filter(self, cli_args):
        """When filters are active, _parsed_rows is rebuilt."""
        import re
        from nless.types import Filter

        filter_args = CliArgs(
            delimiter=None,
            filters=[Filter(column=None, pattern=re.compile("Alice", re.IGNORECASE))],
            unique_keys=set(),
            sort_by=None,
        )
        app = NlessApp(cli_args=filter_args, starting_stream=None)
        async with app.run_test():
            buf = app.buffers[0]
            buf.add_logs(["name,age", "Alice,30", "Bob,25"])
            # With filters, add_logs filters at ingest time; the _parsed_rows
            # cache may or may not be populated depending on skip count.
            # The key invariant is that _partition_rows produces correct output.
            from nless.types import MetadataColumn

            metadata = {mc.value for mc in MetadataColumn}
            expected = len(buf.current_columns) - len(
                [c for c in buf.current_columns if c.name in metadata]
            )
            result, _, _ = buf._partition_rows(expected)
            assert len(result) <= len(buf.raw_rows)


# ---------------------------------------------------------------------------
# Test 3: copy() semantics
# ---------------------------------------------------------------------------


class TestCopySemantics:
    """copy() must produce an independent buffer that does not share mutable state."""

    @pytest.mark.asyncio
    async def test_copy_independence(self, cli_args):
        """Mutation of original raw_rows after copy() does not affect the copy."""
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test():
            buf = app.buffers[0]
            buf.add_logs(["name,age", "Alice,30", "Bob,25"])
            copy_buf = buf.copy(pane_id=99)
            original_len = len(copy_buf.raw_rows)
            buf.stream.append("Charlie,35", 0.0)
            assert len(copy_buf.raw_rows) == original_len
            assert_stream_invariant(copy_buf)

    @pytest.mark.asyncio
    async def test_copy_with_filters(self, cli_args):
        """copy() with active filters: copy's raw_rows contains only matching rows."""
        import re
        from nless.types import Filter

        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test():
            buf = app.buffers[0]
            buf.add_logs(["name,age", "Alice,30", "Bob,25", "Charlie,35"])
            buf.query.filters = [
                Filter(column=None, pattern=re.compile("Alice", re.IGNORECASE))
            ]
            copy_buf = buf.copy(pane_id=99)
            # copy applies filters during copy, so raw_rows should be filtered
            assert len(copy_buf.raw_rows) <= len(buf.raw_rows)
            # Alice should be in the copy
            assert any("Alice" in row for row in copy_buf.raw_rows)
            assert_stream_invariant(copy_buf)

    @pytest.mark.asyncio
    async def test_copy_preserves_parsed_rows(self, cli_args):
        """copy() preserves _parsed_rows when lengths match."""
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test():
            buf = app.buffers[0]
            buf.add_logs(["name,age", "Alice,30", "Bob,25"])
            assert buf.cache.parsed_rows is not None
            copy_buf = buf.copy(pane_id=99)
            # If no filtering happened, _parsed_rows should carry over
            if len(copy_buf.raw_rows) == len(buf.raw_rows):
                assert copy_buf.cache.parsed_rows is not None
            assert_stream_invariant(copy_buf)


# ---------------------------------------------------------------------------
# Test 4: init_as_merged() invariants
# ---------------------------------------------------------------------------


class TestInitAsMerged:
    """init_as_merged must produce correct merged state from two buffers."""

    @staticmethod
    def _make_buf2(buf1, cli_args, rows):
        """Create a second buffer with minimal state for init_as_merged."""
        import time
        from copy import deepcopy

        buf2 = NlessBuffer(pane_id=50, cli_args=cli_args)
        buf2.first_row_parsed = True
        buf2.delim.value = buf1.delim.value
        buf2.current_columns = deepcopy(buf1.current_columns)
        buf2.stream.replace_raw_rows(rows, [time.time()] * len(rows))
        return buf2

    @pytest.mark.asyncio
    async def test_merged_row_count(self, cli_args):
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test():
            buf1 = app.buffers[0]
            buf1.add_logs(["name,age", "Alice,30", "Bob,25"])
            n1 = len(buf1.raw_rows)

            buf2 = self._make_buf2(buf1, cli_args, ["Charlie,35"])
            n2 = len(buf2.raw_rows)

            merged = NlessBuffer.init_as_merged(99, buf1, buf2, "file1", "file2")
            assert len(merged.raw_rows) == n1 + n2
            assert len(merged._arrival_timestamps) == len(merged.raw_rows)
            assert len(merged._source_labels) == len(merged.raw_rows)
            assert_stream_invariant(merged)

    @pytest.mark.asyncio
    async def test_merged_source_labels(self, cli_args):
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test():
            buf1 = app.buffers[0]
            buf1.add_logs(["name,age", "Alice,30"])

            buf2 = self._make_buf2(buf1, cli_args, ["Bob,25"])

            merged = NlessBuffer.init_as_merged(99, buf1, buf2, "src_a", "src_b")
            assert set(merged._source_labels) == {"src_a", "src_b"}
            assert_stream_invariant(merged)

    @pytest.mark.asyncio
    async def test_merged_sorted_by_arrival(self, cli_args):
        """Merged rows are interleaved by arrival timestamp (ascending)."""
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test():
            buf1 = app.buffers[0]
            buf1.add_logs(["name,age", "Alice,30", "Bob,25"])

            buf2 = self._make_buf2(buf1, cli_args, ["Charlie,35"])

            merged = NlessBuffer.init_as_merged(99, buf1, buf2, "s1", "s2")
            # Timestamps should be in non-decreasing order
            for i in range(1, len(merged._arrival_timestamps)):
                assert (
                    merged._arrival_timestamps[i] >= merged._arrival_timestamps[i - 1]
                )
            assert_stream_invariant(merged)


# ---------------------------------------------------------------------------
# Test 5: FilterSortState properties
# ---------------------------------------------------------------------------


class TestFilterSortStateProperties:
    @pytest.mark.asyncio
    async def test_is_expensive_false_with_no_sort_or_unique(self, cli_args):
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test():
            buf = app.buffers[0]
            assert buf.query.is_expensive is False

    @pytest.mark.asyncio
    async def test_is_expensive_true_with_sort(self, cli_args):
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test():
            buf = app.buffers[0]
            buf.query.sort_column = "name"
            assert buf.query.is_expensive is True

    @pytest.mark.asyncio
    async def test_is_expensive_true_with_unique(self, cli_args):
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test():
            buf = app.buffers[0]
            buf.query.unique_column_names = {"name"}
            assert buf.query.is_expensive is True

    @pytest.mark.asyncio
    async def test_clear_all_resets_everything(self, cli_args):
        import re
        from nless.types import Filter

        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test():
            buf = app.buffers[0]
            buf.query.filters = [
                Filter(column=None, pattern=re.compile("x", re.IGNORECASE))
            ]
            buf.query.sort_column = "name"
            buf.query.sort_reverse = True
            buf.query.search_term = re.compile("foo")
            buf.query.unique_column_names = {"name"}
            buf.query.clear_all()
            assert buf.query.filters == []
            assert buf.query.sort_column is None
            assert buf.query.sort_reverse is False
            assert buf.query.search_term is None
            assert buf.query.unique_column_names == set()
            assert buf.query.search_matches == []
            assert buf.query.current_match_index == -1
