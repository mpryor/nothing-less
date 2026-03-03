"""Tests for streaming data into the app via add_logs and _add_rows_incremental."""

import pytest

from nless.app import NlessApp
from nless.input import LineStream
from nless.types import CliArgs


async def _wait(pilot, app):
    """Pump the event loop until all buffers finish loading."""
    settled = 0
    for _ in range(300):  # 3s max
        await pilot.pause(delay=0.01)
        if all(not b._is_loading for b in app.buffers):
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


class TestAddLogsCsv:
    """Test add_logs with CSV data."""

    @pytest.mark.asyncio
    async def test_basic_csv(self, cli_args):
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test():
            buf = app.buffers[0]
            buf.add_logs(["name,age,city", "Alice,30,NYC", "Bob,25,SF"])
            assert buf.delimiter == ","
            assert buf.first_row_parsed
            assert len(buf.displayed_rows) == 2
            assert buf.displayed_rows[0] == ["Alice", "30", "NYC"]
            assert buf.displayed_rows[1] == ["Bob", "25", "SF"]

    @pytest.mark.asyncio
    async def test_csv_with_quotes(self, cli_args):
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test():
            buf = app.buffers[0]
            buf.add_logs(
                [
                    "name,age,city",
                    'Alice,30,"New York"',
                    'Bob,25,"San Francisco"',
                ]
            )
            assert len(buf.displayed_rows) == 2
            assert buf.displayed_rows[0][2] == "New York"
            assert buf.displayed_rows[1][2] == "San Francisco"


class TestAddLogsMultipleChunks:
    """Test multiple add_logs calls simulating streaming input."""

    @pytest.mark.asyncio
    async def test_two_chunks(self, cli_args):
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test():
            buf = app.buffers[0]
            buf.add_logs(["name,age,city", "Alice,30,NYC"])
            assert len(buf.displayed_rows) == 1

            buf.add_logs(["Bob,25,SF", "Charlie,35,LA"])
            assert len(buf.displayed_rows) == 3

    @pytest.mark.asyncio
    async def test_many_small_chunks(self, cli_args):
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test():
            buf = app.buffers[0]
            buf.add_logs(["name,age,city", "Alice,30,NYC"])
            for i in range(10):
                buf.add_logs([f"Person{i},{20 + i},City{i}"])
            assert len(buf.displayed_rows) == 11

    @pytest.mark.asyncio
    async def test_columns_consistent_across_chunks(self, cli_args):
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test():
            buf = app.buffers[0]
            buf.add_logs(["name,age,city", "Alice,30,NYC"])
            cols_after_first = [c.name for c in buf.current_columns]

            buf.add_logs(["Bob,25,SF"])
            cols_after_second = [c.name for c in buf.current_columns]

            assert cols_after_first == cols_after_second


class TestAddLogsDelimiters:
    """Test add_logs with various delimiter types."""

    @pytest.mark.asyncio
    async def test_tsv(self, cli_args):
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test():
            buf = app.buffers[0]
            buf.add_logs(["name\tage\tcity", "Alice\t30\tNYC"])
            assert buf.delimiter == "\t"
            assert len(buf.displayed_rows) == 1
            assert buf.displayed_rows[0] == ["Alice", "30", "NYC"]

    @pytest.mark.asyncio
    async def test_pipe_delimiter(self, cli_args):
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test():
            buf = app.buffers[0]
            buf.add_logs(["name|age|city", "Alice|30|NYC"])
            assert buf.delimiter == "|"
            assert len(buf.displayed_rows) == 1

    @pytest.mark.asyncio
    async def test_explicit_csv_delimiter(self, csv_cli_args):
        app = NlessApp(cli_args=csv_cli_args, starting_stream=None)
        async with app.run_test():
            buf = app.buffers[0]
            buf.add_logs(["name,age,city", "Alice,30,NYC"])
            assert buf.delimiter == ","
            assert len(buf.displayed_rows) == 1

    @pytest.mark.asyncio
    async def test_json_delimiter(self):
        args = CliArgs(delimiter="json", filters=[], unique_keys=set(), sort_by=None)
        app = NlessApp(cli_args=args, starting_stream=None)
        async with app.run_test():
            buf = app.buffers[0]
            buf.add_logs(
                [
                    '{"name": "Alice", "age": 30}',
                    '{"name": "Bob", "age": 25}',
                ]
            )
            assert len(buf.displayed_rows) == 2


class TestLargeBatchChunking:
    """Test the chunked processing path for large batches."""

    @pytest.mark.asyncio
    async def test_large_batch_processed(self, cli_args):
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test():
            buf = app.buffers[0]
            header = "a,b,c"
            data = [f"{i},{i + 1},{i + 2}" for i in range(60000)]
            buf.add_logs([header] + data)
            assert len(buf.displayed_rows) == 60000

    @pytest.mark.asyncio
    async def test_loading_flag_cleared_after_large_batch(self, cli_args):
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test():
            buf = app.buffers[0]
            header = "a,b,c"
            data = [f"{i},{i + 1},{i + 2}" for i in range(60000)]
            buf.add_logs([header] + data)
            assert not buf._is_loading

    @pytest.mark.asyncio
    async def test_loading_flag_not_set_for_small_batch(self, cli_args):
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test():
            buf = app.buffers[0]
            buf.add_logs(["a,b,c", "1,2,3"])
            assert not buf._is_loading


class TestIncrementalRowAddition:
    """Test _add_rows_incremental specifically."""

    @pytest.mark.asyncio
    async def test_column_widths_tracked(self, cli_args):
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test():
            buf = app.buffers[0]
            buf.add_logs(["name,age", "Alice,30", "Bartholomew,25"])
            from nless.datatable import Datatable

            dt = buf.query_one(Datatable)
            # Column widths should reflect the longest cell
            assert dt.column_widths[0] >= len("Bartholomew")

    @pytest.mark.asyncio
    async def test_mismatched_rows_skipped(self, cli_args):
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test():
            buf = app.buffers[0]
            buf.add_logs(["name,age,city", "Alice,30,NYC", "BadRow", "Bob,25,SF"])
            # Mismatched row should be skipped
            assert len(buf.displayed_rows) == 2

    @pytest.mark.asyncio
    async def test_raw_rows_accumulate(self, cli_args):
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test():
            buf = app.buffers[0]
            buf.add_logs(["name,age", "Alice,30"])
            assert len(buf.raw_rows) == 1
            buf.add_logs(["Bob,25"])
            assert len(buf.raw_rows) == 2


class TestLineStreamIntegration:
    """Test data arriving through a LineStream."""

    @pytest.mark.asyncio
    async def test_notify_delivers_to_buffer(self, cli_args):
        stream = LineStream()
        app = NlessApp(cli_args=cli_args, starting_stream=stream)
        async with app.run_test(size=(120, 40)) as pilot:
            buf = app.buffers[0]
            # Wait for buffer to be mounted and ready
            await _wait(pilot, app)

            stream.notify(["name,age,city", "Alice,30,NYC", "Bob,25,SF"])
            await _wait(pilot, app)

            assert buf.first_row_parsed
            assert len(buf.displayed_rows) == 2

    @pytest.mark.asyncio
    async def test_multiple_notifies(self, cli_args):
        stream = LineStream()
        app = NlessApp(cli_args=cli_args, starting_stream=stream)
        async with app.run_test(size=(120, 40)) as pilot:
            await _wait(pilot, app)

            stream.notify(["name,age,city", "Alice,30,NYC"])
            await _wait(pilot, app)

            stream.notify(["Bob,25,SF"])
            await _wait(pilot, app)

            buf = app.buffers[0]
            assert len(buf.displayed_rows) == 2
