"""Tests for streaming data into the app via add_logs and _add_rows_incremental."""

import os
import threading
import time

import pytest

from nless.app import NlessApp
from nless.input import LineStream, StdinLineStream
from nless.types import CliArgs


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
            assert buf.delim.value == ","
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
            assert buf.delim.value == "\t"
            assert len(buf.displayed_rows) == 1
            assert buf.displayed_rows[0] == ["Alice", "30", "NYC"]

    @pytest.mark.asyncio
    async def test_pipe_delimiter(self, cli_args):
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test():
            buf = app.buffers[0]
            buf.add_logs(["name|age|city", "Alice|30|NYC"])
            assert buf.delim.value == "|"
            assert len(buf.displayed_rows) == 1

    @pytest.mark.asyncio
    async def test_explicit_csv_delimiter(self, csv_cli_args):
        app = NlessApp(cli_args=csv_cli_args, starting_stream=None)
        async with app.run_test():
            buf = app.buffers[0]
            buf.add_logs(["name,age,city", "Alice,30,NYC"])
            assert buf.delim.value == ","
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
            assert not buf.loading_state.reason

    @pytest.mark.asyncio
    async def test_loading_flag_not_set_for_small_batch(self, cli_args):
        app = NlessApp(cli_args=cli_args, starting_stream=None)
        async with app.run_test():
            buf = app.buffers[0]
            buf.add_logs(["a,b,c", "1,2,3"])
            assert not buf.loading_state.reason


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

    @pytest.mark.asyncio
    async def test_delimiter_inference_with_header_and_data(self, cli_args):
        """Delimiter inference works correctly when header + data arrive together.

        The input-level first-batch accumulation in StdinLineStream.run()
        guarantees that subscribers always see header + data lines in the
        same batch, so infer_delimiter can use cross-line consistency
        checks to pick the right delimiter.
        """
        stream = LineStream()
        app = NlessApp(cli_args=cli_args, starting_stream=stream)
        async with app.run_test(size=(120, 40)) as pilot:
            await _wait(pilot, app)
            buf = app.buffers[0]

            # Header + data arrive together (as guaranteed by input-level batching)
            stream.notify(["name,age,city", "Alice,30,NYC"])
            await _wait(pilot, app)

            assert buf.delim.value == ","
            assert buf.first_row_parsed
            assert len(buf.displayed_rows) == 1

            # Subsequent lines work normally
            stream.notify(["Bob,25,SF"])
            await _wait(pilot, app)

            assert len(buf.displayed_rows) == 2


class TestPipeDelimiterInference:
    """Test delimiter inference via real pipes (reproduces slow-streaming bug)."""

    CSV_LINES = [
        "id,name,city,description,status\n",
        '1,"Smith, John","New York City","Works in finance dept",active\n',
        '2,"Doe, Jane","San Francisco","Leads the engineering team here",active\n',
        '3,"Brown, Bob","Los Angeles","Senior VP of operations dept",inactive\n',
        '4,"Wilson, Carol","Chicago","Managing director for sales",active\n',
        '5,"Davis, Eve","Seattle","Principal engineer in platform",active\n',
    ]

    def _run_pipe_test(self, write_delay: float) -> str:
        """Feed CSV lines through a real pipe and return the inferred delimiter."""
        r_fd, w_fd = os.pipe()
        cli_args = CliArgs(delimiter=None, filters=[], unique_keys=set(), sort_by=None)
        stream = StdinLineStream(cli_args, file_name=None, new_fd=r_fd)

        # Track the delimiter from the first subscriber callback
        results = []
        ready = threading.Event()

        def on_lines(lines):
            from nless.delimiter import infer_delimiter

            results.append(infer_delimiter(lines))

        stream.subscribe("test", on_lines, lambda: ready.is_set())

        def writer():
            for line in self.CSV_LINES:
                os.write(w_fd, line.encode())
                if write_delay:
                    time.sleep(write_delay)
            os.close(w_fd)

        w_thread = threading.Thread(target=writer, daemon=True)
        w_thread.start()
        io_thread = threading.Thread(target=stream.run, daemon=True)
        io_thread.start()

        # Simulate app startup delay (subscriber becomes ready after I/O starts)
        time.sleep(0.3)
        ready.set()

        w_thread.join(timeout=5)
        io_thread.join(timeout=5)
        time.sleep(0.1)

        assert results, "No subscriber callbacks received"
        return results[0]

    def test_pipe_no_delay(self):
        """Lines piped all at once should infer comma, not space."""
        assert self._run_pipe_test(0.0) == ","

    def test_pipe_slow_streaming(self):
        """Lines arriving one at a time (like while-read-sleep) should infer comma."""
        assert self._run_pipe_test(0.05) == ","

    def test_csv_with_spaces_in_data_infers_comma(self):
        """CSV where data has more spaces than commas must still infer comma.

        This is the root cause of the slow-streaming bug: data lines with
        quoted fields containing spaces (e.g. city names, descriptions)
        have more spaces than commas, causing infer_delimiter to pick space
        when the header (which has zero spaces) isn't weighted properly.
        """
        from nless.delimiter import infer_delimiter

        lines = [line.rstrip("\n") for line in self.CSV_LINES]
        # All lines together — must infer comma even though data has more spaces
        assert infer_delimiter(lines) == ","
        # Just header — must infer comma
        assert infer_delimiter(lines[:1]) == ","
        # Header + 1 data line — must infer comma
        assert infer_delimiter(lines[:2]) == ","
