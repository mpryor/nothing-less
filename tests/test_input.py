import os
import time
from threading import Thread
from unittest.mock import patch

from nless.input import LineStream, ShellCommandLineStream, StdinLineStream
from nless.types import CliArgs


class TestLineStream:
    def test_subscribe_and_notify(self):
        stream = LineStream()
        received = []
        stream.subscribe(
            subscriber=self,
            add_lines_func=lambda lines: received.extend(lines),
            is_ready_func=lambda: True,
        )
        stream.notify(["line1", "line2"])
        assert received == ["line1", "line2"]

    def test_unsubscribe(self):
        stream = LineStream()
        received = []
        stream.subscribe(
            subscriber=self,
            add_lines_func=lambda lines: received.extend(lines),
            is_ready_func=lambda: True,
        )
        stream.unsubscribe(self)
        stream.notify(["should_not_appear"])
        assert received == []

    def test_lines_accumulation(self):
        stream = LineStream()
        stream.notify(["a", "b"])
        stream.notify(["c"])
        assert stream.lines == ["a", "b", "c"]


class TestStdinLineStreamParsing:
    def _make_stream(self):
        cli_args = CliArgs(delimiter=None, filters=[], unique_keys=set(), sort_by=None)
        # Create a minimal instance without actual fd
        stream = LineStream.__new__(StdinLineStream)
        LineStream.__init__(stream)
        stream.delimiter = cli_args.delimiter
        return stream

    def test_parse_complete_lines(self):
        stream = self._make_stream()
        lines, leftover = stream.parse_streaming_line("line1\nline2\n")
        assert lines == ["line1", "line2"]
        assert leftover == ""

    def test_parse_incomplete_line(self):
        stream = self._make_stream()
        lines, leftover = stream.parse_streaming_line("line1\npartial")
        assert lines == ["line1"]
        assert leftover == "partial"

    def test_parse_multiple_lines_trailing_partial(self):
        stream = self._make_stream()
        lines, leftover = stream.parse_streaming_line("a\nb\nc")
        assert lines == ["a", "b"]
        assert leftover == "c"

    def test_handle_input_non_json_passthrough(self):
        stream = self._make_stream()
        received = []
        stream.subscribe(
            subscriber=self,
            add_lines_func=lambda lines: received.extend(lines),
            is_ready_func=lambda: True,
        )
        stream.handle_input(["hello", "world"])
        assert received == ["hello", "world"]

    def test_handle_input_json_line_by_line(self):
        stream = self._make_stream()
        stream.delimiter = "json"
        received = []
        stream.subscribe(
            subscriber=self,
            add_lines_func=lambda lines: received.extend(lines),
            is_ready_func=lambda: True,
        )
        stream.handle_input(['{"a":1}', '{"a":2}'])
        assert received == ['{"a":1}', '{"a":2}']

    def test_handle_input_json_coalescence(self):
        stream = self._make_stream()
        stream.delimiter = "json"
        received = []
        stream.subscribe(
            subscriber=self,
            add_lines_func=lambda lines: received.extend(lines),
            is_ready_func=lambda: True,
        )
        # Multi-line JSON that needs coalescence
        stream.handle_input(["{", '"a": 1', "}"])
        assert len(received) == 1
        assert '"a"' in received[0]


class TestStdinLineStreamPipe:
    """Test StdinLineStream with a real pipe to verify streaming I/O."""

    def _make_pipe_stream(self, delimiter=None):
        r_fd, w_fd = os.pipe()
        cli_args = CliArgs(
            delimiter=delimiter, filters=[], unique_keys=set(), sort_by=None
        )
        stream = StdinLineStream(cli_args, None, r_fd)
        return stream, w_fd

    def test_streaming_lines_arrive(self):
        """Lines written to a pipe are delivered to subscribers."""
        stream, w_fd = self._make_pipe_stream()
        received = []
        stream.subscribe(self, lambda lines: received.extend(lines), lambda: True)

        t = Thread(target=stream.run, daemon=True)
        t.start()

        os.write(w_fd, b"line1\n")
        time.sleep(0.6)
        os.write(w_fd, b"line2\n")
        time.sleep(0.6)
        os.close(w_fd)

        assert any("line1" in line for line in received)
        assert any("line2" in line for line in received)

    def test_read_survives_type_error(self):
        """I/O thread keeps reading after TypeError from non-blocking read.

        Non-blocking stdin.read() can return None when no data is available,
        causing the codec layer to raise TypeError. The read loop must catch
        this and continue instead of crashing the I/O thread.
        """
        stream, w_fd = self._make_pipe_stream()
        received = []
        stream.subscribe(self, lambda lines: received.extend(lines), lambda: True)

        real_fdopen = os.fdopen
        read_call_count = 0

        def fdopen_with_flaky_read(fd, *args, **kwargs):
            f = real_fdopen(fd, *args, **kwargs)
            original_read = f.read

            def flaky_read(*a, **kw):
                nonlocal read_call_count
                read_call_count += 1
                # Simulate the codec TypeError on the 3rd read call
                if read_call_count == 3:
                    raise TypeError("can't concat NoneType to bytes")
                return original_read(*a, **kw)

            f.read = flaky_read
            return f

        with patch("nless.input.os.fdopen", side_effect=fdopen_with_flaky_read):
            t = Thread(target=stream.run, daemon=True)
            t.start()

            os.write(w_fd, b"before_error\n")
            time.sleep(0.6)
            os.write(w_fd, b"after_error\n")
            time.sleep(0.6)
            os.close(w_fd)

        assert any("before_error" in line for line in received)
        assert any("after_error" in line for line in received)


class TestShellCommandLineStream:
    def test_simple_command(self):
        received = []
        stream = ShellCommandLineStream("echo hello")
        stream.subscribe(
            subscriber=self,
            add_lines_func=lambda lines: received.extend(lines),
            is_ready_func=lambda: True,
        )
        stream.start()
        # Give the background thread time to run
        time.sleep(0.5)
        assert any("hello" in line for line in received)
