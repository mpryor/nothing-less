import time

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


class TestShellCommandLineStream:
    def test_simple_command(self):
        received = []
        stream = ShellCommandLineStream("echo hello")
        stream.subscribe(
            subscriber=self,
            add_lines_func=lambda lines: received.extend(lines),
            is_ready_func=lambda: True,
        )
        # Give the background thread time to run
        time.sleep(0.5)
        assert any("hello" in line for line in received)
