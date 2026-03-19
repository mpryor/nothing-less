import array
import fcntl
import io
import json
import logging
import os
import select
import stat
import subprocess
import termios
from threading import Thread
import time
from typing import IO, Any, Callable

from nless.types import CliArgs

logger = logging.getLogger(__name__)

# Minimum milliseconds between buffer flushes during streaming.
# Controls UI update frequency vs CPU overhead tradeoff.
FLUSH_INTERVAL_MS = 20

# Maximum milliseconds a partial line is held before forced flush.
# Prevents stale data display on slow streams.
MAX_BUFFER_HOLD_MS = 200

# Maximum bytes buffered before forced flush regardless of line boundaries.
# Prevents unbounded memory growth on line-free binary streams.
MAX_BUFFER_SIZE = 1_000_000

AddLinesCallback = Callable[[list[str]], None]
IsReadyCallback = Callable[[], bool]


class LineStream:
    def __init__(self):
        self.subscribers = []
        self.lines = []
        self.done = False

    def _initial_notify(
        self,
        is_ready_func: IsReadyCallback,
        add_lines_func: AddLinesCallback,
        init_lines: list[str],
    ) -> None:
        while not is_ready_func():
            time.sleep(0.1)
        if len(init_lines) > 0:
            add_lines_func(init_lines)

    def subscribe(
        self,
        subscriber: Any,
        add_lines_func: AddLinesCallback,
        is_ready_func: IsReadyCallback,
    ) -> None:
        self.subscribers.append((subscriber, is_ready_func, add_lines_func))
        thread = Thread(
            target=self._initial_notify,
            args=(is_ready_func, add_lines_func, self.lines.copy()),
            daemon=True,
        )
        thread.start()

    def subscribe_future_only(
        self,
        subscriber: Any,
        add_lines_func: AddLinesCallback,
        is_ready_func: IsReadyCallback,
    ) -> None:
        """Register for future lines without replaying history."""
        self.subscribers.append((subscriber, is_ready_func, add_lines_func))

    def unsubscribe(self, subscriber: Any) -> None:
        self.subscribers = [s for s in self.subscribers if s[0] != subscriber]

    def notify(self, lines: list[str]) -> None:
        self.lines.extend(lines)
        for subscriber, is_ready, callback in list(self.subscribers):
            while not is_ready():
                time.sleep(0.1)
            try:
                callback(lines)
            except Exception:
                logger.exception("Error in subscriber callback for %r", subscriber)


class ShellCommandLineStream(LineStream):
    def __init__(self, command: str):
        super().__init__()
        self._command = command
        self._process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=True,
            text=True,
        )

    def start(self) -> None:
        """Start reading from the subprocess. Call after subscribers are set up."""
        Thread(
            target=self._setup_io_stream, args=(self._process.stdout,), daemon=True
        ).start()

    def _setup_io_stream(self, io: IO[str]) -> None:
        # Buffer the first batch so delimiter inference / log format detection
        # has enough lines to work with (single-line notify causes mis-inference).
        initial_batch: list[str] = []
        deadline = time.time() + 0.5
        while time.time() < deadline:
            line = io.readline()
            if not line:
                break
            initial_batch.append(line)
            if len(initial_batch) >= 15:
                break
        if initial_batch:
            self.notify(initial_batch)

        while line := io.readline():
            self.notify([line])
        self.done = True


class MergedLineStream(LineStream):
    """Wraps multiple LineStream children into a single logical stream."""

    def __init__(self, children: list[LineStream]):
        super().__init__()
        self._children = children

    @property
    def lines(self) -> list[str]:  # type: ignore[override]
        all_lines: list[str] = []
        for child in self._children:
            all_lines.extend(child.lines)
        return all_lines

    @lines.setter
    def lines(self, value: list[str]) -> None:
        pass  # controlled by children

    @property
    def done(self) -> bool:  # type: ignore[override]
        return all(c.done for c in self._children)

    @done.setter
    def done(self, value: bool) -> None:
        pass  # controlled by children

    def is_streaming(self) -> bool:
        return any(
            hasattr(c, "is_streaming") and c.is_streaming() for c in self._children
        )

    def run(self) -> None:
        # Each child stream runs on its own thread; nothing to do here.
        pass


class StdinLineStream(LineStream):
    """Handles stdin input and command processing."""

    def __init__(
        self,
        cli_args: CliArgs,
        file_name: str | None,
        new_fd: int | None,
    ):
        super().__init__()
        self._cli_args = cli_args
        self._opened_file = None
        if file_name is not None:
            file_name = os.path.expanduser(file_name)
            try:
                self._opened_file = open(file_name, "r+", errors="ignore")  # noqa: SIM115
            except (io.UnsupportedOperation, OSError):
                # Pipes/FIFOs (e.g. process substitution) aren't seekable
                self._opened_file = open(file_name, "r", errors="ignore")  # noqa: SIM115
            self.new_fd = self._opened_file.fileno()
        elif new_fd is not None:
            self.new_fd = new_fd

        self.delimiter = cli_args.delimiter

    def is_streaming(self) -> bool:
        # Returns True if stdin is a pipe (streaming), False if it's a regular file
        mode = os.fstat(self.new_fd).st_mode
        return stat.S_ISFIFO(mode)

    def pipe_pending_bytes(self) -> int | None:
        """Return bytes waiting in the OS pipe buffer, or None if unavailable."""
        try:
            buf = array.array("i", [0])
            fcntl.ioctl(self.new_fd, termios.FIONREAD, buf)
            return buf[0]
        except (OSError, ValueError):
            return None

    def run(self) -> None:
        """Read input and handle commands."""
        streaming = self.is_streaming()
        stdin = os.fdopen(self.new_fd, errors="ignore")
        fl = fcntl.fcntl(self.new_fd, fcntl.F_GETFL)
        fcntl.fcntl(self.new_fd, fcntl.F_SETFL, fl | os.O_NONBLOCK)
        buffer = ""
        TIMEOUT = 0.5
        last_read_time = time.time_ns() / 1_000_000  # - FLUSH_INTERVAL_MS
        buffer_start_time = 0.0

        try:
            while True:
                if streaming:
                    current_time = time.time_ns() / 1_000_000
                    if buffer:
                        elapsed_since_read = current_time - last_read_time
                        elapsed_since_start = current_time - buffer_start_time
                        should_flush = (
                            elapsed_since_read >= FLUSH_INTERVAL_MS
                            or elapsed_since_start >= MAX_BUFFER_HOLD_MS
                            or len(buffer) >= MAX_BUFFER_SIZE
                        )
                        if should_flush:
                            lines, leftover = self.parse_streaming_line(buffer)
                            self.handle_input(lines)
                            buffer = leftover
                            last_read_time = current_time
                            if leftover:
                                buffer_start_time = current_time
                            else:
                                buffer_start_time = 0.0
                    file_readable, _, _ = select.select([stdin], [], [], TIMEOUT)
                    if file_readable:
                        got_data = False
                        hit_eof = False
                        while True:
                            try:
                                line = stdin.read()
                                if not line:
                                    if not got_data:
                                        hit_eof = True
                                    break
                                got_data = True
                                if not buffer:
                                    buffer_start_time = current_time
                                buffer += line
                                last_read_time = current_time
                                if self.delimiter != "json":
                                    # If we're reading json - we assume we need to coalesce multiple lines
                                    #   to account for multi-line json objects during initial read
                                    #   This *could* cause a lock if streaming json objects faster than the FLUSH_INTERVAL_MS
                                    # Otherwise, we can process line-by-line
                                    lines, leftover = self.parse_streaming_line(buffer)
                                    self.handle_input(lines)
                                    buffer = leftover
                            except (OSError, IOError, ValueError, TypeError):
                                break
                        if hit_eof:
                            # select said readable but read got nothing = EOF
                            if buffer:
                                lines, _ = self.parse_streaming_line(buffer)
                                self.handle_input(lines)
                                buffer = ""
                            self.done = True
                            break
                else:
                    lines = stdin.readlines()
                    if len(lines) > 0:
                        self.handle_input(lines)
                    else:
                        time.sleep(1)
        finally:
            stdin.close()
            if self._opened_file is not None:
                self._opened_file.close()

    def parse_streaming_line(self, line: str) -> tuple[list[str], str]:
        lines = line.split("\n")
        if line.endswith("\n"):
            return lines[:-1], ""
        else:
            return lines[:-1], lines[-1]

    def handle_input(self, lines: list[str]) -> None:
        if lines:
            if self.delimiter == "json" or (
                not self.delimiter and self._looks_like_json(lines)
            ):
                try:
                    json.loads(
                        lines[0]
                    )  # determine if we have a series of json strings, or if we have one json file
                    self.notify(lines)
                except json.JSONDecodeError:
                    try:
                        parsed_json = json.loads("".join(lines))
                        if isinstance(parsed_json, list):
                            self.notify([json.dumps(item) for item in parsed_json])
                        else:
                            self.notify([json.dumps(parsed_json)])
                    except json.JSONDecodeError:
                        self.notify(lines)
            else:
                self.notify(lines)

    @staticmethod
    def _looks_like_json(lines: list[str]) -> bool:
        """Check if lines look like JSON (array or line-delimited objects)."""
        stripped = [line.strip() for line in lines if line.strip()]
        if not stripped:
            return False
        if stripped[0].startswith("["):
            return True
        if stripped[0].startswith("{"):
            try:
                json.loads(stripped[0].rstrip(","))
                return True
            except (json.JSONDecodeError, ValueError):
                pass
        return False
