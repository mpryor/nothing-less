from concurrent.futures import ThreadPoolExecutor
import fcntl
import json
import os
import select
import stat
import subprocess
from threading import Thread
import time
from typing import IO, Any, Callable, Tuple

from nless.types import CliArgs

AddLinesCallback = Callable[[list[str]], None]
IsReadyCallback = Callable[[], bool]


class LineStream:
    def __init__(self):
        self.subscribers = []
        self.lines = []

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
        tpe = ThreadPoolExecutor(max_workers=1)
        tpe.submit(
            self._initial_notify, is_ready_func, add_lines_func, self.lines.copy()
        )

    def unsubscribe(self, subscriber: Any) -> None:
        self.subscribers = [s for s in self.subscribers if s[0] != subscriber]

    def notify(self, lines: list[str]) -> None:
        self.lines.extend(lines)
        for subscriber, is_ready, callback in self.subscribers:
            while not is_ready():
                time.sleep(0.1)
            callback(lines)


class ShellCommmandLineStream(LineStream):
    def __init__(self, command: str):
        super().__init__()
        result = subprocess.Popen(
            command, stdout=subprocess.PIPE, shell=True, text=True
        )
        Thread(target=self._setup_io_stream, args=(result.stdout,), daemon=True).start()

    def _setup_io_stream(self, io: IO[str]) -> None:
        while line := io.readline():
            self.notify([line])


class StdinLineStream(LineStream):
    """Handles stdin input and command processing."""

    def __init__(
        self,
        cli_args: CliArgs,
        file_name: str | None,
        new_fd: int | None,
    ):
        super().__init__()
        if file_name is not None:
            file_name = os.path.expanduser(file_name)
            self.file = open(file_name, "r+", errors="ignore")
            self.new_fd = self.file.fileno()
        elif new_fd is not None:
            self.new_fd = new_fd

        self.delimiter = cli_args.delimiter

    def is_streaming(self) -> bool:
        # Returns True if stdin is a pipe (streaming), False if it's a regular file
        mode = os.fstat(self.new_fd).st_mode
        return stat.S_ISFIFO(mode)

    def run(self) -> None:
        """Read input and handle commands."""
        streaming = self.is_streaming()
        stdin = os.fdopen(self.new_fd, errors="ignore")
        fl = fcntl.fcntl(self.new_fd, fcntl.F_GETFL)
        fcntl.fcntl(self.new_fd, fcntl.F_SETFL, fl | os.O_NONBLOCK)
        buffer = ""
        TIMEOUT = 0.5
        FLUSH_INTERVAL_MS = 20
        last_read_time = time.time_ns() / 1_000_000  # - FLUSH_INTERVAL_MS

        while True:
            if streaming:
                current_time = time.time_ns() / 1_000_000
                if buffer:
                    if current_time - last_read_time >= FLUSH_INTERVAL_MS:
                        lines, leftover = self.parse_streaming_line(buffer)
                        self.handle_input(lines)
                        buffer = leftover
                        last_read_time = current_time
                file_readable, _, _ = select.select([stdin], [], [], TIMEOUT)
                if file_readable:
                    while True:
                        try:
                            line = stdin.read()
                            if not line:
                                break
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
                        except Exception:
                            break
            else:
                lines = stdin.readlines()
                if len(lines) > 0:
                    self.handle_input(lines)
                else:
                    time.sleep(1)

    def parse_streaming_line(self, line: str) -> Tuple[list[str], str]:
        lines = line.split("\n")
        if line.endswith("\n"):
            return lines[:-1], ""
        else:
            return lines[:-1], lines[-1]

    def handle_input(self, lines: list[str]) -> None:
        if lines:
            if self.delimiter == "json":
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
