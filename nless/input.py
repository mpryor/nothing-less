import fcntl
import os
import select
import stat
import time
from typing import Callable, Tuple


class InputConsumer:
    """Handles stdin input and command processing."""

    def __init__(self, file_name: str | None, new_fd: int | None, output_ready_func: Callable[[], bool], output_func: Callable[[list[str]], None]):
        if file_name is not None:
            file_name = os.path.expanduser(file_name)
            self.file = open(file_name, "r+", errors="ignore")
            self.new_fd = self.file.fileno()
        elif new_fd is not None:
            self.new_fd = new_fd
        self.new_line_callback = output_func
        self.read_condition = output_ready_func

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
        raw_str = ""
        TIMEOUT = 0.5
        FLUSH_INTERVAL_MS = 1000
        last_flush_time = time.time_ns() / 1_000_000 - FLUSH_INTERVAL_MS

        while True:
            if self.read_condition():
                if streaming:
                    if raw_str:
                        current_time = time.time_ns() / 1_000_000
                        if current_time - last_flush_time >= FLUSH_INTERVAL_MS:
                            lines, leftover = self.parse_streaming_line(raw_str)
                            self.handle_input(lines)
                            raw_str = leftover
                            last_flush_time = current_time
                    file_readable, _, _ = select.select([stdin], [], [], TIMEOUT)
                    if file_readable:
                        while True:
                            try:
                                line = stdin.read()
                                if not line:
                                    break
                                raw_str += line
                                lines, leftover = self.parse_streaming_line(raw_str)
                                self.handle_input(lines)
                                raw_str = leftover
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
            self.new_line_callback(lines)

