import sys
from typing import Optional, override
from threading import Thread

from textual.app import App, ComposeResult
from textual.widgets import DataTable, Footer
from typing import List
import select
import shlex


class NlessApp(App):
    """A modern pager with tabular data sorting/filtering capabilities."""

    BINDINGS = [
        ("q", "quit", "Quit"),
        ("G", "scroll_to_bottom", "Scroll to Bottom"),
        ("g", "scroll_to_top", "Scroll to Top"),
        ("d", "page_down", "Page Down"),
        ("u", "page_up", "Page up"),
        ("up", "cursor_up", "Up"),
        ("k", "cursor_up", "Up"),
        ("down", "cursor_down", "Down"),
        ("j", "cursor_down", "Down"),
        ("l", "cursor_right", "Right"),
        ("h", "cursor_left", "Left"),
        ("f", "filter", "Filter"),
    ]

    def __init__(self):
        super().__init__()
        self.mounted = False
        self.first_row_parsed = False
        self.raw_rows = []

    def compose(self) -> ComposeResult:
        """Create and yield the DataTable widget."""
        table = DataTable()
        yield table
        yield Footer()

    def action_filter(self) -> None:
        """Filter rows based on user input."""


    def action_cursor_up(self) -> None:
        """Move cursor up."""
        self.query_one(DataTable).action_cursor_up()

    def action_cursor_down(self) -> None:
        """Move cursor down."""
        self.query_one(DataTable).action_cursor_down()

    def action_cursor_left(self) -> None:
        """Move cursor left."""
        self.query_one(DataTable).action_cursor_left()

    def action_cursor_right(self) -> None:
        """Move cursor left."""
        self.query_one(DataTable).action_cursor_right()

    def action_scroll_to_bottom(self) -> None:
        """Scroll to top."""
        self.query_one(DataTable).action_scroll_bottom()

    def action_scroll_to_top(self) -> None:
        """Scroll to top."""
        self.query_one(DataTable).action_scroll_top()

    def action_page_up(self) -> None:
        """Scroll to top."""
        data_table = self.query_one(DataTable)
        data_table.action_page_up()

    def action_page_down(self) -> None:
        """Scroll to top."""
        data_table = self.query_one(DataTable)
        data_table.action_page_down()

    def on_mount(self) -> None:
        self.mounted = True

    def add_log(self, log_line: str) -> None:
        if self.mounted:
            data_table = self.query_one(DataTable)

            if not self.first_row_parsed:
                columns = [f"col{i}" for i in range(1, len(log_line.split(",")) + 1)]
                data_table.add_columns(*columns)
                self.first_row_parsed = True

            data_table.add_row(*log_line.split(","))
            self.raw_rows.append(log_line)


class InputConsumer:
    """Handles stdin input and command processing."""

    def __init__(self, app: NlessApp):
        self.app = app
        self.running = True

    def run(self) -> None:
        """Read input and handle commands."""
        while self.running:
            rlist, _, _ = select.select([sys.stdin], [], [], 0.1)
            if rlist:
                line = sys.stdin.readline()
                if line:
                    self.handle_input(line.strip())

    def handle_input(self, line: str) -> None:
        self.app.add_log(line)


if __name__ == "__main__":
    app = NlessApp()
    ic = InputConsumer(app)
    t = Thread(target=ic.run, daemon=True)
    t.start()
    app.run()
