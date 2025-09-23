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
        ("up", "cursor_up", "Up"),
        ("down", "cursor_down", "Down"),
    ]

    def __init__(self):
        super().__init__()
        self.mounted = False
        self.raw_rows = []

    def compose(self) -> ComposeResult:
        """Create and yield the DataTable widget."""
        table = DataTable()
        table.add_columns("Log")
        yield table
        yield Footer()

    def action_cursor_up(self) -> None:
        """Move cursor up."""
        self.query_one(DataTable).action_cursor_up()

    def on_mount(self) -> None:
        self.mounted = True

    def action_cursor_down(self) -> None:
        """Move cursor down."""
        self.query_one(DataTable).action_cursor_down()

    def add_log(self, log_line: str) -> None:
        if self.mounted:
            data_table = self.query_one(DataTable)
            data_table.add_row(log_line)
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
