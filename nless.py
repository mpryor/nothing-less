import sys
from typing import Optional
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
        ("f", "filter", "Filter"),
        ("s", "sort", "Sort"),
        ("up", "cursor_up", "Up"),
        ("down", "cursor_down", "Down"),
    ]

    def __init__(self):
        super().__init__()
        self.raw_rows: List[List[str]] = []  # Store all data rows (list of strings)
        self.headers: List[str] = []
        self.sort_column: Optional[int] = None
        self.sort_reverse: bool = False
        self.filter_query: Optional[str] = None

    def compose(self) -> ComposeResult:
        """Create and yield the DataTable widget."""
        self.table = DataTable()
        self.setup_table()
        yield self.table
        yield Footer()

    def setup_table(self) -> None:
        """Initialize table columns."""
        self.table.zebra_stripes = True
        self.table.cursor_type = "row"

    def action_quit(self) -> None:
        """Handle quit action."""
        self.exit()

    def action_filter(self) -> None:
        """Handle filter action."""
        self.notify("Filter: Type filter text in input", timeout=3)
        # You would add actual filter input handling here

    def action_sort(self) -> None:
        """Handle sort action."""
        self.notify("Sort: Select column with cursor", timeout=3)
        # You would add actual sort column selection here

    def action_cursor_up(self) -> None:
        """Move cursor up."""
        self.table.action_cursor_up()

    def on_ready(self) -> None:
        """Start the input consumer thread after the app is ready."""
        t = Thread(target=InputConsumer(self).run, daemon=True)
        t.start()
        self.consumer_thread = t  # Keep reference to prevent GC

    def action_cursor_down(self) -> None:
        """Move cursor down."""
        self.table.action_cursor_down()

    def get_filtered_sorted_rows(self) -> List[List[str]]:
        """Return processed rows based on current filters/sorts."""
        rows = self.raw_rows[:]   # working on a copy

        # Filtering
        if self.filter_query:
            rows = [
                row
                for row in rows
                if any(self.filter_query in str(cell) for cell in row)
            ]

        # Sorting
        if self.sort_column is not None:
            try:
                rows.sort(key=lambda x: x[self.sort_column] if self.sort_column < len(x) else "", reverse=self.sort_reverse)
            except Exception:
                pass   # If sorting fails, skip

        return rows

    def refresh_table(self) -> None:
        """Refresh the table with the current raw_rows and filter/sort."""
        if not self.raw_rows and not self.headers:
            return   # No data

        rows = self.get_filtered_sorted_rows()
        
        self.table.clear()
        if self.headers:
            self.table.add_columns(*self.headers)
        for row in rows:
            self.table.add_row(*row)

    def add_log(self, log_line: str) -> None:
        """Parse and add a tabular data line."""
        # Try to detect delimiter
        delimiters = [",", "\t", "|", ";"]
        if any(d in log_line for d in delimiters):
            delimiter = max(delimiters, key=lambda d: log_line.count(d))
            row = [cell.strip() for cell in log_line.split(delimiter)]
        else:  # Space-delimited
            row = shlex.split(log_line)

        if not self.headers and len(row) > 1:
            self.headers = row
            self.call_from_thread(self.refresh_table)
        else:
            self.raw_rows.append(row)
            self.call_from_thread(self.refresh_table)


class InputConsumer:
    """Handles stdin input and command processing."""

    def __init__(self, app: NlessApp):
        self.app = app
        self.running = True

    def run(self) -> None:
        """Read input and handle commands."""
        while self.running:
            # Use select to allow non-blocking input
            rlist, _, _ = select.select([sys.stdin], [], [], 0.1)
            if rlist:
                line = sys.stdin.readline()
                if not line:  # EOF
                    self.running = False
                    break
                self.handle_input(line.strip())

    def handle_input(self, line: str) -> None:
        """Process commands or data lines."""
        if line.startswith(":"):
            cmd, *args = line[1:].split(maxsplit=1)
            if cmd == "sort":
                self.app.sort_column = int(args[0]) if args else None
                self.app.call_from_thread(self.app.refresh_table)
            elif cmd == "filter":
                self.app.filter_query = args[0] if args else None
                self.app.call_from_thread(self.app.refresh_table)
            elif cmd == "quit":
                self.app.call_from_thread(self.app.exit)
            # Add more commands as needed
        else:
            self.app.add_log(line)


if __name__ == "__main__":
    app = NlessApp()
    app.run()
