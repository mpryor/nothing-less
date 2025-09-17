import sys
from typing import Optional
from threading import Thread

from textual.app import App, ComposeResult
from textual.widgets import DataTable
from textual import on
from typing import List, Dict, Optional
import select
import shlex


class NlessApp(App):
    """A modern pager with tabular data sorting/filtering capabilities."""
    BINDINGS = [
        ("q", "quit", "Quit"),
        ("f", "filter", "Filter"),
        ("s", "sort", "Sort"),
        ("up", "cursor_up", ""),
        ("down", "cursor_down", ""),
    ]
    
    def __init__(self):
        super().__init__()
        self.lines: List[List[str]] = []  # Store rows as lists of strings
        self.headers: List[str] = []
        self.sort_column: Optional[int] = None
        self.sort_reverse: bool = False
        self.filter_query: Optional[str] = None

    def compose(self) -> ComposeResult:
        """Create and yield the DataTable widget."""
        self.table = DataTable()
        self.setup_table()
        yield self.table

    def setup_table(self) -> None:
        """Initialize table columns."""
        self.table.zebra_stripes = True
        self.table.cursor_type = "row"
        if self.headers:
            self.table.add_columns(*self.headers)

    def on_mount(self) -> None:
        """Set up periodic updates when the app is mounted."""
        self.set_interval(.1, self.update)

    def update(self) -> None:
        """Update the table with new/filtered/sorted data."""
        try:
            # Process any new lines
            while self.lines:
                row = self.lines.pop(0)
                if not self.headers and len(row) > 1:  # Auto-detect headers
                    self.headers = row
                    self.setup_table()
                    continue
                self.table.add_row(*row)
            
            # Update existing rows incrementally
            current_row_count = self.table.row_count
            new_rows = self.get_filtered_sorted_rows()
            
            # Add new rows if needed
            for row in new_rows[current_row_count:]:
                self.table.add_row(*row)
            
            # Remove extra rows if needed
            while self.table.row_count > len(new_rows):
                self.table.remove_row(self.table.row_count - 1)
                
        except Exception as e:
            self.exit(message=f"Error: {e}")

    def get_filtered_sorted_rows(self) -> List[List[str]]:
        """Return processed rows based on current filters/sorts."""
        rows = [self.table.get_row_at(i) for i in range(self.table.row_count)]
        
        # Filtering
        if self.filter_query:
            rows = [row for row in rows
                    if any(self.filter_query in str(cell) for cell in row)]
            
        # Sorting
        if self.sort_column is not None:
            rows.sort(key=lambda x: x[self.sort_column], 
                     reverse=self.sort_reverse)
            
        return rows

    def add_log(self, log_line: str) -> None:
        """Parse and add a tabular data line."""
        # Try to detect delimiter
        delimiters = [',', '\t', '|', ';']
        if any(d in log_line for d in delimiters):
            delimiter = max(delimiters, key=lambda d: log_line.count(d))
            self.lines.append([cell.strip() for cell in log_line.split(delimiter)])
        else:  # Space-delimited
            self.lines.append(shlex.split(log_line))


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
        if line.startswith(':'):
            cmd, *args = line[1:].split(maxsplit=1)
            if cmd == 'sort':
                self.app.sort_column = int(args[0]) if args else None
            elif cmd == 'filter':
                self.app.filter_query = args[0] if args else None
            elif cmd == 'quit':
                self.app.exit()
            # Add more commands as needed
        else:
            self.app.add_log(line)


if __name__ == "__main__":
    app = NlessApp()
    t = Thread(target=InputConsumer(app).run, daemon=True)
    t.start()
    app.run()
