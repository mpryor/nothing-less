import sys
from typing import Optional, override
from threading import Thread

from textual.app import App, ComposeResult
from textual.widgets import DataTable, Footer, Input
from typing import List
import select
import shlex


class NlessApp(App):
    """A modern pager with tabular data sorting/filtering capabilities."""

    ENABLE_COMMAND_PALETTE = False
    CSS = """
    #filter_input {
        dock: bottom;
        visibility: visible;
        height: 3;
    }
    """

    BINDINGS = [
        ("q", "quit", "Quit"),
        ("G", "scroll_to_bottom", "Scroll to Bottom"),
        ("g", "scroll_to_top", "Scroll to Top"),
        ("d", "page_down", "Page Down"),
        ("u", "page_up", "Page up"),
        ("up,k", "cursor_up", "Up"),
        ("down,j", "cursor_down", "Down"),
        ("l,w,W", "cursor_right", "Right"),
        ("h,b,B", "cursor_left", "Left"),
        ("s", "sort", "Sort"),
        ("f", "filter", "Filter"),
        ("$", "scroll_to_end", "End of Line"),
        ("0", "scroll_to_beginning", "Start of Line"),
    ]

    def __init__(self):
        super().__init__()
        self.mounted = False
        self.first_row_parsed = False
        self.raw_rows = []
        self.current_filter = None
        self.filter_column = None
        self.sort_ascending = True

    def compose(self) -> ComposeResult:
        """Create and yield the DataTable widget."""
        table = DataTable()
        yield table
        yield Footer()


    def on_input_submitted(self, event: Input.Submitted) -> None:
        filter_value = event.value
        event.input.remove()
        if not filter_value:
            self.current_filter = None
            self.filter_column = None
            data_table = self.query_one(DataTable)
            data_table.clear()
            for row in self.raw_rows:
                data_table.add_row(*row.split(","))
            self.notify("Filter cleared")
            return

        self.current_filter = filter_value

        data_table = self.query_one(DataTable)
        current_column = data_table.cursor_column
        column_index = current_column if current_column is not None else 0
        self.filter_column = column_index

        # Clear the table but keep the columns
        columns = data_table.columns.values()
        data_table.clear()

        # Filter and re-add matching rows
        for row in self.raw_rows:
            cells = row.split(",")
            if (
                column_index < len(cells)
                and filter_value.lower() in cells[column_index].lower()
            ):
                data_table.add_row(*cells)
        self.notify(f"Filtered by '{filter_value}' in column {data_table.ordered_columns[column_index].label}")

    def action_sort(self) -> None:
        data_table = self.query_one(DataTable)
        selected_column = data_table.ordered_columns[data_table.cursor_column]
        data_table.sort(selected_column.key, reverse=not self.sort_ascending)
        self.sort_ascending = not self.sort_ascending
        self.notify(f"Sorted by {selected_column.label} {'descending' if self.sort_ascending else 'ascending'}")

    def action_filter(self) -> None:
        """Filter rows based on user input."""
        input = Input(placeholder="Type filter and press Enter", id="filter_input")
        self.mount(input)
        input.focus()

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

    def action_scroll_to_end(self) -> None:
        """Move cursor to end of current row."""
        data_table = self.query_one(DataTable)
        last_column = len(data_table.columns) - 1
        data_table.cursor_coordinate = data_table.cursor_coordinate._replace(column=last_column)

    def action_scroll_to_beginning(self) -> None:
        """Move cursor to beginning of current row."""
        data_table = self.query_one(DataTable)
        data_table.cursor_coordinate = data_table.cursor_coordinate._replace(column=0)

    def action_page_up(self) -> None:
        """Page up."""
        data_table = self.query_one(DataTable)
        data_table.action_page_up()

    def action_page_down(self) -> None:
        """Page down."""
        data_table = self.query_one(DataTable)
        data_table.action_page_down()

    def on_mount(self) -> None:
        self.mounted = True

    def add_log(self, log_line: str) -> None:
        if self.mounted:
            data_table = self.query_one(DataTable)

            if not self.first_row_parsed:
                # columns = [f"col{i}" for i in range(1, len(log_line.split(",")) + 1)]
                # data_table.add_columns(*columns)
                data_table.add_columns(*log_line.split(","))
                self.first_row_parsed = True
                return

            # Always add to raw_rows
            self.raw_rows.append(log_line)

            # Only add to display if no filter is active or if it matches the current filter
            if self.current_filter:
                cells = log_line.split(",")
                if (
                    self.filter_column < len(cells)
                    and self.current_filter.lower() in cells[self.filter_column].lower()
                ):
                    data_table.add_row(*log_line.split(","))
            else:
                data_table.add_row(*log_line.split(","))


class InputConsumer:
    """Handles stdin input and command processing."""

    def __init__(self, app: NlessApp):
        self.app = app

    def run(self) -> None:
        """Read input and handle commands."""
        while True:
            if self.app.mounted:
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
