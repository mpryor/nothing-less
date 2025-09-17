import sys
from typing import Optional
from threading import Thread
from rich.markup import _parse

from textual.app import App, ComposeResult
from textual.coordinate import Coordinate
from textual.widgets import DataTable, Footer, Input
from typing import List


class NlessApp(App):
    """A modern pager with tabular data sorting/filtering capabilities."""

    ENABLE_COMMAND_PALETTE = False
    CSS = """
    #filter_input {
        dock: bottom;
        visibility: visible;
        height: 3;
    }
    #search_input {
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
        ("/", "search", "Search"),
        ("$", "scroll_to_end", "End of Line"),
        ("0", "scroll_to_beginning", "Start of Line"),
        ("n", "next_search", "Next Search Result"),
        ("*", "search_cursor_word", "Search for word under cursor"),
        ("p,N", "previous_search", "Previous Search Result"),
        ("F", "filter_cursor_word", "Filter by word under cursor"),
    ]

    def __init__(self):
        super().__init__()
        self.mounted = False
        self.first_row_parsed = False
        self.raw_rows = []
        self.current_filter = None
        self.filter_column = None
        self.search_term = None
        self.sort_key = None
        self.sort_reverse = False
        self.search_matches: List[Coordinate] = []
        self.current_match_index: int = -1

    def handle_search_submitted(self, event: Input.Submitted) -> None:
        input_value = event.value
        event.input.remove()
        self._perform_search(input_value)

    def _update_table(self) -> None:
        """Updates the table based on the current filter and search terms."""
        data_table = self.query_one(DataTable)
        data_table.clear()
        self.search_matches = []
        self.current_match_index = -1

        # 1. Filter rows
        filtered_rows = []
        if self.current_filter:
            for row_str in self.raw_rows:
                cells = row_str.split(",")
                if (
                    self.filter_column < len(cells)
                    and self.current_filter.lower() in cells[self.filter_column].lower()
                ):
                    filtered_rows.append(row_str)
        else:
            filtered_rows = self.raw_rows[:]

        # 2. Sort rows
        if self.sort_key:
            try:
                sort_column_index = [
                    c.key.value for c in data_table.ordered_columns
                ].index(self.sort_key.value)
                filtered_rows.sort(
                    key=lambda r: r.split(",")[sort_column_index], reverse=self.sort_reverse
                )
            except (ValueError, IndexError):
                # Fallback if column not found or row is malformed
                pass

        # 3. Add to table and find search matches
        for displayed_row_idx, row_str in enumerate(filtered_rows):
            cells = row_str.split(",")
            highlighted_cells = []
            for col_idx, cell in enumerate(cells):
                if self.search_term and self.search_term in cell.lower():
                    highlighted_cells.append(f"[reverse]{cell}[/reverse]")
                    self.search_matches.append(Coordinate(displayed_row_idx, col_idx))
                else:
                    highlighted_cells.append(cell)
            data_table.add_row(*highlighted_cells, key=str(displayed_row_idx))

    def _perform_filter(
        self, filter_value: Optional[str], column_index: Optional[int]
    ) -> None:
        """Performs a filter on the data and updates the table."""
        if not filter_value:
            self.current_filter = None
            self.filter_column = None
            self.notify("Filter cleared")
        else:
            self.current_filter = filter_value
            self.filter_column = column_index if column_index is not None else 0
            data_table = self.query_one(DataTable)
            column_label = data_table.ordered_columns[self.filter_column].label
            self.notify(
                f"Filtered column {column_label} by filter text: '{self.current_filter}'"
            )

        self._update_table()

    def _perform_search(self, search_term: Optional[str]) -> None:
        """Performs a search on the data and updates the table."""
        self.search_term = search_term.lower() if search_term else None
        self.notify(
            "Search cleared" if not search_term else f"Searching for '{search_term}'"
        )
        self._update_table()
        if self.search_matches:
            self._navigate_search(1)  # Jump to first match

    def handle_filter_submitted(self, event: Input.Submitted) -> None:
        filter_value = event.value
        event.input.remove()
        data_table = self.query_one(DataTable)
        column_index = data_table.cursor_column
        self._perform_filter(filter_value, column_index)

    def on_input_submitted(self, event: Input.Submitted) -> None:
        if event.input.id == "search_input":
            self.handle_search_submitted(event)
        elif event.input.id == "filter_input":
            self.handle_filter_submitted(event)

    def _navigate_search(self, direction: int) -> None:
        """Navigate through search matches."""
        if not self.search_matches:
            self.notify("No search results.", severity="warning")
            return

        num_matches = len(self.search_matches)
        self.current_match_index = (
            self.current_match_index + direction + num_matches
        ) % num_matches # Wrap around
        target_coord = self.search_matches[self.current_match_index]
        data_table = self.query_one(DataTable)
        data_table.cursor_coordinate = target_coord
        self.notify(f"Match {self.current_match_index + 1} of {num_matches}", timeout=1)

    def action_next_search(self) -> None:
        """Move cursor to the next search result."""
        self._navigate_search(1)

    def action_previous_search(self) -> None:
        """Move cursor to the previous search result."""
        self._navigate_search(-1)

    def action_search_cursor_word(self) -> None:
        """Search for the word under the cursor."""
        data_table = self.query_one(DataTable)
        coordinate = data_table.cursor_coordinate
        try:
            cell_value = data_table.get_cell_at(coordinate)
            self._perform_search(cell_value)
        except Exception:
            self.notify("Cannot get cell value.", severity="error")

    def action_search(self) -> None:
        """Bring up search input to highlight matching text."""
        search_input = Input(
            placeholder="Type search term and press Enter", id="search_input"
        )
        setattr(search_input, "search_input", True)  # Mark this as search input
        self.mount(search_input)
        search_input.focus()

    def compose(self) -> ComposeResult:
        """Create and yield the DataTable widget."""
        table = DataTable()
        yield table
        yield Footer()

    def action_filter_cursor_word(self) -> None:
        """Filter by the word under the cursor."""
        data_table = self.query_one(DataTable)
        coordinate = data_table.cursor_coordinate
        try:
            cell_value = data_table.get_cell_at(coordinate)
            parsed_value = [*_parse(cell_value)]
            if len(parsed_value) > 1:
                cell_value = parsed_value[1][1]
            self._perform_filter(cell_value, coordinate.column)
        except Exception:
            self.notify("Cannot get cell value.", severity="error")

    def action_sort(self) -> None:
        data_table = self.query_one(DataTable)
        selected_column = data_table.ordered_columns[data_table.cursor_column]

        if self.sort_key == selected_column.key:
            self.sort_reverse = not self.sort_reverse
        else:
            self.sort_key = selected_column.key
            self.sort_reverse = False

        data_table.sort(self.sort_key, reverse=self.sort_reverse)
        self.notify(
            f"Sorted by {selected_column.label} {'descending' if self.sort_reverse else 'ascending'}"
        )

    def action_filter(self) -> None:
        """Filter rows based on user input."""
        data_table = self.query_one(DataTable)
        column_index = data_table.cursor_column
        column_label = data_table.ordered_columns[column_index].label
        input = Input(
            placeholder=f"Type filter text for column: {column_label} and press enter",
            id="filter_input",
        )
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
        data_table.cursor_coordinate = data_table.cursor_coordinate._replace(
            column=last_column
        )

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
