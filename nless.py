import sys
import re
from typing import Optional
from threading import Thread
from rich.markup import _parse

from textual.app import App, ComposeResult
from textual.containers import Vertical
from textual.coordinate import Coordinate
from textual.events import Key
from textual.widgets import DataTable, Input, Static
from textual.screen import Screen
from typing import List


class HelpScreen(Screen):
    """A widget to display keybindings help."""

    BINDINGS = [("escape", "app.pop_screen", "Close Help")]

    def compose(self) -> ComposeResult:
        bindings = self.app.BINDINGS
        help_text = "[bold]Keybindings[/bold]:\n\n"
        for binding in bindings:
            keys, _, description = binding
            help_text += f"{keys:<12} - {description}\n"
        yield Static(help_text)
        yield Static(
            "[bold]Press 'Escape' to close this help.[/bold]", id="help-footer"
        )


class NlessApp(App):
    """A modern pager with tabular data sorting/filtering capabilities."""

    ENABLE_COMMAND_PALETTE = False
    CSS = """
    #bottom-container {
        height:auto;
        dock:bottom;
        width: 100%;
    }
    #filter_input {
        dock: bottom;
        visibility: visible;
        height: 3;
    }
    #filter_input_any {
        dock: bottom;
        visibility: visible;
        height: 3;
    }
    #search_input {
        dock: bottom;
        visibility: visible;
        height: 3;
    }
    #help-screen {
        background: $surface;
        border: solid $primary;
        padding: 1;
        margin: 1;
        height: 80%;
        width: 80%;
        align: center middle;
    }
    """

    SCREENS = {"HelpScreen": HelpScreen}

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
        ("$", "scroll_to_end", "End of Line"),
        ("0", "scroll_to_beginning", "Start of Line"),
        ("s", "sort", "Sort selected column"),
        ("f", "filter", "Filter selected column (by prompt)"),
        ("F", "filter_cursor_word", "Filter selected column by word under cursor"),
        ("/", "search", "Search (all columns, by prompt)"),
        ("&", "search_to_filter", "Apply current search as filter"),
        ("?", "push_screen('HelpScreen')", "Show Help"),
        ("n", "next_search", "Next search result"),
        ("p,N", "previous_search", "Previous search result"),
        ("*", "search_cursor_word", "Search (all columns) for word under cursor"),
        ("?", "push_screen('HelpScreen')", "Show Help"),
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
        self.delimiter = None

    def handle_search_submitted(self, event: Input.Submitted) -> None:
        input_value = event.value
        event.input.remove()
        self._perform_search(input_value)

    def _update_table(self) -> None:
        """Updates the table based on the current filter and search terms."""
        data_table = self.query_one(DataTable)

        # Store current cursor position
        current_column = data_table.cursor_column

        data_table.clear()
        self.search_matches = []
        self.current_match_index = -1

        # 1. Filter rows
        filtered_rows = []
        if self.current_filter:
            for row_str in self.raw_rows:
                cells = self._split_line(row_str)
                if self.filter_column is None:
                    # Filter across all columns
                    if any(self.current_filter.search(cell) for cell in cells):
                        filtered_rows.append(row_str)
                elif self.filter_column < len(cells) and self.current_filter.search(
                    cells[self.filter_column]
                ):
                    # Filter specific column
                    filtered_rows.append(row_str)
        else:
            filtered_rows = self.raw_rows[:]

        # 2. Sort rows
        if self.sort_key:
            try:
                sort_column_index = [c.key for c in data_table.ordered_columns].index(
                    self.sort_key
                )
                filtered_rows.sort(
                    key=lambda r: self._split_line(r)[sort_column_index],
                    reverse=self.sort_reverse,
                )
            except (ValueError, IndexError):
                # Fallback if column not found or row is malformed
                pass

        # 3. Add to table and find search matches
        for displayed_row_idx, row_str in enumerate(filtered_rows):
            cells = self._split_line(row_str)
            highlighted_cells = []
            for col_idx, cell in enumerate(cells):
                if self.search_term and isinstance(self.search_term, re.Pattern):
                    if self.search_term.search(cell):
                        highlighted_cells.append(f"[reverse]{cell}[/reverse]")
                        self.search_matches.append(
                            Coordinate(displayed_row_idx, col_idx)
                        )
                    else:
                        highlighted_cells.append(cell)
                else:
                    highlighted_cells.append(cell)
            data_table.add_row(*highlighted_cells, key=str(displayed_row_idx))

        # Restore cursor column position
        if current_column is not None:
            data_table.cursor_coordinate = data_table.cursor_coordinate._replace(
                column=min(current_column, len(data_table.columns) - 1)
            )

        self._update_status_bar()

    def on_data_table_cell_highlighted(self, event: DataTable.CellHighlighted) -> None:
        """Handle cell highlighted events to update the status bar."""
        self._update_status_bar()

    def _update_status_bar(self) -> None:
        status_bar = self.query_one("#status_bar", Static)
        data_table = self.query_one(DataTable)

        total_rows = data_table.row_count
        total_cols = len(data_table.columns)
        current_row = data_table.cursor_row + 1  # Add 1 for 1-based indexing
        current_col = data_table.cursor_column + 1  # Add 1 for 1-based indexing

        sort_text = (
            f"[bold]Sort[/bold]: {data_table.columns[self.sort_key].label.strip()} {'desc' if self.sort_reverse else 'asc'}"
            if self.sort_key
            else "[bold]Sort[/bold]: None"
        )
        if not self.current_filter:
            filter_text = "[bold]Filter[/bold]: None"
        elif self.filter_column is None:
            filter_text = (
                f"[bold]Filter[/bold]: Any Column='{self.current_filter.pattern}'"
            )
        else:
            filter_text = f"[bold]Filter[/bold]: {data_table.ordered_columns[self.filter_column].label}='{self.current_filter}'"

        search_text = (
            f"[bold]Search[/bold]: '{self.search_term}' ({self.current_match_index + 1} / {len(self.search_matches)} matches)"
            if self.search_term
            else "[bold]Search[/bold]: None"
        )
        position_text = f"[bold]row[/bold]: {current_row}/{total_rows} [bold]col[/bold]: {current_col}/{total_cols}"

        status_bar.update(
            f"{sort_text} | {filter_text} | {search_text} | {position_text}"
        )

    def _perform_filter_any(self, filter_value: Optional[str]) -> None:
        """Performs a filter across all columns and updates the table."""
        if not filter_value:
            self.current_filter = None
            self.filter_column = None
        else:
            try:
                # Compile the regex pattern
                self.current_filter = re.compile(filter_value, re.IGNORECASE)
                # Use None to indicate all-column filter
                self.filter_column = None
            except re.error:
                self.notify("Invalid regex pattern", severity="error")
                return

        # Update filtering logic
        filtered_rows = []
        for row_str in self.raw_rows:
            cells = self._split_line(row_str)
            # Row matches if any cell matches the filter
            if not self.current_filter or any(
                self.current_filter.search(cell) for cell in cells
            ):
                filtered_rows.append(row_str)

        # Clear and rebuild table with filtered rows
        data_table = self.query_one(DataTable)
        data_table.clear()
        for row_str in filtered_rows:
            data_table.add_row(*self._split_line(row_str))

        self._update_status_bar()

    def _perform_filter(
        self, filter_value: Optional[str], column_index: Optional[int]
    ) -> None:
        """Performs a filter on the data and updates the table."""
        if not filter_value:
            self.current_filter = None
            self.filter_column = None
        else:
            try:
                # Compile the regex pattern
                self.current_filter = re.compile(filter_value, re.IGNORECASE)
                self.filter_column = column_index if column_index is not None else 0
                data_table = self.query_one(DataTable)
                column_label = data_table.ordered_columns[self.filter_column].label
            except re.error:
                self.notify("Invalid regex pattern", severity="error")
                return

        self._update_table()

    def _perform_search(self, search_term: Optional[str]) -> None:
        """Performs a search on the data and updates the table."""
        try:
            self.search_term = re.compile(search_term, re.IGNORECASE)
        except re.error:
            self.notify("Invalid regex pattern", severity="error")
            return
        self._update_table()
        if self.search_matches:
            self._navigate_search(1)  # Jump to first match

    def handle_filter_submitted(self, event: Input.Submitted) -> None:
        filter_value = event.value
        event.input.remove()
        data_table = self.query_one(DataTable)

        if event.input.id == "filter_input_any":
            self._perform_filter_any(filter_value)
        else:
            column_index = data_table.cursor_column
            self._perform_filter(filter_value, column_index)

    def on_input_submitted(self, event: Input.Submitted) -> None:
        if event.input.id == "search_input":
            self.handle_search_submitted(event)
        elif event.input.id == "filter_input" or event.input.id == "filter_input_any":
            self.handle_filter_submitted(event)

    def on_key(self, event: Key) -> None:
        """Handle key events."""
        if event.key == "escape" and isinstance(self.focused, Input):
            self.focused.remove()

    def _navigate_search(self, direction: int) -> None:
        """Navigate through search matches."""
        if not self.search_matches:
            self.notify("No search results.", severity="warning")
            return

        num_matches = len(self.search_matches)
        self.current_match_index = (
            self.current_match_index + direction + num_matches
        ) % num_matches  # Wrap around
        target_coord = self.search_matches[self.current_match_index]
        data_table = self.query_one(DataTable)
        data_table.cursor_coordinate = target_coord
        self._update_status_bar()

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
        table = DataTable(zebra_stripes=True, id="data_table")
        yield table
        with Vertical(id="bottom-container"):
            yield Static(
                "Sort: None | Filter: None | Search: None",
                classes="bd",
                id="status_bar",
            )

    def action_filter_cursor_word(self) -> None:
        """Filter by the word under the cursor."""
        data_table = self.query_one(DataTable)
        coordinate = data_table.cursor_coordinate
        try:
            cell_value = data_table.get_cell_at(coordinate)
            parsed_value = [*_parse(cell_value)]
            if len(parsed_value) > 1:
                cell_value = parsed_value[1][1]
            self._perform_filter(f"^{cell_value}$", coordinate.column)
        except Exception:
            self.notify("Cannot get cell value.", severity="error")

    def action_sort(self) -> None:
        data_table = self.query_one(DataTable)
        selected_column_key = data_table.ordered_columns[data_table.cursor_column].key

        if self.sort_key == selected_column_key:
            self.sort_reverse = not self.sort_reverse
        else:
            self.sort_key = selected_column_key
            self.sort_reverse = False

        # Update column labels with sort indicators
        for column in data_table.columns.values():
            # Remove existing indicators
            label_text = str(column.label).strip(" ▲▼")
            if column.key == self.sort_key:
                indicator = "▼" if self.sort_reverse else "▲"
                column.label = f"{label_text} {indicator}"
            else:
                column.label = label_text

        self._update_table()

    def action_search_to_filter(self) -> None:
        """Convert current search into a filter across all columns."""
        if not self.search_term:
            self.notify("No active search to convert to filter", severity="warning")
            return
            
        self.current_filter = self.search_term  # Reuse the compiled regex
        self.filter_column = None  # Filter across all columns
        self._update_table()

    def action_filter_any(self) -> None:
        """Filter any column based on user input."""
        data_table = self.query_one(DataTable)
        input = Input(
            placeholder="Type filter text to match across all columns",
            id="filter_input_any",
        )
        self.mount(input)
        input.focus()

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

    def add_logs(self, log_lines: list[str]) -> None:
        print(f"Adding {len(log_lines)} log lines", file=sys.stderr)
        data_table = self.query_one(DataTable)

        # Infer delimiter from first few lines if not already set
        if not self.delimiter and len(log_lines) > 0:
            self.delimiter = self._infer_delimiter(log_lines[: min(5, len(log_lines))])

        if self.delimiter != "n/a":
            if not self.first_row_parsed:
                first_log_line = log_lines[0]
                parts = self._split_line(first_log_line)
                data_table.add_columns(*parts)
                self.first_row_parsed = True
                log_lines = log_lines[1:]

            for log_line in log_lines:
                self.raw_rows.append(log_line)
                data_table.add_row(*self._split_line(log_line))
        else:
            # No delimiter found, treat entire line as single column
            if not self.first_row_parsed:
                data_table.add_column("log")
                self.first_row_parsed = True

            for log_line in log_lines:
                self.raw_rows.append(log_line)
                data_table.add_row(log_line)

        self._update_table()

    def _split_line(self, line: str) -> list[str]:
        """Split a line using the appropriate delimiter method.

        Args:
            line: The input line to split

        Returns:
            List of fields from the line
        """
        if self.delimiter == " ":
            return self._split_aligned_row(line)
        if self.delimiter == "n/a":
            return [line]
        return line.split(self.delimiter)

    def _split_aligned_row(self, line: str) -> list[str]:
        """Split a space-aligned row into fields by collapsing multiple spaces.

        Args:
            line: The input line to split

        Returns:
            List of fields from the line
        """
        # Split on multiple spaces and filter out empty strings
        return [field for field in line.split() if field]

    def _infer_delimiter(self, sample_lines: list[str]) -> str | None:
        """Infer the delimiter from a sample of lines.

        Args:
            sample_lines: A list of strings to analyze for delimiter detection.

        Returns:
            The most likely delimiter character.
        """
        common_delimiters = [",", "\t", "|", ";", " "]
        delimiter_scores = {d: 0 for d in common_delimiters}

        for line in sample_lines:
            # Skip empty lines
            if not line.strip():
                continue

            for delimiter in common_delimiters:
                if delimiter == " ":
                    # Special handling for space-aligned tables
                    parts = self._split_aligned_row(line)
                else:
                    parts = line.split(delimiter)

                # Score based on number of fields and consistency
                if len(parts) > 1:
                    # More fields = higher score
                    delimiter_scores[delimiter] += len(parts)

                    # Consistent non-empty fields = higher score
                    non_empty = sum(1 for p in parts if p.strip())
                    if non_empty == len(parts):
                        delimiter_scores[delimiter] += 2

                    # If fields are roughly similar lengths = higher score
                    lengths = [len(p.strip()) for p in parts]
                    avg_len = sum(lengths) / len(lengths)
                    if all(abs(l - avg_len) < avg_len for l in lengths):
                        delimiter_scores[delimiter] += 1

                    # Special case: if tab and consistent fields, boost score
                    if delimiter == "\t" and non_empty == len(parts):
                        delimiter_scores[delimiter] += 3

                    # Special case: if space delimiter and parts are consistent across lines
                    if delimiter == " " and len(sample_lines) > 1:
                        # Check if number of fields is consistent across lines
                        first_line_parts = self._split_aligned_row(sample_lines[0])
                        if len(parts) == len(first_line_parts):
                            delimiter_scores[delimiter] += 2
                        else:
                            delimiter_scores[delimiter] -= 20

        print("\n\n\n===========================================")
        print("Delimiter scores:", delimiter_scores)
        print("===========================================\n\n\n")
        # Default to comma if no clear winner
        if not delimiter_scores or max(delimiter_scores.values()) == 0:
            return "n/a"

        # Return the delimiter with the highest score
        return max(delimiter_scores.items(), key=lambda x: x[1])[0]


class InputConsumer:
    """Handles stdin input and command processing."""

    def __init__(self, app: NlessApp):
        self.app = app

    def run(self) -> None:
        """Read input and handle commands."""
        while True:
            if self.app.mounted:
                lines = sys.stdin.readlines()
                if len(lines) > 0:
                    self.handle_input(lines)

    def handle_input(self, line: list[str]) -> None:
        self.app.add_logs(line)


if __name__ == "__main__":
    app = NlessApp()
    ic = InputConsumer(app)
    t = Thread(target=ic.run, daemon=True)
    t.start()
    app.run()
