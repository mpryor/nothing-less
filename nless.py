import sys
from typing import Optional
from threading import Thread
from rich.markup import _parse

from textual.app import App, ComposeResult
from textual.containers import Horizontal, Vertical
from textual.coordinate import Coordinate
from textual.events import Key
from textual.theme import BUILTIN_THEMES
from textual.widgets import DataTable, Footer, Input, Static
from textual.screen import Screen
from typing import List


class HelpScreen(Screen):
    """A widget to display keybindings help."""
    BINDINGS = [("escape", "app.pop_screen", "Close Help")]

    def compose(self) -> ComposeResult:
        bindings = self.app.BINDINGS
        help_text = "Keybindings:\n\n"
        for binding in bindings:
            keys, _, description = binding
            help_text += f"{keys:<12} - {description}\n"
        yield Static(help_text)
        yield Static("[bold]Press 'Escape' to close this help.[/bold]", id="help-footer")

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

    SCREENS = { "HelpScreen": HelpScreen }

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
                    c.key for c in data_table.ordered_columns
                ].index(self.sort_key)
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

        # Restore cursor column position
        if current_column is not None:
            data_table.cursor_coordinate = data_table.cursor_coordinate._replace(column=min(current_column, len(data_table.columns) - 1))

        self._update_status_bar()

    def _update_status_bar(self) -> None:
        status_bar = self.query_one("#status_bar", Static)
        data_table = self.query_one(DataTable)
        sort_text = f"[bold]Sort[/bold]: {data_table.columns[self.sort_key].label.strip()} {'desc' if self.sort_reverse else 'asc'}" if self.sort_key else "[bold]Sort[/bold]: None"
        filter_text = f"[bold]Filter[/bold]: {data_table.ordered_columns[self.filter_column].label}='{self.current_filter}'" if self.current_filter else "[bold]Filter[/bold]: None"
        search_text = f"[bold]Search[/bold]: '{self.search_term}' ({self.current_match_index + 1} / {len(self.search_matches)} matches)" if self.search_term else "[bold]Search[/bold]: None"
        status_bar.update(f"{sort_text} | {filter_text} | {search_text}")

    def _perform_filter(
        self, filter_value: Optional[str], column_index: Optional[int]
    ) -> None:
        """Performs a filter on the data and updates the table."""
        if not filter_value:
            self.current_filter = None
            self.filter_column = None
        else:
            self.current_filter = filter_value
            self.filter_column = column_index if column_index is not None else 0
            data_table = self.query_one(DataTable)
            column_label = data_table.ordered_columns[self.filter_column].label

        self._update_table()

    def _perform_search(self, search_term: Optional[str]) -> None:
        """Performs a search on the data and updates the table."""
        self.search_term = search_term.lower() if search_term else None
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
        ) % num_matches # Wrap around
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
            yield Static("Sort: None | Filter: None | Search: None", classes="bd", id="status_bar")

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

        if not self.first_row_parsed:
            first_log_line = log_lines[0]
            data_table.add_columns(*first_log_line.split(","))
            self.first_row_parsed = True
            log_lines = log_lines[1:]

        for log_line in log_lines:
            self.raw_rows.append(log_line)
            data_table.add_row(*log_line.split(","))
        self._update_table()

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
