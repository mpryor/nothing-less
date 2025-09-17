import argparse
import bisect
from errno import EILSEQ
from io import StringIO, TextIOWrapper
import select
from multiprocessing import Process
import os
import stat
import sys
import re
import csv
import threading
import time
import fcntl
from typing import Callable, Optional
from threading import Thread
from rich.markup import _parse

from rich.text import Text
from textual.app import App, ComposeResult
from textual.containers import Vertical
from textual.coordinate import Coordinate
from textual.events import Key
from textual.widgets import DataTable, Input, Static
from textual.screen import Screen
from typing import List

from .nlesstable import NlessDataTable
from .input import InputConsumer


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
    .bottom-input {
        dock: bottom;
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

    scroll_x = 100
    scroll_y = 500

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
        ("D", "delimiter", "Change Delimiter"),
        ("s", "sort", "Sort selected column"),
        ("f", "filter", "Filter selected column (by prompt)"),
        ("|", "filter_any", "Filter any column (by prompt)"),
        ("F", "filter_cursor_word", "Filter selected column by word under cursor"),
        ("/", "search", "Search (all columns, by prompt)"),
        ("&", "search_to_filter", "Apply current search as filter"),
        ("n", "next_search", "Next search result"),
        ("p,N", "previous_search", "Previous search result"),
        ("*", "search_cursor_word", "Search (all columns) for word under cursor"),
        ("?", "push_screen('HelpScreen')", "Show Help"),
    ]

    def __init__(self):
        super().__init__()
        self.mounted = False
        self.data_initalized = False
        self.first_row_parsed = False
        self.raw_rows = []
        self.displayed_rows = []
        self.raw_header = ""
        self.current_filter = None
        self.filter_column = None
        self.search_term = None
        self.sort_index = None
        self.sort_reverse = False
        self.search_matches: List[Coordinate] = []
        self.current_match_index: int = -1
        self.delimiter = None
        self.delimiter_inferred = False

    def handle_search_submitted(self, event: Input.Submitted) -> None:
        input_value = event.value
        event.input.remove()
        self._perform_search(input_value)

    def _update_table(self, restore_position: bool = True) -> None:
        """Completely refreshes the table, repopulating it with the raw backing data, applying all sorts, filters, delimiters, etc."""
        data_table = self.query_one(NlessDataTable)
        curr_columns = (data_table.columns).copy()
        cursor_x = data_table.cursor_column
        cursor_y = data_table.cursor_row
        scroll_x = data_table.scroll_x
        scroll_y = data_table.scroll_y

        data_table.clear(columns=True)
        data_table.add_columns(*[v.label for k, v in curr_columns.items()])
        column_count = len(data_table.columns)
        self.search_matches = []
        self.current_match_index = -1

        # 1. Filter rows
        filtered_rows = []
        rows_with_inconsistent_length = []
        if self.current_filter:
            for row_str in self.raw_rows:
                cells = self._split_line(row_str)
                if len(cells) != column_count:
                    rows_with_inconsistent_length.append(cells)
                    continue
                if (
                    self.filter_column is None
                ):  # If we have a current_filter, but filter_column is None, we are searching all columns
                    if any(
                        self.current_filter.search(
                            self.get_cell_value_without_markup(cell)
                        )
                        for cell in cells
                    ):
                        filtered_rows.append(row_str)
                elif self.filter_column < len(cells) and self.current_filter.search(
                    self.get_cell_value_without_markup(cells[self.filter_column])
                ):
                    # Filter specific column
                    filtered_rows.append(row_str)
        else:
            for row in self.raw_rows:
                cells = self._split_line(row)
                if len(cells) == column_count:
                    filtered_rows.append(row)
                else:
                    rows_with_inconsistent_length.append(row)

        # 2. Sort rows
        if self.sort_index is not None:
            try:
                filtered_rows.sort(
                    key=lambda r: self._split_line(r)[self.sort_index],
                    reverse=self.sort_reverse,
                )
            except (ValueError, IndexError):
                # Fallback if column not found or row is malformed
                pass

        final_rows = []

        # 3. Add to table and find search matches
        if self.search_term:
            for displayed_row_idx, row_str in enumerate(filtered_rows):
                cells = self._split_line(row_str)
                highlighted_cells = []
                for col_idx, cell in enumerate(cells):
                    if isinstance(
                        self.search_term, re.Pattern
                    ) and self.search_term.search(cell):
                        cell = re.sub(
                            self.search_term,
                            lambda m: f"[reverse]{m.group(0)}[/reverse]",
                            cell,
                        )
                        highlighted_cells.append(cell)
                        self.search_matches.append(
                            Coordinate(displayed_row_idx, col_idx)
                        )
                    else:
                        highlighted_cells.append(cell)

                final_rows.append(highlighted_cells)
        else:
            for row_str in filtered_rows:
                cells = self._split_line(row_str)
                final_rows.append(cells)

        if len(rows_with_inconsistent_length) > 0:
            self.notify(
                f"{len(rows_with_inconsistent_length)} rows not matching columns, skipped. Use 'raw' delimiter (press D) to disable parsing.",
                severity="warning",
            )

        self.displayed_rows = final_rows
        data_table.add_rows(final_rows)
        if restore_position:
            self.call_after_refresh(
                lambda: self._restore_position(
                    data_table, cursor_x, cursor_y, scroll_x, scroll_y
                )
            )

    def _restore_position(self, data_table, cursor_x, cursor_y, scroll_x, scroll_y):
        data_table.move_cursor(
            row=cursor_y, column=cursor_x, animate=False, scroll=False
        )
        self.call_after_refresh(
            lambda: data_table.scroll_to(
                scroll_x, scroll_y, animate=False, immediate=True
            )
        )

    def on_data_table_cell_highlighted(
        self, event: NlessDataTable.CellHighlighted
    ) -> None:
        """Handle cell highlighted events to update the status bar."""
        self._update_status_bar()

    def _update_status_bar(self) -> None:
        status_bar = self.query_one("#status_bar", Static)
        data_table = self.query_one(NlessDataTable)

        total_rows = data_table.row_count
        total_cols = len(data_table.columns)
        current_row = data_table.cursor_row + 1  # Add 1 for 1-based indexing
        current_col = data_table.cursor_column + 1  # Add 1 for 1-based indexing

        sort_text = (
            f"[bold]Sort[/bold]: {str(data_table.ordered_columns[self.sort_index].label).strip()} {'desc' if self.sort_reverse else 'asc'}"
            if self.sort_index is not None
            else "[bold]Sort[/bold]: None"
        )
        if not self.current_filter:
            filter_text = "[bold]Filter[/bold]: None"
        elif self.filter_column is None:
            filter_text = (
                f"[bold]Filter[/bold]: Any Column='{self.current_filter.pattern}'"
            )
        else:
            filter_text = f"[bold]Filter[/bold]: {data_table.ordered_columns[self.filter_column].label}='{self.current_filter.pattern}'"

        search_text = (
            f"[bold]Search[/bold]: '{self.search_term.pattern}' ({self.current_match_index + 1} / {len(self.search_matches)} matches)"
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
                filter_value = re.escape(filter_value)
                self.current_filter = re.compile(filter_value, re.IGNORECASE)
                # Use None to indicate all-column filter
                self.filter_column = None
            except re.error:
                self.notify("Invalid regex pattern", severity="error")
                return

        self._update_table()

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
                print(
                    f"Filtering column {self.filter_column} with pattern: {self.current_filter.pattern}"
                )
            except re.error:
                self.notify("Invalid regex pattern", severity="error")
                return

        self._update_table()

    def _perform_search(self, search_term: Optional[str]) -> None:
        """Performs a search on the data and updates the table."""
        try:
            if search_term:
                self.search_term = re.compile(search_term, re.IGNORECASE)
            else:
                self.search_term = None
        except re.error:
            self.notify("Invalid regex pattern", severity="error")
            return
        self._update_table(restore_position=False)
        if self.search_matches:
            self._navigate_search(1)  # Jump to first match

    def handle_filter_submitted(self, event: Input.Submitted) -> None:
        filter_value = event.value
        event.input.remove()
        data_table = self.query_one(NlessDataTable)

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
        elif event.input.id == "delimiter_input":
            self.handle_delimiter_submitted(event)

    def handle_delimiter_submitted(self, event: Input.Submitted) -> None:
        self.current_filter = None
        self.filter_column = None
        self.search_term = None
        self.sort_index = None
        prev_delimiter = self.delimiter
        event.input.remove()
        data_table = self.query_one(NlessDataTable)
        self.delimiter_inferred = False
        delimiter = event.value
        if delimiter not in [
            ",",
            "\\t",
            " ",
            "  ",
            "|",
            ";",
            "raw",
        ]:  # if our delimiter is not one of the common ones, treat it as a regex
            try:
                pattern = re.compile(rf"{delimiter}")  # Validate regex
                self.delimiter = pattern
                data_table.clear(columns=True)
                data_table.add_columns(*list(pattern.groupindex.keys()))
                if prev_delimiter != "raw" and not isinstance(
                    prev_delimiter, re.Pattern
                ):
                    self.raw_rows.insert(0, self.raw_header)
                self._update_table()
                return
            except:
                self.notify("Invalid delimiter", severity="error")
                return

        if delimiter == "\\t":
            delimiter = "\t"

        self.delimiter = delimiter

        if delimiter == "raw":
            new_header = ["log"]
        elif prev_delimiter == "raw" or isinstance(prev_delimiter, re.Pattern):
            new_header = self._split_line(self.raw_rows[0])
            self.raw_rows.pop(0)
        else:
            new_header = self._split_line(self.raw_header)

        if (
            (prev_delimiter != delimiter)
            and (prev_delimiter != "raw" and not isinstance(prev_delimiter, re.Pattern))
            and (delimiter == "raw" or isinstance(delimiter, re.Pattern))
        ):
            self.raw_rows.insert(0, self.raw_header)

        data_table.clear(columns=True)
        data_table.add_columns(*new_header)
        self._update_table()

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
        data_table = self.query_one(NlessDataTable)
        data_table.cursor_coordinate = target_coord
        self._update_status_bar()

    def action_next_search(self) -> None:
        """Move cursor to the next search result."""
        self._navigate_search(1)

    def action_previous_search(self) -> None:
        """Move cursor to the previous search result."""
        self._navigate_search(-1)

    def get_cell_value_without_markup(self, cell_value) -> str:
        """Extract plain text from a cell value, removing any markup."""
        parsed_value = [*_parse(cell_value)]
        if len(parsed_value) > 1:
            return "".join([res[1] for res in parsed_value if res[1]])
        return cell_value

    def action_search_cursor_word(self) -> None:
        """Search for the word under the cursor."""
        data_table = self.query_one(NlessDataTable)
        coordinate = data_table.cursor_coordinate
        try:
            cell_value = data_table.get_cell_at(coordinate)
            cell_value = self.get_cell_value_without_markup(cell_value)
            cell_value = re.escape(cell_value)  # Validate regex
            self._perform_search(cell_value)
        except Exception:
            self.notify("Cannot get cell value.", severity="error")

    def action_delimiter(self) -> None:
        """Change the delimiter used for parsing."""
        input = Input(
            placeholder="Type delimiter character (e.g. ',', '\\t', ' ', '|') or 'raw' for no parsing",
            id="delimiter_input",
            classes="bottom-input",
        )
        self.mount(input)
        input.focus()

    def action_search(self) -> None:
        """Bring up search input to highlight matching text."""
        search_input = Input(
            placeholder="Type search term and press Enter",
            id="search_input",
            classes="bottom-input",
        )
        self.mount(search_input)
        search_input.focus()

    def compose(self) -> ComposeResult:
        """Create and yield the DataTable widget."""
        table = NlessDataTable(zebra_stripes=True, id="data_table")
        yield table
        with Vertical(id="bottom-container"):
            yield Static(
                "Sort: None | Filter: None | Search: None",
                classes="bd",
                id="status_bar",
            )

    def action_filter_cursor_word(self) -> None:
        """Filter by the word under the cursor."""
        data_table = self.query_one(NlessDataTable)
        coordinate = data_table.cursor_coordinate
        try:
            cell_value = data_table.get_cell_at(coordinate)
            cell_value = self.get_cell_value_without_markup(cell_value)
            cell_value = re.escape(cell_value)  # Validate regex
            self._perform_filter(f"^{cell_value}$", coordinate.column)
        except Exception:
            self.notify("Cannot get cell value.", severity="error")

    def action_sort(self) -> None:
        data_table = self.query_one(NlessDataTable)
        selected_column_index = data_table.cursor_column

        if self.sort_index == selected_column_index:
            self.sort_reverse = not self.sort_reverse
        else:
            self.sort_index = selected_column_index
            self.sort_reverse = False

        # Update column labels with sort indicators
        for index, column in enumerate(data_table.columns.values()):
            # Remove existing indicators
            label_text = str(column.label).strip(" ▲▼")
            if index == self.sort_index:
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
        data_table = self.query_one(NlessDataTable)
        input = Input(
            placeholder="Type filter text to match across all columns",
            id="filter_input_any",
            classes="bottom-input",
        )
        self.mount(input)
        input.focus()

    def action_filter(self) -> None:
        """Filter rows based on user input."""
        data_table = self.query_one(NlessDataTable)
        column_index = data_table.cursor_column
        column_label = data_table.ordered_columns[column_index].label
        input = Input(
            placeholder=f"Type filter text for column: {column_label} and press enter",
            id="filter_input",
            classes="bottom-input",
        )
        self.mount(input)
        input.focus()

    def action_cursor_up(self) -> None:
        """Move cursor up."""
        self.query_one(NlessDataTable).action_cursor_up()

    def action_cursor_down(self) -> None:
        """Move cursor down."""
        self.query_one(NlessDataTable).action_cursor_down()

    def action_cursor_left(self) -> None:
        """Move cursor left."""
        self.query_one(NlessDataTable).action_cursor_left()

    def action_cursor_right(self) -> None:
        """Move cursor left."""
        self.query_one(NlessDataTable).action_cursor_right()

    def action_scroll_to_bottom(self) -> None:
        """Scroll to top."""
        self.query_one(NlessDataTable).action_scroll_bottom()

    def action_scroll_to_top(self) -> None:
        """Scroll to top."""
        self.query_one(NlessDataTable).action_scroll_top()

    def action_scroll_to_end(self) -> None:
        """Move cursor to end of current row."""
        data_table = self.query_one(NlessDataTable)
        last_column = len(data_table.columns) - 1
        data_table.cursor_coordinate = data_table.cursor_coordinate._replace(
            column=last_column
        )

    def action_scroll_to_beginning(self) -> None:
        """Move cursor to beginning of current row."""
        data_table = self.query_one(NlessDataTable)
        data_table.cursor_coordinate = data_table.cursor_coordinate._replace(column=0)

    def action_page_up(self) -> None:
        """Page up."""
        data_table = self.query_one(NlessDataTable)
        data_table.action_page_up()

    def action_page_down(self) -> None:
        """Page down."""
        data_table = self.query_one(NlessDataTable)
        data_table.action_page_down()

    def on_mount(self) -> None:
        self.mounted = True

    def add_logs(self, log_lines: list[str]) -> None:
        data_table = self.query_one(NlessDataTable)

        # Infer delimiter from first few lines if not already set
        if not self.delimiter and len(log_lines) > 0:
            self.delimiter = self._infer_delimiter(log_lines[: min(5, len(log_lines))])
            self.delimiter_inferred = True

        if self.delimiter != "raw":
            if not self.first_row_parsed:
                first_log_line = log_lines[0]
                self.raw_header = first_log_line
                parts = self._split_line(first_log_line)
                data_table.add_columns(*parts)
                self.first_row_parsed = True
                log_lines = log_lines[1:]  # Exclude header line
        else:
            # No delimiter found, treat entire line as single column
            if not self.first_row_parsed:
                data_table.add_column("log")
                self.first_row_parsed = True

        self.raw_rows.extend(log_lines)
        if not self.data_initalized:
            self.data_initalized = True
            self._update_table()
        else:
            for line in log_lines:
                self._add_log_line(line)

    def _add_log_line(self, log_line: str):
        data_table = self.query_one(NlessDataTable)
        cells = self._split_line(log_line)
        if len(cells) != len(data_table.columns):
            return

        if self.current_filter:
            matches = False
            if self.filter_column is None:
                # We're filtering any column
                matches = any(
                    self.current_filter.search(self.get_cell_value_without_markup(cell))
                    for cell in cells
                )
            else:
                matches = self.filter_column < len(
                    cells
                ) and self.current_filter.search(
                    self.get_cell_value_without_markup(cells[self.filter_column])
                )
            if not matches:
                return

        if self.sort_index is not None:
            sort_key = cells[self.sort_index]
            displayed_row_keys = [r[self.sort_index] for r in self.displayed_rows]
            displayed_row_keys.sort(reverse=self.sort_reverse)
            new_index = bisect.bisect_left(displayed_row_keys, sort_key)
        else:
            new_index = len(self.displayed_rows)

        if self.search_term:
            highlighted_cells = []
            for col_idx, cell in enumerate(cells):
                if isinstance(
                    self.search_term, re.Pattern
                ) and self.search_term.search(cell):
                    cell = re.sub(
                        self.search_term,
                        lambda m: f"[reverse]{m.group(0)}[/reverse]",
                        cell,
                    )
                    highlighted_cells.append(cell)
                    self.search_matches.append(
                        Coordinate(new_index, col_idx)
                    )
                else:
                    highlighted_cells.append(cell)
            cells = highlighted_cells

        self.displayed_rows.append(cells)
        data_table.add_row_at(*cells, row_index=new_index)

    def _split_line(self, line: str) -> list[str]:
        """Split a line using the appropriate delimiter method.

        Args:
            line: The input line to split

        Returns:
            List of fields from the line
        """
        if self.delimiter == " ":
            cells = self._split_aligned_row(line)
        elif self.delimiter == "  ":
            cells = self._split_aligned_row_preserve_single_spaces(line)
        elif self.delimiter == ",":
            cells = self._split_csv_row(line)
        elif self.delimiter == "raw":
            cells = [line]
        elif isinstance(self.delimiter, re.Pattern):
            match = self.delimiter.match(line)
            if match:
                cells = [*match.groups()]
            else:
                cells = []
        else:
            cells = line.split(self.delimiter)
        cells = [txt.replace("\t", "  ") for txt in cells]
        cells = [
            f"[#aaaaaa]{cell}[/#aaaaaa]" if i % 2 != 0 else cell
            for (i, cell) in enumerate(cells)
        ]
        return cells

    def _split_aligned_row_preserve_single_spaces(self, line: str) -> list[str]:
        """Split a space-aligned row into fields by collapsing multiple spaces, but preserving single spaces within fields.

        Args:
            line: The input line to split

        Returns:
            List of fields from the line
        """
        # Use regex to split on two or more spaces
        return [field for field in re.split(r" {2,}", line) if field]

    def _split_aligned_row(self, line: str) -> list[str]:
        """Split a space-aligned row into fields by collapsing multiple spaces.

        Args:
            line: The input line to split

        Returns:
            List of fields from the line
        """
        # Split on multiple spaces and filter out empty strings
        return [field for field in line.split() if field]

    def _split_csv_row(self, line: str) -> list[str]:
        """Split a CSV row properly handling quoted values.

        Args:
            line: The input line to split

        Returns:
            List of fields from the line
        """
        try:
            # Use csv module to properly parse the line
            reader = csv.reader(StringIO(line.strip()))
            row = next(reader)
            return row
        except (csv.Error, StopIteration):
            # Fallback to simple split if CSV parsing fails
            return line.split(",")

    def _infer_delimiter(self, sample_lines: list[str]) -> str | None:
        """Infer the delimiter from a sample of lines.

        Args:
            sample_lines: A list of strings to analyze for delimiter detection.

        Returns:
            The most likely delimiter character.
        """
        common_delimiters = [",", "\t", "|", ";", " ", "  "]
        delimiter_scores = {d: 0 for d in common_delimiters}

        for line in sample_lines:
            # Skip empty lines
            if not line.strip():
                continue

            for delimiter in common_delimiters:
                if delimiter == " ":
                    # Special handling for space-aligned tables
                    parts = self._split_aligned_row(line)
                elif delimiter == "  ":
                    parts = self._split_aligned_row_preserve_single_spaces(line)
                elif delimiter == ",":
                    parts = self._split_csv_row(line)
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

        # Default to comma if no clear winner
        if not delimiter_scores or max(delimiter_scores.values()) == 0:
            return "raw"

        # Return the delimiter with the highest score
        return max(delimiter_scores.items(), key=lambda x: x[1])[0]

def main():
    app = NlessApp()
    new_fd = sys.stdin.fileno()

    parser = argparse.ArgumentParser(description="Test InputConsumer with stdin.")
    parser.add_argument("filename", nargs="?", help="File to read input from (defaults to stdin)")
    args = parser.parse_args()

    if args.filename:
        with open(args.filename, "r") as f:
            ic = InputConsumer(args.filename, None, lambda: app.mounted, lambda lines: app.add_logs(lines))
            t = Thread(target=ic.run, daemon=True)
            t.start()
    else:
        ic = InputConsumer(None, new_fd, lambda: app.mounted, lambda lines: app.add_logs(lines))
        t = Thread(target=ic.run, daemon=True)
        t.start()

    sys.__stdin__ = open("/dev/tty")
    app.run()

if __name__ == "__main__":
    main()
