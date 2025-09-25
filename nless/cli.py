import argparse
import bisect
from copy import deepcopy
import csv
from dataclasses import dataclass
from collections import defaultdict
from threading import Thread
import time
from typing import Callable, List, Optional
from webbrowser import get
import os
import json
import re
import sys

from rich.markup import _parse
from rich.text import Text
from textual import on
from textual import events
from textual.app import App, ComposeResult
from textual.containers import Grid, Vertical
from textual.coordinate import Coordinate
from textual.events import Key
from textual.geometry import Offset
from textual.screen import ModalScreen, Screen
from textual.scroll_view import ScrollView
from textual.widgets import (
    DataTable,
    Input,
    RichLog,
    Select,
    Static,
    Tab,
    TabPane,
    TabbedContent,
    Static,
)

from nless.autocomplete import AutocompleteInput
from nless.version import get_version

from .delimiter import infer_delimiter, split_line
from .types import Column, Filter, MetadataColumn
from .help import HelpScreen
from .input import InputConsumer
from .nlesstable import NlessDataTable

from enum import Enum

class RowLengthMismatchError(Exception):
    pass

def write_buffer(current_buffer: "NlessBuffer", output_path: str) -> None:
    if output_path == "-":
        output_path = "/dev/stdout"
        while current_buffer.app.is_running:
            time.sleep(.1)
        time.sleep(.1)

    with open(output_path, "w") as f:
        writer = csv.writer(f)
        writer.writerow(current_buffer._get_visible_column_labels())
        for row in current_buffer.displayed_rows:
            plain_row = [current_buffer._get_cell_value_without_markup(str(cell)) for cell in row]
            writer.writerow(plain_row)


def write_buffer(current_buffer: "NlessBuffer", output_path: str) -> None:
    if output_path == "-":
        output_path = "/dev/stdout"
        while current_buffer.app.is_running:
            time.sleep(.1)
        time.sleep(.1)

    with open(output_path, "w") as f:
        writer = csv.writer(f)
        writer.writerow(current_buffer._get_visible_column_labels())
        for row in current_buffer.displayed_rows:
            plain_row = [current_buffer._get_cell_value_without_markup(str(cell)) for cell in row]
            writer.writerow(plain_row)


class UnparsedLogsScreen(Screen):
    BINDINGS = [("q", "app.pop_screen", "Close")]

    def __init__(self, unparsed_rows: List[str], delimiter: str):
        super().__init__()
        self.unparsed_rows = unparsed_rows
        self.delimiter = delimiter

    def compose(self) -> ComposeResult:
        yield Static(
            f"{len(self.unparsed_rows)} logs not matching columns (delimiter '{self.delimiter}'), press 'q' to close.",
        )
        rl = RichLog()
        for row in self.unparsed_rows:
            rl.write(row.strip())
        yield rl

class NlessBuffer(Static):
    """A modern pager with tabular data sorting/filtering capabilities."""

    ENABLE_COMMAND_PALETTE = False
    CSS_PATH = "nless.tcss"

    BINDINGS = [
        ("q", "quit", "Quit"),
        ("c", "jump_columns", "Jump to column (by select)"),
        (">", "move_column_right", "Move column right"),
        ("<", "move_column_left", "Move column left"),
        ("s", "sort", "Sort selected column"),
        ("n", "next_search", "Next search result"),
        ("p", "previous_search", "Previous search result"),
        ("*", "search_cursor_word", "Search (all columns) for word under cursor"),
        ("v", "change_cursor", "Change cursor type - row, column, cell"),
        ("~", "view_unparsed_logs", "View logs not matching delimiter"),
        (
            "t",
            "toggle_tail",
            "Keep cursor at the bottom of the screen even as new logs arrive.",
        ),
    ]

    def __init__(self, pane_id: int):
        super().__init__()
        self.locked = False
        self.pane_id: int = pane_id
        self.mounted = False
        self.first_row_parsed = False
        self.raw_rows = []
        self.displayed_rows = []
        self.first_log_line = ""  # used to determine columns when delimiter is set
        self.current_columns: list[Column] = []
        self.current_filters: List[Filter] = []
        self.search_term = None
        self.sort_column = None
        self.sort_reverse = False
        self.search_matches: List[Coordinate] = []
        self.current_match_index: int = -1
        self.delimiter = None
        self.delimiter_inferred = False
        self.is_tailing = False
        self.unique_column_names = set()
        self.count_by_column_key = defaultdict(lambda: 0)

    def copy(self, pane_id) -> "NlessBuffer":
        new_buffer = NlessBuffer(pane_id=pane_id)
        new_buffer.mounted = self.mounted
        new_buffer.first_row_parsed = self.first_row_parsed
        new_buffer.raw_rows = deepcopy(self.raw_rows)
        new_buffer.displayed_rows = deepcopy(self.displayed_rows)
        new_buffer.first_log_line = self.first_log_line
        new_buffer.current_columns = deepcopy(self.current_columns)
        new_buffer.current_filters = deepcopy(self.current_filters)
        new_buffer.search_term = self.search_term
        new_buffer.sort_column = self.sort_column
        new_buffer.sort_reverse = self.sort_reverse
        new_buffer.search_matches = deepcopy(self.search_matches)
        new_buffer.current_match_index = self.current_match_index
        new_buffer.delimiter = self.delimiter
        new_buffer.delimiter_inferred = self.delimiter_inferred
        new_buffer.is_tailing = self.is_tailing
        new_buffer.unique_column_names = deepcopy(self.unique_column_names)
        new_buffer.count_by_column_key = deepcopy(self.count_by_column_key)
        return new_buffer

    def compose(self) -> ComposeResult:
        """Create and yield the DataTable widget."""
        with Vertical():
            table = NlessDataTable(
                zebra_stripes=True, id="data_table", show_row_labels=True
            )
            yield table

    def on_mount(self) -> None:
        self.mounted = True

    def on_data_table_cell_highlighted(
        self, event: NlessDataTable.CellHighlighted
    ) -> None:
        """Handle cell highlighted events to update the status bar."""
        self._update_status_bar()

    def _open_select(self, select: Select) -> None:
        select.focus()
        select.expanded = True


    def action_view_unparsed_logs(self) -> None:
        """View logs that do not match the current delimiter."""
        if self.delimiter == "raw":
            self.notify("Delimiter is 'raw', all logs are being shown.", severity="info")
            return

        unparsed_rows = []
        expected_cell_count = len(
            [c for c in self.current_columns if not c.hidden]
        )
        for row in self.raw_rows:
            cells = split_line(row, self.delimiter, self.current_columns)
            if len(cells) != expected_cell_count:
                unparsed_rows.append(row)

        if len(unparsed_rows) == 0:
            self.notify("All logs match the current delimiter.", severity="info")
            return

        delimiter = self.delimiter

        self.app.push_screen(UnparsedLogsScreen(unparsed_rows, delimiter))


    def action_jump_columns(self) -> None:
        """Show columns by user input."""
        column_options = [
            (self._get_cell_value_without_markup(c.name), c.render_position)
            for c in self.current_columns
            if not c.hidden
        ]
        select = Select(
            options=column_options,
            classes="bottom-input",
            prompt="Type a column to jump to",
        )
        self.mount(select)
        self.call_after_refresh(lambda: self._open_select(select))

    def on_select_changed(self, event: Select.Changed) -> None:
        col_index = event.value
        event.control.remove()
        data_table = self.query_one(NlessDataTable)
        data_table.move_cursor(column=col_index)

    def action_move_column(self, direction: int) -> None:
        data_table = self.query_one(NlessDataTable)
        current_cursor_column = data_table.cursor_column
        selected_column = [
            c
            for c in self.current_columns
            if c.render_position == current_cursor_column
        ]
        if not selected_column:
            self.notify("No column selected to move", severity="error")
            return
        selected_column = selected_column[0]
        if selected_column.name in [m.value for m in MetadataColumn]:
            return  # can't move metadata columns
        if (
            direction == 1
            and selected_column.render_position == len(self.current_columns) - 1
        ) or (direction == -1 and selected_column.render_position == 0):
            return  # can't move further in that direction

        adjacent_column = [
            c
            for c in self.current_columns
            if c.render_position == selected_column.render_position + direction
        ]
        if not adjacent_column or adjacent_column[0].name in [
            m.value for m in MetadataColumn
        ]:  # can't move past metadata columns
            return
        adjacent_column = adjacent_column[0]

        if (
            adjacent_column.pinned
            and not selected_column.pinned
            or (selected_column.pinned and not adjacent_column.pinned)
        ):
            return  # can't move a pinned column past a non-pinned column or vice versa

        selected_column.render_position, adjacent_column.render_position = (
            adjacent_column.render_position,
            selected_column.render_position,
        )
        self._update_table()
        self.call_after_refresh(
            lambda: data_table.move_cursor(column=selected_column.render_position)
        )

    def action_move_column_left(self) -> None:
        self.action_move_column(-1)

    def action_move_column_right(self) -> None:
        self.action_move_column(1)

    def action_toggle_tail(self) -> None:
        self.is_tailing = not self.is_tailing
        self._update_status_bar()

    def action_change_cursor(self) -> None:
        data_table = self.query_one(NlessDataTable)
        if data_table.cursor_type == "cell":
            data_table.cursor_type = "column"
        elif data_table.cursor_type == "column":
            data_table.cursor_type = "row"
        else:
            data_table.cursor_type = "cell"

    def action_next_search(self) -> None:
        """Move cursor to the next search result."""
        self._navigate_search(1)

    def action_previous_search(self) -> None:
        """Move cursor to the previous search result."""
        self._navigate_search(-1)

    def action_search_cursor_word(self) -> None:
        """Search for the word under the cursor."""
        data_table = self.query_one(NlessDataTable)
        coordinate = data_table.cursor_coordinate
        try:
            cell_value = data_table.get_cell_at(coordinate)
            cell_value = self._get_cell_value_without_markup(cell_value)
            cell_value = re.escape(cell_value)  # Validate regex
            self._perform_search(cell_value)
        except Exception:
            self.notify("Cannot get cell value.", severity="error")

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

    def action_sort(self) -> None:
        data_table = self.query_one(NlessDataTable)
        current_cursor_column = data_table.cursor_column
        selected_column = [
            c
            for c in self.current_columns
            if c.render_position == current_cursor_column
        ]
        if not selected_column:
            self.notify("No column selected for sorting", severity="error")
            return
        else:
            selected_column = selected_column[0]

        new_sort_column_name = self._get_cell_value_without_markup(selected_column.name)

        if self.sort_column == new_sort_column_name and self.sort_reverse:
            self.sort_column = None
        elif self.sort_column == new_sort_column_name and not self.sort_reverse:
            self.sort_reverse = True
        else:
            self.sort_column = new_sort_column_name
            self.sort_reverse = False

        # Update sort indicators
        if self.sort_column is None:
            selected_column.labels.discard("▲")
            selected_column.labels.discard("▼")
        elif self.sort_reverse:
            selected_column.labels.discard("▲")
            selected_column.labels.add("▼")
        else:
            selected_column.labels.discard("▼")
            selected_column.labels.add("▲")

        # Remove sort indicators from other columns
        for col in self.current_columns:
            if col.name != selected_column.name:
                col.labels.discard("▲")
                col.labels.discard("▼")

        self._update_table()

    def _get_label(self, label: Text | str) -> str:
        if isinstance(label, Text):
            return label.plain
        else:
            return label

    def _get_visible_column_labels(self) -> List[str]:
        labels = []
        for col in sorted(self.current_columns, key=lambda c: c.render_position):
            if not col.hidden:
                labels.append(f"{col.name} {' '.join(col.labels)}".strip())
        return labels

    def _update_table(self, restore_position: bool = True) -> None:
        """Completely refreshes the table, repopulating it with the raw backing data, applying all sorts, filters, delimiters, etc."""
        self.locked = True
        data_table = self.query_one(NlessDataTable)
        cursor_x = data_table.cursor_column
        cursor_y = data_table.cursor_row
        scroll_x = data_table.scroll_x
        scroll_y = data_table.scroll_y

        curr_metadata_columns = {
            c.name
            for c in self.current_columns
            if c.name in [m.value for m in MetadataColumn]
        }
        expected_cell_count = len(self.current_columns) - len(curr_metadata_columns)
        data_table.clear(
            columns=True
        )  # might be needed to trigger column resizing with longer cell content

        data_table.fixed_columns = len(curr_metadata_columns)
        data_table.add_columns(*self._get_visible_column_labels())

        self.search_matches = []
        self.current_match_index = -1
        self.count_by_column_key = defaultdict(lambda: 0)

        # 1. Filter rows
        filtered_rows = []
        rows_with_inconsistent_length = []
        if len(self.current_filters) > 0:
            for row_str in self.raw_rows:
                cells = split_line(row_str, self.delimiter, self.current_columns)
                if len(cells) != expected_cell_count:
                    rows_with_inconsistent_length.append(cells)
                    continue

                filter_matches = []

                for filter in self.current_filters:
                    if (
                        filter.column is None
                    ):  # If we have a current_filter, but filter_column is None, we are searching all columns
                        if any(
                            filter.pattern.search(
                                self._get_cell_value_without_markup(cell)
                            )
                            for cell in cells
                        ):
                            filter_matches.append(True)
                    else:
                        col_idx = self._get_col_idx_by_name(filter.column)
                        if col_idx is None:
                            break
                        if (
                            len(self.unique_column_names) > 0
                        ):  # account for count column
                            col_idx -= 1
                        if filter.pattern.search(
                            self._get_cell_value_without_markup(cells[col_idx])
                        ):
                            filter_matches.append(True)
                if len(filter_matches) == len(self.current_filters):
                    filtered_rows.append(cells)
        else:
            for row in self.raw_rows:
                cells = split_line(row, self.delimiter, self.current_columns)
                if len(cells) == expected_cell_count:
                    filtered_rows.append(cells)
                else:
                    rows_with_inconsistent_length.append(row)

        # 2. Dedup by composite column key
        if len(self.unique_column_names) > 0:
            dedup_map = {}
            deduped_rows = []
            for cells in filtered_rows:
                composite_key = []
                for col_name in self.unique_column_names:
                    col_idx = self._get_col_idx_by_name(col_name)
                    if col_idx is None:
                        continue
                    if len(self.unique_column_names) > 0:  # account for count column
                        col_idx -= 1
                    composite_key.append(
                        self._get_cell_value_without_markup(cells[col_idx])
                    )
                composite_key = ",".join(composite_key)
                dedup_map[composite_key] = cells  # always overwrite to keep latest
                self.count_by_column_key[composite_key] += 1
            for k, cells in dedup_map.items():
                count = self.count_by_column_key[k]
                cells.insert(0, count)
                deduped_rows.append(cells)
        else:
            deduped_rows = filtered_rows

        # 3. Sort rows
        if self.sort_column is not None:
            sort_column_idx = self._get_col_idx_by_name(self.sort_column)
            if sort_column_idx is not None:
                try:
                    deduped_rows.sort(
                        key=lambda r: r[sort_column_idx],
                        reverse=self.sort_reverse,
                    )
                except (ValueError, IndexError):
                    # Fallback if column not found or row is malformed
                    pass

        final_rows = []

        # 4. Add to table and find search matches
        if self.search_term:
            for displayed_row_idx, cells in enumerate(deduped_rows):
                highlighted_cells = []
                for col_idx, cell in enumerate(cells):
                    if isinstance(
                        self.search_term, re.Pattern
                    ) and self.search_term.search(str(cell)):
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
            for cells in deduped_rows:
                final_rows.append(cells)

        if len(rows_with_inconsistent_length) > 0:
            self.notify(
                f"{len(rows_with_inconsistent_length)} rows not matching columns, skipped. Use 'raw' delimiter (press D) to disable parsing.",
                severity="warning",
            )

        final_rows = self._align_cells_to_visible_columns(final_rows)
        final_rows = self._apply_styles_to_cells(final_rows)
        self.displayed_rows = final_rows
        data_table.add_rows(final_rows)
        self.locked = False

        if restore_position:
            self._restore_position(
                data_table, cursor_x, cursor_y, scroll_x, scroll_y
            ) 

    def _apply_styles_to_cells(self, rows: list[list[str]]) -> list[list[str]]:
        styled_rows = []
        for row in rows:
            styled_cells = []
            for i, cell in enumerate(row):
                if i % 2 != 0:
                    cell = f"[#aaaaaa]{cell}[/#aaaaaa]"
                styled_cells.append(cell)
            styled_rows.append(styled_cells)
        return styled_rows

    def _align_cells_to_visible_columns(self, rows: list[list[str]]) -> list[list[str]]:
        new_rows = []
        for row in rows:
            cells = []
            for col in sorted(self.current_columns, key=lambda c: c.render_position):
                if not col.hidden:
                    cells.append(row[col.data_position])
            new_rows.append(cells)
        return new_rows

    def _restore_position(self, data_table, cursor_x, cursor_y, scroll_x, scroll_y):
        data_table.move_cursor(
            row=cursor_y, column=cursor_x, animate=False, scroll=False
        )
        self.call_after_refresh(
            lambda: data_table.scroll_to(
                scroll_x, scroll_y, animate=False, immediate=False
            )
        )

    def _rich_bold(self, text):
        return f"[bold]{text}[/bold]"

    def _update_status_bar(self) -> None:
        data_table = self.query_one(NlessDataTable)

        sort_prefix = self._rich_bold("Sort")
        filter_prefix = self._rich_bold("Filter")
        search_prefix = self._rich_bold("Search")

        if self.sort_column is None:
            sort_text = f"{sort_prefix}: None"
        else:
            sort_text = f"{sort_prefix}: {self.sort_column} {'desc' if self.sort_reverse else 'asc'}"

        if len(self.current_filters) == 0:
            filter_text = f"{filter_prefix}: None"
        else:
            filter_descriptions = []
            for f in self.current_filters:
                if f.column is None:
                    filter_descriptions.append(f"any='{f.pattern.pattern}'")
                else:
                    filter_descriptions.append(f"{f.column}='{f.pattern.pattern}'")
            filter_text = f"{filter_prefix}: " + ", ".join(filter_descriptions)

        if self.search_term is not None:
            search_text = f"{search_prefix}: '{self.search_term.pattern}' ({self.current_match_index + 1} / {len(self.search_matches)} matches)"
        else:
            search_text = f"{search_prefix}: None"

        total_rows = data_table.row_count
        total_cols = len(data_table.columns)
        current_row = data_table.cursor_row + 1  # Add 1 for 1-based indexing
        current_col = data_table.cursor_column + 1  # Add 1 for 1-based indexing

        row_prefix = self._rich_bold("Row")
        col_prefix = self._rich_bold("Col")
        position_text = f"{row_prefix}: {current_row}/{total_rows} {col_prefix}: {current_col}/{total_cols}"

        if self.is_tailing:
            tailing_text = "| " + self._rich_bold(
                "[#00bb00]Tailing (`t` to stop)[/#00bb00]"
            )
        else:
            tailing_text = ""

        column_text = ""
        if len(self.unique_column_names):
            column_names = ",".join(self.unique_column_names)
            column_text = f"| unique cols: ({column_names}) "

        status_bar = self.app.query_one("#status_bar", Static)
        status_bar.update(
            f"{sort_text} | {filter_text} | {search_text} | {position_text} {column_text}{tailing_text}"
        )

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

    def _get_cell_value_without_markup(self, cell_value) -> str:
        """Extract plain text from a cell value, removing any markup."""
        parsed_value = [*_parse(cell_value)]
        if len(parsed_value) > 1:
            return "".join([res[1] for res in parsed_value if res[1]])
        return cell_value

    def add_logs(self, log_lines: list[str]) -> None:
        self.locked = True
        data_table = self.query_one(NlessDataTable)

        # Infer delimiter from first few lines if not already set
        if not self.delimiter and len(log_lines) > 0:
            self.delimiter = infer_delimiter(log_lines[: min(5, len(log_lines))])
            self.delimiter_inferred = True

        if not self.first_row_parsed:
            first_log_line = log_lines[0]
            self.first_log_line = first_log_line
            if self.delimiter != "raw":
                parts = split_line(first_log_line, self.delimiter, self.current_columns)
                data_table.add_columns(*parts)
                self.current_columns = [
                    Column(
                        name=p,
                        labels=set(),
                        render_position=i,
                        data_position=i,
                        hidden=False,
                    )
                    for i, p in enumerate(parts)
                ]
                log_lines = log_lines[1:]  # Exclude header line
            else:
                # Delimiter is raw, treat entire line as single column
                data_table.add_column("log")
                self.current_columns = [
                    Column(
                        name="log",
                        labels=set(),
                        render_position=0,
                        data_position=0,
                        hidden=False,
                    )
                ]
            self.first_row_parsed = True

        self.raw_rows.extend(log_lines)

        mismatch_count = 0
        for line in log_lines:
            try:
                self._add_log_line(line)
            except RowLengthMismatchError:
                mismatch_count += 1
                continue

        if mismatch_count > 0:
            self.notify(
                f"{mismatch_count} rows not matching columns, skipped. Use 'raw' delimiter (press D) to disable parsing.",
                severity="warning",
            )

        self._update_status_bar()
        self.locked = False

    def _bisect_left(self, r_list: list[str], value: str, reverse: bool):
        tmp_list = list(r_list)
        if value.isnumeric():
            value = int(value)
            tmp_list = [int(v) for v in tmp_list]
        tmp_list.sort()
        if reverse:
            idx_in_temp = bisect.bisect_left(tmp_list, value)
            return len(tmp_list) - idx_in_temp
        else:
            return bisect.bisect_left(tmp_list, value)

    def _strip_column_indicators(self, col_name: str) -> str:
        return col_name.replace(" U", "").replace(" ▲", "").replace(" ▼", "")

    def _get_col_idx_by_name(
        self, col_name: str, render_position: bool = False
    ) -> Optional[int]:
        for col in self.current_columns:
            if self._get_cell_value_without_markup(col.name) == col_name:
                if render_position:
                    return col.render_position
                else:
                    return col.data_position
        return None

    def _add_log_line(self, log_line: str):
        """
        Adds a single log line by determining:
        1. if it should be displayed (based on filters)
        2. if it should be highlighted (based on current search term)
        3. where it should go, based off current sort
        """
        data_table = self.query_one(NlessDataTable)
        cells = split_line(log_line, self.delimiter, self.current_columns)
        if len(self.unique_column_names) > 0:
            cells.insert(0, "1")

        expected_cell_count = len([c for c in self.current_columns])
        if len(cells) != expected_cell_count:
            raise RowLengthMismatchError()

        try:
            aligned_cells = self._align_cells_to_visible_columns([cells])[0]
        except:
            raise RowLengthMismatchError()

        if len(self.current_filters) > 0:
            filter_matches = []
            for filter in self.current_filters:
                if filter.column is None:
                    # We're filtering any column
                    if any(
                        filter.pattern.search(
                            self._get_cell_value_without_markup(cell)
                        )
                        for cell in cells
                    ): filter_matches.append(True)
                else:
                    col_idx = self._get_col_idx_by_name(filter.column)
                    if col_idx is None:
                        return
                    if filter.pattern.search(
                        self._get_cell_value_without_markup(cells[col_idx])
                    ):
                        filter_matches.append(True)

            if len(filter_matches) != len(self.current_filters):
                return

        old_index = None
        old_row = None
        if len(self.unique_column_names) > 0:
            new_row_composite_key = []
            for col_name in self.unique_column_names:
                col_idx = self._get_col_idx_by_name(col_name)
                if col_idx is None:
                    continue
                new_row_composite_key.append(
                    self._get_cell_value_without_markup(cells[col_idx])
                )
            new_row_composite_key = ",".join(new_row_composite_key)

            for row_idx, row in enumerate(self.displayed_rows):
                composite_key = []
                for col_name in self.unique_column_names:
                    col_idx = self._get_col_idx_by_name(col_name, render_position=True)
                    if col_idx is None:
                        continue
                    composite_key.append(
                        self._get_cell_value_without_markup(row[col_idx])
                    )
                composite_key = ",".join(composite_key)

                if composite_key == new_row_composite_key:
                    new_cells = []
                    for col_idx, cell in enumerate(cells):
                        if col_idx == 0:
                            self.count_by_column_key[composite_key] += 1
                            cell = self.count_by_column_key[composite_key]
                        else:
                            cell = self._get_cell_value_without_markup(cell)
                        new_cells.append(f"[#00ff00]{cell}[/#00ff00]")
                    old_index = row_idx
                    cells = new_cells
                    old_row = self.displayed_rows[old_index]
                    break

            if old_index is None:
                self.count_by_column_key[new_row_composite_key] = 1

        if self.sort_column is not None:
            displayed_sort_column_idx = self._get_col_idx_by_name(
                self.sort_column, render_position=True
            )
            data_sort_column_idx = self._get_col_idx_by_name(
                self.sort_column, render_position=False
            )

            sort_key = self._get_cell_value_without_markup(
                str(cells[data_sort_column_idx])
            )
            displayed_row_keys = [
                self._get_cell_value_without_markup(str(r[displayed_sort_column_idx]))
                for r in self.displayed_rows
            ]
            if self.sort_reverse:
                new_index = self._bisect_left(
                    displayed_row_keys, sort_key, reverse=True
                )
            else:
                new_index = self._bisect_left(
                    displayed_row_keys, sort_key, reverse=False
                )
        else:
            new_index = len(self.displayed_rows)

        if self.search_term:
            highlighted_cells = []
            for col_idx, cell in enumerate(cells):
                if isinstance(self.search_term, re.Pattern) and self.search_term.search(
                    cell
                ):
                    cell = re.sub(
                        self.search_term,
                        lambda m: f"[reverse]{m.group(0)}[/reverse]",
                        cell,
                    )
                    highlighted_cells.append(cell)
                    self.search_matches.append(Coordinate(new_index, col_idx))
                else:
                    highlighted_cells.append(cell)
            cells = highlighted_cells

        old_row_key = None
        if old_index is not None:
            old_row_key = data_table.ordered_rows[old_index].key

        cells = self._align_cells_to_visible_columns([cells])[0]
        cells = self._apply_styles_to_cells([cells])[0]

        data_table.add_row_at(*cells, row_index=new_index)
        self.displayed_rows.insert(new_index, cells)

        if old_index is not None and old_row_key is not None:
            self.displayed_rows.remove(old_row)
            data_table.remove_row(old_row_key)

        if self.is_tailing:
            data_table.action_scroll_bottom()


class NlessApp(App):
    def __init__(self):
        super().__init__()
        self.input_history = []
        self.logs = []
        self.mounted = False
        self.buffers = [NlessBuffer(pane_id=1)]
        self.curr_buffer_idx = 0

    SCREENS = {"HelpScreen": HelpScreen}
    HISTORY_FILE = "~/.config/nless/history.json"

    CSS_PATH = "nless.tcss"

    BINDINGS = [
        ("N", "add_buffer", "New Buffer"),
        ("L", "show_tab_next", "Next Buffer"),
        ("H", "show_tab_previous", "Previous Buffer"),
        ("q", "close_active_buffer", "Close Active Buffer"),
        ("/", "search", "Search (all columns, by prompt)"),
        ("&", "search_to_filter", "Apply current search as filter"),
        ("|", "filter_any", "Filter any column (by prompt)"),
        ("f", "filter", "Filter selected column (by prompt)"),
        ("F", "filter_cursor_word", "Filter selected column by word under cursor"),
        ("D", "delimiter", "Change Delimiter"),
        ("d", "column_delimiter", "Change Column Delimiter"),
        ("W", "write_to_file", "Write current view to file"),
        ("J", "json_header", "Select new header from JSON in cell"),
        ("W", "write_to_file", "Write current view to file"),
        (
            "U",
            "mark_unique",
            "Mark a column unique to create a composite key for distinct/analysis",
        ),
        ("C", "filter_columns", "Filter Columns (by prompt)"),
        ("?", "push_screen('HelpScreen')", "Show Help"),
    ]

    async def on_resize(self, event: events.Resize) -> None:
        self.refresh()

    def on_select_changed(self, event: Select.Changed) -> None:
        event.control.remove()
        if event.control.id == "json_header_select":
            curr_buffer = self._get_current_buffer()
            cursor_column = curr_buffer.query_one(NlessDataTable).cursor_column
            curr_column = [c for c in curr_buffer.current_columns if c.render_position == cursor_column]
            if not curr_column:
                curr_buffer.notify("No column selected to add JSON key to", severity="error")
                return
            curr_column = curr_column[0]
            curr_column_name = curr_buffer._get_cell_value_without_markup(curr_column.name)

            new_column_name = f"{curr_column_name}.{event.value}"

            new_col = Column(
                name=new_column_name,
                labels=set(),
                computed=True,
                render_position=len(curr_buffer.current_columns),
                data_position=len(curr_buffer.current_columns),
                hidden=False,
                json_ref=f"{curr_column_name}.{event.value}"
            )
            curr_buffer.current_columns.append(new_col)
            curr_buffer._update_table()
            data_table = self.query_one(NlessDataTable)
            data_table.move_cursor(column=new_col.render_position)

    def action_json_header(self) -> None:
        """Set the column headers from JSON in the selected cell."""
        curr_buffer = self._get_current_buffer()
        data_table = curr_buffer.query_one(NlessDataTable)
        coordinate = data_table.cursor_coordinate
        try:
            cell_value = data_table.get_cell_at(coordinate)
            cell_value = curr_buffer._get_cell_value_without_markup(cell_value)
            json_data = json.loads(cell_value)
            if not isinstance(json_data, (dict, list)):
                curr_buffer.notify("Cell does not contain a JSON object.", severity="error")
                return
            new_columns = []
            #iterate through the full JSON heirarchy of keys, building up a list of keys
            def extract_keys(obj, prefix=""):
                if isinstance(obj, dict):
                    for k, v in obj.items():
                        new_prefix = f"{prefix}.{k}" if prefix else k
                        new_columns.append(new_prefix)
                        extract_keys(v, new_prefix)
                elif isinstance(obj, list) and len(obj) > 0:
                    for i in range(len(obj)):
                        extract_keys(obj[i], prefix + f"{i}")
            extract_keys(json_data)
            new_columns = list(dict.fromkeys(new_columns))  # deduplicate while preserving order

            #mount a select, with the new_colums as options - when the option is selected, a new column is added to the current buffer
            select = Select(
                options=[(col, col) for col in new_columns],
                classes="bottom-input",
                prompt="Select a json key to add as a column",
                id="json_header_select"
            )
            self.mount(select)
            self.call_after_refresh(lambda: curr_buffer._open_select(select))
        except Exception as e:
            curr_buffer.notify(f"Error parsing JSON: {str(e)}", severity="error")

    def action_column_delimiter(self) -> None:
        """Change the column delimiter."""
        self._create_prompt(
            "Type column delimiter (e.g. , or \\t or 'space' or 'raw')",
            "column_delimiter_input",
        )

    def action_write_to_file(self) -> None:
        """Write the current view to a file."""
        self._create_prompt(
            "Type output file path (e.g. /tmp/output.csv)", "write_to_file_input"
        )

    def action_filter_columns(self) -> None:
        """Filter columns by user input."""
        self._create_prompt(
            "Type pipe delimited column names to show (e.g. col1|col2) or 'all' to reset",
            "column_filter_input",
        )

    def _get_new_pane_id(self) -> int:
        return max(b.pane_id for b in self.buffers) + 1 if self.buffers else 1


    def handle_mark_unique(self, new_buffer: NlessBuffer, new_unique_column_name: str) -> None:
        if new_unique_column_name in [mc.value for mc in MetadataColumn]:
            # can't toggle count column
            return

        col_idx = new_buffer._get_col_idx_by_name(new_unique_column_name)
        new_unique_column = new_buffer.current_columns[col_idx] if col_idx is not None else None

        if new_unique_column is None:
            return

        new_buffer.count_by_column_key = defaultdict(lambda: 0)

        if new_unique_column_name in new_buffer.unique_column_names:
            new_buffer.unique_column_names.remove(new_unique_column_name)
            if new_buffer.sort_column in [metadata.value for metadata in MetadataColumn]:
                new_buffer.sort_column = None
            new_unique_column.labels.discard("U")
        else:
            new_buffer.unique_column_names.add(new_unique_column_name)
            new_unique_column.labels.add("U")

        if len(new_buffer.unique_column_names) == 0:
            # remove count column
            new_buffer.current_columns = [
                Column(
                    name=c.name,
                    labels=c.labels,
                    render_position=c.render_position - 1,
                    data_position=c.data_position - 1,
                    hidden=c.hidden,
                    json_ref=c.json_ref,
                    computed=c.computed,
                )
                for c in new_buffer.current_columns
                if c.name != MetadataColumn.COUNT.value
            ]
        elif MetadataColumn.COUNT.value not in [
            c.name for c in new_buffer.current_columns
        ]:
            # add count column at the start
            new_buffer.current_columns = [
                Column(
                    name=c.name,
                    labels=c.labels,
                    render_position=c.render_position + 1,
                    data_position=c.data_position + 1,
                    hidden=c.hidden,
                    json_ref=c.json_ref,
                    computed=c.computed,
                )
                for c in new_buffer.current_columns
            ]
            new_buffer.current_columns.insert(
                0,
                Column(
                    name=MetadataColumn.COUNT.value,
                    labels=set(),
                    render_position=0,
                    data_position=0,
                    hidden=False,
                    pinned=True,
                ),
            )

        pinned_columns_visible = len(
            [c for c in new_buffer.current_columns if c.pinned and not c.hidden]
        )
        if new_unique_column_name in new_buffer.unique_column_names:
            old_position = new_unique_column.render_position
            for col in new_buffer.current_columns:
                col_name = new_buffer._get_cell_value_without_markup(col.name)
                if col_name == new_unique_column_name:
                    col.render_position = pinned_columns_visible  # bubble to just after last pinned column
                    col.pinned = True
                elif (
                    col_name != new_unique_column_name
                    and col.render_position <= old_position
                    and col.name not in [mc.value for mc in MetadataColumn]
                    and not col.pinned
                ):
                    col.render_position += 1  # shift right to make space
        else:
            old_position = new_unique_column.render_position
            pinned_columns_visible -= 1
            for col in new_buffer.current_columns:
                col_name = new_buffer._get_cell_value_without_markup(col.name)
                if col_name == new_unique_column_name:
                    col.pinned = False
                    col.render_position = (
                        pinned_columns_visible if pinned_columns_visible > 0 else 0
                    )
                elif col.pinned and col.render_position >= old_position:
                    col.render_position -= 1

    def action_mark_unique(self) -> None:
        curr_buffer = self._get_current_buffer()
        data_table = curr_buffer.query_one(NlessDataTable)
        new_buffer = curr_buffer.copy(pane_id=self._get_new_pane_id())
        current_cursor_column = data_table.cursor_column
        new_unique_column = [
            c
            for c in new_buffer.current_columns
            if c.render_position == current_cursor_column
        ]
        if not new_unique_column:
            self.notify("No column selected to mark as unique")
            return

        new_unique_column = new_unique_column[0]
        new_unique_column_name = new_buffer._get_cell_value_without_markup(
            new_unique_column.name
        )

        self.handle_mark_unique(new_buffer, new_unique_column_name)
        buffer_name = f"+u:{new_unique_column_name}" if new_unique_column_name in new_buffer.unique_column_names else f"-u:{new_unique_column_name}"
        self.add_buffer(new_buffer, name=buffer_name)

        # update the cursor position's index to match the new position of the column
        new_cursor_position = 0
        for i, col in enumerate(
            sorted(new_buffer.current_columns, key=lambda c: c.render_position)
        ):
            if (
                new_buffer._get_cell_value_without_markup(col.name)
                == new_unique_column_name
            ):
                new_cursor_position = i
                break
        self.call_after_refresh(
            lambda: new_buffer.query_one(NlessDataTable).move_cursor(column=new_cursor_position)
        )

    def action_delimiter(self) -> None:
        """Change the delimiter used for parsing."""
        self._create_prompt(
            "Type delimiter character (e.g. ',', '\\t', ' ', '|') or 'raw' for no parsing",
            "delimiter_input",
        )

    def action_search_to_filter(self) -> None:
        """Convert current search into a filter across all columns."""
        current_buffer = self._get_current_buffer()
        if not current_buffer.search_term:
            current_buffer.notify(
                "No active search to convert to filter", severity="warning"
            )
            return
        else:
            new_buffer = current_buffer.copy(pane_id=self._get_new_pane_id())
            new_buffer.current_filters.append(Filter(column=None, pattern=current_buffer.search_term))
            self.add_buffer(new_buffer, name=f"+f:any={current_buffer.search_term.pattern}")

    def action_search(self) -> None:
        """Bring up search input to highlight matching text."""
        self._create_prompt("Type search term and press Enter", "search_input")

    def _create_prompt(self, placeholder, id):
        input = AutocompleteInput(
            placeholder=placeholder,
            id=id,
            classes="bottom-input",
            history=[h["val"] for h in self.input_history if h["id"] == id],
            on_add=lambda val: self.input_history.append({"id": id, "val": val})
        )
        tab_content = self.query_one(TabbedContent)
        active_tab = tab_content.active
        for tab_pane in tab_content.query(TabPane):
            if tab_pane.id == active_tab:
                tab_pane.mount(input)
                self.call_after_refresh(lambda: input.focus())
                break

    def _filter_composite_key(self, current_buffer: NlessBuffer) -> None:
        data_table = current_buffer.query_one(NlessDataTable)
        cursor_column = data_table.cursor_column
        selected_column = [c for c in current_buffer.current_columns if c.render_position == cursor_column]
        if selected_column:
            selected_column_name = current_buffer._get_cell_value_without_markup(selected_column[0].name)
            if selected_column_name in current_buffer.unique_column_names:
                new_buffer = current_buffer.copy(pane_id=self._get_new_pane_id())
                filters = []
                for column in current_buffer.unique_column_names:
                    col_idx = current_buffer._get_col_idx_by_name(column, render_position=True)
                    cell_value = data_table.get_cell_at((data_table.cursor_row, col_idx))
                    cell_value = current_buffer._get_cell_value_without_markup(cell_value)
                    filters.append(Filter(column=current_buffer._get_cell_value_without_markup(column), pattern=re.compile(re.escape(cell_value), re.IGNORECASE)))
                    self.handle_mark_unique(new_buffer, column)
                new_buffer.current_filters.extend(filters)
                self.add_buffer(new_buffer, name=f"+f:{','.join([f'{f.column}={f.pattern.pattern}' for f in filters])}")

    def on_key(self, event: Key) -> None:
        """Handle key events."""
        if event.key == "escape" and (isinstance(self.focused, Input) or isinstance(self.focused, Select)):
            self.focused.remove()

        current_buffer = self._get_current_buffer()
        if event.key == "enter" and isinstance(self.focused, NlessDataTable):
            self._filter_composite_key(current_buffer)

        if event.key in [str(i) for i in range(1, 10)] and isinstance(self.focused, NlessDataTable):
            self.show_tab_by_index(int(event.key) - 1)

    def handle_column_delimiter_submitted(self, event: Input.Submitted) -> None:
        event.input.remove()
        new_col_delimiter = event.value
        if new_col_delimiter == "json":
            current_buffer = self._get_current_buffer()
            data_table = current_buffer.query_one(NlessDataTable)
            cursor_coordinate = data_table.cursor_coordinate
            cell = data_table.get_cell_at(cursor_coordinate)
            selected_column = [c for c in current_buffer.current_columns if c.render_position == cursor_coordinate.column]
            if not selected_column:
                current_buffer.notify("No column selected to parse as JSON", severity="error")
                return
            selected_column = selected_column[0]
            try:
                cell_json = json.loads(current_buffer._get_cell_value_without_markup(cell))
                if not isinstance(cell_json, (dict, list)):
                    current_buffer.notify("Selected cell does not contain a JSON object or array", severity="error")
                    return
                column_count = len(current_buffer.current_columns)
                cell_json_keys = list(cell_json.keys()) if isinstance(cell_json, dict) else [i for i in range(len(cell_json))]
                for i, key in enumerate(cell_json_keys):
                    if f"{selected_column.name}.{key}" not in [c.name for c in current_buffer.current_columns]:
                        current_buffer.current_columns.append(Column(name=f"{selected_column.name}.{key}", labels=set(), render_position=column_count+i, data_position=column_count+i, hidden=False, computed=True, json_ref=f"{selected_column.name}.{key}"))
                current_buffer._update_table()
            except json.JSONDecodeError:
                current_buffer.notify("Selected cell does not contain a JSON object or array", severity="error")
                return

    def handle_write_to_file_submitted(self, event: Input.Submitted) -> None:
        output_path = event.value
        event.input.remove()
        current_buffer = self._get_current_buffer()
        try:
            t = Thread(target=write_buffer, args=(current_buffer, output_path))
            if output_path != "-":
                t.start()
                t.join()
                current_buffer.notify(f"Wrote current view to {output_path}")
            else:
                t.start()
                self.exit()
        except Exception as e:
            current_buffer.notify(f"Failed to write to file: {e}")

    def on_input_submitted(self, event: Input.Submitted) -> None:
        if event.input.id == "search_input":
            self.handle_search_submitted(event)
        elif event.input.id == "filter_input" or event.input.id == "filter_input_any":
            self.handle_filter_submitted(event)
        elif event.input.id == "delimiter_input":
            self.handle_delimiter_submitted(event)
        elif event.input.id == "column_filter_input":
            self.handle_column_filter_submitted(event)
        elif event.input.id == "write_to_file_input":
            self.handle_write_to_file_submitted(event)
        elif event.input.id == "column_delimiter_input":
            self.handle_column_delimiter_submitted(event)

    def handle_search_submitted(self, event: Input.Submitted) -> None:
        input_value = event.value
        event.input.remove()
        current_buffer = self._get_current_buffer()
        current_buffer._perform_search(input_value)

    def _get_current_buffer(self) -> NlessBuffer:
        return self.buffers[self.curr_buffer_idx]

    def action_filter(self) -> None:
        """Filter rows based on user input."""
        data_table = self._get_current_buffer().query_one(NlessDataTable)
        column_index = data_table.cursor_column
        column_label = data_table.ordered_columns[column_index].label
        self._create_prompt(
            f"Type filter text for column: {column_label} and press enter",
            "filter_input",
        )

    def action_filter_any(self) -> None:
        """Filter any column based on user input."""
        self._create_prompt(
            "Type filter text to match across all columns", "filter_input_any"
        )

    def handle_filter_submitted(self, event: Input.Submitted) -> None:
        filter_value = event.value
        event.input.remove()
        curr_buffer = self._get_current_buffer()
        data_table = curr_buffer.query_one(NlessDataTable)

        if event.input.id == "filter_input_any":
            self._perform_filter_any(filter_value)
        else:
            column_index = data_table.cursor_column
            column_label = [
                c
                for c in curr_buffer.current_columns
                if c.render_position == column_index
            ]
            if not column_label:
                self.notify("No column selected for filtering")
                return
            column_label = curr_buffer._get_cell_value_without_markup(
                column_label[0].name
            )
            self._perform_filter(filter_value, column_label)

    def _perform_filter_any(self, filter_value: Optional[str]) -> None:
        new_buffer = self._get_current_buffer().copy(pane_id=self._get_new_pane_id())
        """Performs a filter across all columns and updates the table."""
        if not filter_value:
            new_buffer.current_filters = []
            new_buf_name = new_buffer.current_filters and f"-f:{','.join([f'{f.column if f.column else "any"}={f.pattern.pattern}' for f in new_buffer.current_filters])}" or "-f"
        else:
            try:
                new_buffer.current_filters.append(Filter(column=None, pattern=re.compile(filter_value, re.IGNORECASE)))
            except re.error:
                new_buffer.notify("Invalid regex pattern", severity="error")
                return
            new_buf_name = f"+f:any={filter_value}"

        self.add_buffer(new_buffer, name=new_buf_name)

    def _perform_filter(
        self, filter_value: Optional[str], column_name: Optional[str]
    ) -> None:
        """Performs a filter on the data and updates the table."""
        new_buffer = self._get_current_buffer().copy(pane_id=self._get_new_pane_id())
        if not filter_value:
            new_buf_name = new_buffer.current_filters and f"-f:{','.join([f'{f.column if f.column else "any"}={f.pattern.pattern}' for f in new_buffer.current_filters])}" or "-f"
            new_buffer.current_filters = []
        else:
            if column_name in new_buffer.unique_column_names:
                self.handle_mark_unique(new_buffer, column_name)
                self.notify(f"Removed unique column: {column_name}, to allow filtering.", severity="info")
            try:
                # Compile the regex pattern
                new_buffer.current_filters.append(Filter(column=column_name, pattern=re.compile(filter_value, re.IGNORECASE)))
            except re.error:
                new_buffer.notify("Invalid regex pattern", severity="error")
                return
            new_buf_name = f"+f:{column_name}={filter_value}"

        self.add_buffer(new_buffer, name=new_buf_name)

    def handle_delimiter_submitted(self, event: Input.Submitted) -> None:
        curr_buffer = self._get_current_buffer()
        curr_buffer.current_filters = []
        curr_buffer.search_term = None
        curr_buffer.sort_column = None
        curr_buffer.unique_column_names = set()
        prev_delimiter = curr_buffer.delimiter

        event.input.remove()
        data_table = curr_buffer.query_one(NlessDataTable)
        curr_buffer.delimiter_inferred = False
        delimiter = event.value
        if delimiter not in [
            ",",
            "\\t",
            " ",
            "  ",
            "|",
            ";",
            "raw",
            "json",
        ]:  # if our delimiter is not one of the common ones, treat it as a regex
            try:
                pattern = re.compile(rf"{delimiter}")  # Validate regex
                curr_buffer.delimiter = pattern
                curr_buffer.current_columns = [
                    Column(
                        name=h, labels=set(), render_position=i, data_position=i, hidden=False
                    )
                    for i, h in enumerate(pattern.groupindex.keys())
                ]
                if prev_delimiter != "raw" and not isinstance(
                    prev_delimiter, re.Pattern
                ):
                    curr_buffer.raw_rows.insert(0, curr_buffer.first_log_line)
                curr_buffer._update_table()
                return
            except:
                curr_buffer.notify("Invalid delimiter", severity="error")
                return

        if delimiter == "\\t":
            delimiter = "\t"

        curr_buffer.delimiter = delimiter

        parsed_full_json_file = False

        if delimiter == "raw":
            new_header = ["log"]
        elif delimiter == "json":
            try:
                new_header = json.loads(curr_buffer.first_log_line).keys()
            except Exception as e:
                # attempt to read all logs as one json payload
                try:
                    all_logs = ""
                    if prev_delimiter != "raw" and not isinstance(prev_delimiter, re.Pattern):
                        all_logs = curr_buffer.first_log_line + "\n"
                    all_logs += "\n".join(curr_buffer.raw_rows)
                    buffer_json = json.loads(all_logs)
                    if isinstance(buffer_json, list) and len(buffer_json) > 0 and isinstance(buffer_json[0], dict):
                        new_header = buffer_json[0].keys()
                        curr_buffer.raw_rows = [json.dumps(item) for item in buffer_json]
                    elif isinstance(buffer_json, dict):
                        new_header = buffer_json.keys()
                        curr_buffer.raw_rows = [json.dumps(buffer_json)]
                    else:
                        curr_buffer.notify(f"Failed to parse JSON logs: {e}", severity="error")
                        return
                    curr_buffer.first_log_line = curr_buffer.raw_rows[0]
                    parsed_full_json_file = True
                except Exception as e2:
                    curr_buffer.notify(f"Failed to parse JSON logs: {e2}", severity="error")
                    return
        elif prev_delimiter == "raw" or isinstance(prev_delimiter, re.Pattern):
            new_header = split_line(curr_buffer.raw_rows[0], curr_buffer.delimiter, curr_buffer.current_columns)
            curr_buffer.raw_rows.pop(0)
        else:
            new_header = split_line(curr_buffer.first_log_line, curr_buffer.delimiter, curr_buffer.current_columns)

        if (
            (prev_delimiter != delimiter)
            and (prev_delimiter != "raw" and not isinstance(prev_delimiter, re.Pattern) and prev_delimiter != "json")
            and (delimiter == "raw" or isinstance(delimiter, re.Pattern) or delimiter == "json")
            and not parsed_full_json_file
        ):
            curr_buffer.raw_rows.insert(0, curr_buffer.first_log_line)

        curr_buffer.current_columns = [
            Column(
                name=h, labels=set(), render_position=i, data_position=i, hidden=False
            )
            for i, h in enumerate(new_header)
        ]
        curr_buffer._update_table()

    def handle_column_filter_submitted(self, event: Input.Submitted) -> None:
        curr_buffer = self._get_current_buffer()
        input_value = event.value
        event.input.remove()
        if input_value.lower() == "all":
            for col in curr_buffer.current_columns:
                col.hidden = False
        else:
            column_name_filters = [name.strip() for name in input_value.split("|")]
            column_name_filter_regexes = [
                re.compile(rf"{name}", re.IGNORECASE) for name in column_name_filters
            ]
            metadata_columns = [mc.value for mc in MetadataColumn]
            visible_pinned_columns = [
                col
                for col in curr_buffer.current_columns
                if col.pinned and not col.hidden
            ]
            for col in curr_buffer.current_columns:
                matched = False
                plain_name = curr_buffer._get_cell_value_without_markup(col.name)
                for i, column_name_filter in enumerate(column_name_filter_regexes):
                    if column_name_filter.search(plain_name) and not col.pinned:
                        col.hidden = False
                        col.render_position = i + len(
                            visible_pinned_columns
                        )  # keep metadata columns at the start
                        matched = True
                        break

                if matched:
                    continue

                if (
                    col.name not in [mc.value for mc in MetadataColumn]
                    and not col.pinned
                ):
                    col.hidden = True
                    col.render_position = 99999

            # Ensure at least one column is visible
            if all(col.hidden for col in curr_buffer.current_columns):
                curr_buffer.notify(
                    "At least one column must be visible.", severity="warning"
                )
                for col in curr_buffer.current_columns:
                    col.hidden = False

        sorted_columns = sorted(
            curr_buffer.current_columns, key=lambda c: c.render_position
        )
        for i, col in enumerate(sorted_columns):
            col.render_position = i

        curr_buffer._update_table()

    def action_filter_cursor_word(self) -> None:
        """Filter by the word under the cursor."""
        curr_buffer = self._get_current_buffer()
        data_table = curr_buffer.query_one(NlessDataTable)
        coordinate = data_table.cursor_coordinate
        try:
            cell_value = data_table.get_cell_at(coordinate)
            cell_value = curr_buffer._get_cell_value_without_markup(cell_value)
            cell_value = re.escape(cell_value)  # Validate regex
            selected_column = [
                c
                for c in curr_buffer.current_columns
                if c.render_position == coordinate.column
            ]
            if not selected_column:
                self.notify("No column selected for filtering")
                return
            self._perform_filter(
                f"^{cell_value}$",
                curr_buffer._get_cell_value_without_markup(selected_column[0].name),
            )
        except Exception as e:
            self.notify("Cannot get cell value.", severity="error")

    def refresh_buffer_and_focus(self, new_buffer: NlessBuffer, cursor_coordinate: Coordinate, offset: Offset) -> None:
        new_buffer._update_table()
        data_table = new_buffer.query_one(NlessDataTable)
        data_table.focus()
        new_buffer._restore_position(data_table, cursor_coordinate.column, cursor_coordinate.row, offset.x, offset.y)

    def add_buffer(self, new_buffer: NlessBuffer, name: str, add_prev_index: bool = True) -> None:
        curr_data_table = self._get_current_buffer().query_one(NlessDataTable)

        self.buffers.append(new_buffer)
        tabbed_content = self.query_one(TabbedContent)
        buffer_number = len(self.buffers)
        tab_pane = TabPane(f"[#00ff00]{buffer_number}[/#00ff00] {self.curr_buffer_idx+1 if add_prev_index else ""}{name}", id=f"buffer{new_buffer.pane_id}")
        tabbed_content.add_pane(tab_pane)
        scroll_view = ScrollView()
        tab_pane.mount(scroll_view)
        scroll_view.mount(new_buffer)
        self.curr_buffer_idx = len(self.buffers) - 1
        tabbed_content.active = f"buffer{new_buffer.pane_id}"
        self.call_after_refresh(lambda: self.refresh_buffer_and_focus(new_buffer, curr_data_table.cursor_coordinate, curr_data_table.scroll_offset))


    def on_exit_app(self) -> None:
        # check if file exists, if not create it
        os.makedirs(os.path.dirname(os.path.expanduser(self.HISTORY_FILE)), exist_ok=True)

        with open(os.path.expanduser(self.HISTORY_FILE), "w") as f:
            json.dump(self.input_history, f)

    def action_close_active_buffer(self) -> None:
        if len(self.buffers) == 1:
            self.exit()
            return

        tabbed_content = self.query_one(TabbedContent)
        current_buffer = self._get_current_buffer()

        tabbed_content.remove_pane(f"buffer{current_buffer.pane_id}")
        self.buffers.pop(self.curr_buffer_idx)

        if self.curr_buffer_idx >= len(self.buffers):
            self.curr_buffer_idx = len(self.buffers) - 1

        new_curr_buffer = self.buffers[self.curr_buffer_idx]

        tabbed_content.active = f"buffer{new_curr_buffer.pane_id}"
        tabbed_content.query_one(f"#buffer{new_curr_buffer.pane_id}").query_one(
            NlessDataTable
        ).focus()

        self.call_after_refresh(lambda: new_curr_buffer._update_status_bar())
        self.call_after_refresh(lambda: self._update_panes())

    def _update_panes(self) -> None:
        tabbed_content = self.query_one(TabbedContent)
        pattern = re.compile(r"((\[#00ff00\])?(\d+?)(\[/#00ff00\])?) .*")
        for i, pane in enumerate(tabbed_content.query(Tab).results()):
            curr_title = str(pane.content)
            pattern_matches = pattern.match(curr_title)
            if pattern_matches:
                old_index = pattern_matches.group(1)
                curr_title = curr_title.replace(old_index, f"[#00ff00]{str(i + 1)}[/#00ff00]", count=1)
                pane.update(curr_title)

    def action_show_tab_next(self) -> None:
        tabbed_content = self.query_one(TabbedContent)
        self.curr_buffer_idx = (self.curr_buffer_idx + 1) % len(self.buffers)
        active_buffer_id = f"buffer{self.buffers[self.curr_buffer_idx].pane_id}"
        tabbed_content.active = active_buffer_id
        tabbed_content.query_one(f"#{active_buffer_id}").query_one(
            NlessDataTable
        ).focus()
        self._get_current_buffer()._update_status_bar()

    def action_show_tab_previous(self) -> None:
        tabbed_content = self.query_one(TabbedContent)
        self.curr_buffer_idx = (self.curr_buffer_idx - 1) % len(self.buffers)
        active_buffer_id = f"buffer{self.buffers[self.curr_buffer_idx].pane_id}"
        tabbed_content.active = active_buffer_id
        tabbed_content.query_one(f"#{active_buffer_id}").query_one(
            NlessDataTable
        ).focus()
        self._get_current_buffer()._update_status_bar()

    def show_tab_by_index(self, index: int) -> None:
        if index < 0 or index >= len(self.buffers):
            return
        tabbed_content = self.query_one(TabbedContent)
        self.curr_buffer_idx = index
        active_buffer_id = f"buffer{self.buffers[self.curr_buffer_idx].pane_id}"
        tabbed_content.active = active_buffer_id
        tabbed_content.query_one(f"#{active_buffer_id}").query_one(
            NlessDataTable
        ).focus()
        self._get_current_buffer()._update_status_bar()

    def on_mount(self) -> None:
        self.mounted = True
        self.query_one(NlessDataTable).focus()

        os.makedirs(os.path.dirname(os.path.expanduser(self.HISTORY_FILE)), exist_ok=True)
        if not os.path.exists(os.path.expanduser(self.HISTORY_FILE)):
            open(os.path.expanduser(self.HISTORY_FILE), "w").close()
        with open(os.path.expanduser(self.HISTORY_FILE), "r") as f:
            try:
                self.input_history = json.load(f)
            except:
                self.input_history = []

    def action_add_buffer(self) -> None:
        max_buffer_id = max(buffer.pane_id for buffer in self.buffers)
        new_buffer = NlessBuffer(pane_id=max_buffer_id + 1)
        self.call_after_refresh(lambda: new_buffer.add_logs(self.logs))
        self.add_buffer(new_buffer, name=f"buffer{new_buffer.pane_id}", add_prev_index=False)

    def compose(self) -> ComposeResult:
        init_buffer = self.buffers[0]
        with TabbedContent():
            with TabPane(f"[#00ff00]1[/#00ff00] original", id=f"buffer{init_buffer.pane_id}"):
                with ScrollView():
                    yield init_buffer
        yield Static(id="status_bar", classes="dock-bottom")

    def add_logs(self, log_lines: list[str]) -> None:
        self.logs.extend(log_lines)
        for buffer in self.buffers:
            while not buffer.mounted or buffer.locked:
                time.sleep(0.5)
            buffer.add_logs(log_lines)


def main():
    app = NlessApp()
    new_fd = sys.stdin.fileno()

    parser = argparse.ArgumentParser(description="Test InputConsumer with stdin.")
    parser.add_argument(
        "filename", nargs="?", help="File to read input from (defaults to stdin)"
    )
    parser.add_argument("--version", action="version", version=f"{get_version()}")

    args = parser.parse_args()

    if args.filename:
        ic = InputConsumer(
            args.filename,
            None,
            lambda: app.mounted,
            lambda lines: app.add_logs(lines),
        )
        t = Thread(target=ic.run, daemon=True)
        t.start()
    else:
        ic = InputConsumer(
            None, new_fd, lambda: app.mounted, lambda lines: app.add_logs(lines)
        )
        t = Thread(target=ic.run, daemon=True)
        t.start()

    sys.__stdin__ = open("/dev/tty")
    app.run()


if __name__ == "__main__":
    main()
